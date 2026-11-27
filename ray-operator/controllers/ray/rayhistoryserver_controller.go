package ray

import (
	"context"
	"fmt"
	"reflect"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

const (
	historyServerFinalizer    = "ray.io/history-server-finalizer"
	historyServerOwnerLabel   = "ray.io/history-server-owner"
	historyServerFieldManager = "kuberay-history-server"
	historyServerRetry        = 30 * time.Second
)

// RayHistoryServerController owns only the shared History Server lifecycle.
// CR presence is the activation mechanism. Conditions never change feature gates
// and this controller does not enroll or rewrite RayClusters.
type RayHistoryServerController struct {
	client.Client
	APIReader    client.Reader
	Scheme       *runtime.Scheme
	Namespace    string
	Image        string
	ProxyImage   string
	ProxyTLSArgs []string
}

func NewRayHistoryServerController(mgr ctrl.Manager, namespace, image string, proxyTLSArgs []string) *RayHistoryServerController {
	return &RayHistoryServerController{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(), Namespace: namespace, Image: image, ProxyImage: oidcProxyContainerImage, ProxyTLSArgs: proxyTLSArgs}
}

// +kubebuilder:rbac:groups=ray.io,resources=rayhistoryservers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayhistoryservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayhistoryservers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates,verbs=get;list;create;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=clusterissuers,verbs=get
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,resourceNames="system:auth-delegator",verbs=bind

func (r *RayHistoryServerController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	hs := &rayv1alpha1.RayHistoryServer{}
	if err := r.Get(ctx, req.NamespacedName, hs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !hs.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, hs)
	}
	if !controllerutil.ContainsFinalizer(hs, historyServerFinalizer) {
		before := hs.DeepCopy()
		controllerutil.AddFinalizer(hs, historyServerFinalizer)
		if err := r.Patch(ctx, hs, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
	}
	in, err := r.storageInputs(ctx, hs)
	if err != nil {
		return r.finish(ctx, hs, false, false, false, "InvalidConfiguration", err.Error())
	}
	// Reconcile certificates before waiting on their readiness. cert-manager may
	// be installed after the operator starts; direct reads avoid startup watches.
	tlsReady, tlsRevision, err := r.reconcileHistoryServerTLS(ctx, hs, &in)
	if err != nil {
		return r.finish(ctx, hs, false, false, false, "TLSConfigurationError", err.Error())
	}
	in.Revision += "/" + tlsRevision
	objects, err := historyServerResources(hs, in)
	if err != nil {
		return r.finish(ctx, hs, false, false, false, "InvalidConfiguration", err.Error())
	}
	for _, obj := range objects {
		if err := r.applyOwned(ctx, hs, obj); err != nil {
			_, statusErr := r.finish(ctx, hs, true, false, false, "ApplyFailed", err.Error())
			if statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{}, err
		}
	}
	deployment := &appsv1.Deployment{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: "ray-history-server"}, deployment); err != nil {
		return ctrl.Result{}, err
	}
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	available := deployment.Status.ObservedGeneration >= deployment.Generation &&
		deployment.Status.UpdatedReplicas == replicas && deployment.Status.AvailableReplicas >= replicas &&
		deployment.Status.Replicas == replicas
	for _, condition := range deployment.Status.Conditions {
		if deployment.Status.ObservedGeneration >= deployment.Generation && condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse {
			return r.finish(ctx, hs, true, false, false, "DeploymentFailed", "History Server Deployment is not progressing")
		}
	}
	if !tlsReady {
		return r.finish(ctx, hs, true, available, false, "CertificatePending", "Waiting for a valid serving certificate")
	}
	if !available {
		return r.finish(ctx, hs, true, false, false, "DeploymentPending", "Waiting for the current History Server Deployment rollout")
	}
	return r.finish(ctx, hs, true, true, true, "Ready", "History Server configuration, serving certificate and Deployment are ready")
}

func (r *RayHistoryServerController) finish(ctx context.Context, hs *rayv1alpha1.RayHistoryServer, valid, available, ready bool, reason, message string) (ctrl.Result, error) {
	before := hs.DeepCopy()
	set := func(kind rayv1alpha1.RayHistoryServerConditionType, value bool, conditionReason, conditionMessage string) {
		status := metav1.ConditionFalse
		if value {
			status = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&hs.Status.Conditions, metav1.Condition{Type: string(kind), Status: status, Reason: conditionReason, Message: conditionMessage, ObservedGeneration: hs.Generation})
	}
	configReason, configMessage := reason, message
	if valid {
		configReason, configMessage = "ConfigurationValid", "Storage and security configuration is valid"
	}
	set(rayv1alpha1.RayHistoryServerConfigurationValid, valid, configReason, configMessage)
	deploymentReason, deploymentMessage := reason, message
	if available {
		deploymentReason, deploymentMessage = "DeploymentAvailable", "The current Deployment rollout is available"
	}
	set(rayv1alpha1.RayHistoryServerDeploymentAvailable, available, deploymentReason, deploymentMessage)
	set(rayv1alpha1.RayHistoryServerReady, ready, reason, message)
	degraded := !valid || reason == "ApplyFailed" || reason == "DeploymentFailed"
	if degraded {
		set(rayv1alpha1.RayHistoryServerDegraded, true, reason, message)
	} else {
		set(rayv1alpha1.RayHistoryServerDegraded, false, "NoFailure", "No configuration or reconciliation failure")
	}
	hs.Status.ObservedGeneration = hs.Generation
	if !reflect.DeepEqual(before.Status, hs.Status) {
		if err := r.Status().Patch(ctx, hs, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: historyServerRetry}, nil
}

// ForceOwnership is limited to resources already owned by this exact CR UID.
// Never take over an administrator's object just because its name matches.
func (r *RayHistoryServerController) applyOwned(ctx context.Context, hs *rayv1alpha1.RayHistoryServer, desired client.Object) error {
	gvk, err := apiutil.GVKForObject(desired, r.Scheme)
	if err != nil {
		return err
	}
	desired.GetObjectKind().SetGroupVersionKind(gvk)
	existing := desired.DeepCopyObject().(client.Object)
	err = r.APIReader.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if err == nil && !metav1.IsControlledBy(existing, hs) {
		return fmt.Errorf("ownership conflict: %s %s is not controlled by this RayHistoryServer", gvk.Kind, client.ObjectKeyFromObject(desired))
	}
	if err == nil {
		// Optimistic concurrency also prevents adopting a replacement between
		// the ownership check and server-side apply.
		desired.SetResourceVersion(existing.GetResourceVersion())
		desired.SetUID(existing.GetUID())
	}
	if err := controllerutil.SetControllerReference(hs, desired, r.Scheme); err != nil {
		return err
	}
	labels := desired.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[historyServerOwnerLabel] = string(hs.UID)
	desired.SetLabels(labels)
	// Use Create for absent objects so a racing create cannot be overwritten by
	// ForceOwnership; subsequent reconciles use SSA for drift correction.
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired, client.FieldOwner(historyServerFieldManager))
	}
	return r.Patch(ctx, desired, client.Apply, client.FieldOwner(historyServerFieldManager), client.ForceOwnership) //nolint:staticcheck // client.Apply accepts the dynamically constructed client.Object resources used here.
}

func (r *RayHistoryServerController) finalize(ctx context.Context, hs *rayv1alpha1.RayHistoryServer) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(hs, historyServerFinalizer) {
		return ctrl.Result{}, nil
	}
	// List by CR UID, then verify ownership. This handles renamed certificates
	// without deleting user-provided TLS or storage Secrets.
	lists := []client.ObjectList{&appsv1.DeploymentList{}, &corev1.ServiceList{}, &corev1.ConfigMapList{}, &networkingv1.NetworkPolicyList{}, &corev1.ServiceAccountList{}, &rbacv1.ClusterRoleBindingList{}, &certmanagerv1.CertificateList{}, &certmanagerv1.IssuerList{}, &corev1.SecretList{}}
	pending := false
	for _, list := range lists {
		opts := []client.ListOption{client.MatchingLabels{historyServerOwnerLabel: string(hs.UID)}}
		if _, clusterScoped := list.(*rbacv1.ClusterRoleBindingList); !clusterScoped {
			opts = append(opts, client.InNamespace(r.Namespace))
		}
		if err := r.APIReader.List(ctx, list, opts...); err != nil {
			if meta.IsNoMatchError(err) || errors.IsNotFound(err) {
				continue
			}
			return ctrl.Result{}, err
		}
		if err := meta.EachListItem(list, func(item runtime.Object) error {
			obj := item.(client.Object)
			owned := false
			for _, owner := range obj.GetOwnerReferences() {
				if owner.UID == hs.UID {
					owned = true
				}
			}
			if !owned {
				return nil
			}
			pending = true
			uid, rv := obj.GetUID(), obj.GetResourceVersion()
			return client.IgnoreNotFound(r.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &rv}, client.PropagationPolicy(metav1.DeletePropagationForeground)))
		}); err != nil {
			return ctrl.Result{}, err
		}
	}
	if pending {
		return ctrl.Result{RequeueAfter: historyServerRetry}, nil
	}
	before := hs.DeepCopy()
	controllerutil.RemoveFinalizer(hs, historyServerFinalizer)
	return ctrl.Result{}, r.Patch(ctx, hs, client.MergeFrom(before))
}

func (r *RayHistoryServerController) SetupWithManager(mgr ctrl.Manager, concurrency int) error {
	// Poll cert-manager resources through APIReader; don't make their presence
	// a prerequisite for starting the operator manager.
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1alpha1.RayHistoryServer{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&rbacv1.ClusterRoleBinding{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			if obj.GetNamespace() != r.Namespace {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "default"}}}
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrency}).Complete(r)
}
