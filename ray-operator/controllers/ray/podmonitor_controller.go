package ray

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	podMonitorOptOutAnnotation = "odh.ray.io/cluster-podmonitor"
	podMonitorScrapeLabel      = "monitoring.opendatahub.io/scrape"
	podMonitorRetryInterval    = time.Minute
	podMonitorRayJobIndex      = "podmonitor.rayjob-owner-uid"
)

// PodMonitorController manages optional monitoring independently of RayCluster readiness.
// CARRY: RHAI-23206 - downstream scrape-by-default integration with OpenShift UWM.
//
// OpenShift enables monitoring by default. A standalone RayCluster or its owning
// RayJob opts out with the annotation odh.ray.io/cluster-podmonitor: disabled;
// the live RayJob annotation controls its owned clusters.
// Custom metrics-export-port values require a matching container port named metrics.
// UWM network ingress is separate (RHAI-23207); DSC global control is tracked in RHAI-23213.
type PodMonitorController struct {
	client.Client
	Recorder    events.EventRecorder
	cleanupOnly bool
}

// NewPodMonitorController uses the manager cache for workload and PodMonitor reads.
func NewPodMonitorController(mgr ctrl.Manager) *PodMonitorController {
	return &PodMonitorController{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorder("podmonitor-controller"),
	}
}

// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters;rayjobs,verbs=get;list;watch

// Reconcile creates, repairs or removes only this RayCluster's owned PodMonitor.
func (r *PodMonitorController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cluster := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if utils.ManagedByExternalController(cluster.Spec.ManagedBy) != nil {
		return ctrl.Result{}, nil
	}
	if err := r.reconcilePodMonitor(ctx, cluster); err != nil {
		if errors.IsAlreadyExists(err) {
			return ctrl.Result{RequeueAfter: podMonitorRetryInterval}, nil
		}
		logger := ctrl.LoggerFrom(ctx)
		if meta.IsNoMatchError(err) || errors.IsNotFound(err) {
			logger.V(1).Info("PodMonitor API is unavailable; skipping monitoring", "error", err)
			return ctrl.Result{RequeueAfter: podMonitorRetryInterval}, nil
		}
		logger.Error(err, "Unable to reconcile PodMonitor; RayCluster reconciliation is unaffected")
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "PodMonitorReconcileFailed", string(utils.ReconcileAction), "%s", err)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *PodMonitorController) reconcilePodMonitor(ctx context.Context, cluster *rayv1.RayCluster) error {
	// Finalizers can delay cluster deletion; stop scraping without waiting for GC.
	disabled := cluster.DeletionTimestamp != nil || r.cleanupOnly
	if !disabled {
		var err error
		disabled, err = podMonitorDisabled(ctx, r.Client, cluster)
		if err != nil {
			return err
		}
	}

	monitor := &monitoringv1.PodMonitor{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-podmonitor"}
	err := r.Get(ctx, key, monitor)
	if errors.IsNotFound(err) {
		if disabled {
			return nil
		}
		desired := desiredPodMonitor(cluster)
		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme(), controllerutil.WithBlockOwnerDeletion(false)); err != nil {
			return err
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(monitor, cluster) {
		message := fmt.Sprintf("PodMonitor %s already exists and is not owned by this RayCluster; skipping", key)
		ctrl.LoggerFrom(ctx).Info(message)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "PodMonitorOwnershipConflict", string(utils.ReconcileAction), "%s", message)
		return errors.NewAlreadyExists(monitoringv1.Resource("podmonitors"), key.Name)
	}
	// Optional monitoring must not hold up foreground RayCluster deletion.
	if owner := metav1.GetControllerOfNoCopy(monitor); ptr.Deref(owner.BlockOwnerDeletion, false) {
		owner.BlockOwnerDeletion = new(false)
		if err := r.Update(ctx, monitor); err != nil {
			return err
		}
	}
	if disabled {
		// Preconditions prevent deleting a replacement object created after our read.
		return client.IgnoreNotFound(r.Delete(ctx, monitor, client.Preconditions{UID: &monitor.UID, ResourceVersion: &monitor.ResourceVersion}))
	}

	desired := desiredPodMonitor(cluster)
	if equality.Semantic.DeepEqual(monitor.Spec, desired.Spec) && monitor.Labels[podMonitorScrapeLabel] == "false" {
		return nil
	}
	monitor.Spec = desired.Spec
	if monitor.Labels == nil {
		monitor.Labels = make(map[string]string)
	}
	monitor.Labels[podMonitorScrapeLabel] = "false"
	return r.Update(ctx, monitor)
}

func podMonitorDisabled(ctx context.Context, reader client.Reader, cluster *rayv1.RayCluster) (bool, error) {
	disabled := cluster.Annotations[podMonitorOptOutAnnotation] == "disabled"
	if owner := podMonitorRayJobOwner(cluster); owner != nil {
		job := &rayv1.RayJob{}
		err := reader.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: owner.Name}, job)
		if err != nil && !errors.IsNotFound(err) {
			return false, err
		}
		if err == nil && job.UID == owner.UID {
			// Use the live parent; a copied cluster annotation may be stale.
			disabled = job.Annotations[podMonitorOptOutAnnotation] == "disabled"
		}
	}
	return disabled, nil
}

func desiredPodMonitor(cluster *rayv1.RayCluster) *monitoringv1.PodMonitor {
	endpoint := monitoringv1.PodMetricsEndpoint{
		Port:   ptr.To(utils.MetricsPortName),
		Path:   "/metrics",
		Scheme: ptr.To(monitoringv1.Scheme("http")),
	}
	if owner := podMonitorRayJobOwner(cluster); owner != nil {
		// The RayJob owns this entire cluster; all its head and worker targets
		// share that lineage. Origin labels are not automatically copied to Pods.
		endpoint.RelabelConfigs = []monitoringv1.RelabelConfig{{
			TargetLabel: "rayjob",
			Replacement: new(owner.Name),
			Action:      "replace",
		}}
	}
	return &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-podmonitor",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{podMonitorScrapeLabel: "false"},
		},
		Spec: monitoringv1.PodMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{utils.RayClusterLabelKey: cluster.Name},
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      utils.RayNodeTypeLabelKey,
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{string(rayv1.HeadNode), string(rayv1.WorkerNode)},
				}},
			},
			PodTargetLabels: []string{
				utils.RayClusterLabelKey,
				utils.RayNodeTypeLabelKey,
				utils.RayNodeGroupLabelKey,
			},
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{endpoint},
		},
	}
}

func podMonitorRayJobOwner(cluster *rayv1.RayCluster) *metav1.OwnerReference {
	owner := metav1.GetControllerOf(cluster)
	if owner != nil && owner.Kind == "RayJob" &&
		(owner.APIVersion == rayv1.GroupVersion.String() || owner.APIVersion == "ray.io/v1alpha1") {
		return owner
	}
	return nil
}

// SetupWithManager isolates optional watches from the workload manager's cache
// and startup errors. Only the elected leader runs monitoring; failures retry.
func (r *PodMonitorController) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int, namespaces ...string) error {
	watchNamespaces := make(map[string]cache.Config, len(namespaces))
	for _, namespace := range namespaces {
		watchNamespaces[namespace] = cache.Config{}
	}
	return mgr.Add(&podMonitorTask{run: func(ctx context.Context) error {
		options := mgr.GetControllerOptions()
		// Retries replace the stopped controller using the same metrics name.
		options.SkipNameValidation = new(true)
		monitoring, err := ctrl.NewManager(mgr.GetConfig(), ctrl.Options{
			Scheme: mgr.GetScheme(), Controller: options,
			Cache:   cache.Options{DefaultNamespaces: watchNamespaces},
			Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		})
		if err != nil {
			return err
		}
		controller := *r
		controller.Client = monitoring.GetClient()
		if err := controller.setupController(ctx, monitoring, reconcileConcurrency, namespaces...); err != nil {
			return err
		}
		return monitoring.Start(ctx)
	}})
}

func (r *PodMonitorController) setupController(ctx context.Context, mgr ctrl.Manager, reconcileConcurrency int, namespaces ...string) error {
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	probe, err := client.NewWithWatch(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme(), Mapper: mgr.GetRESTMapper()})
	if err != nil {
		return err
	}
	for _, namespace := range namespaces {
		if err := mgr.GetAPIReader().List(ctx, &monitoringv1.PodMonitorList{}, client.InNamespace(namespace), client.Limit(1)); err != nil {
			return err
		}
		watcher, err := probe.Watch(ctx, &monitoringv1.PodMonitorList{}, client.InNamespace(namespace))
		if err != nil {
			return err
		}
		watcher.Stop()
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &rayv1.RayCluster{}, podMonitorRayJobIndex, podMonitorRayJobIndexValues); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("podmonitor").
		For(&rayv1.RayCluster{}).
		Owns(&monitoringv1.PodMonitor{}).
		Watches(&rayv1.RayJob{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, job client.Object) []ctrl.Request {
			return rayJobClusters(ctx, r.Client, job)
		}), builder.WithPredicates(podMonitorRayJobPredicate())).
		WithOptions(controller.Options{MaxConcurrentReconciles: reconcileConcurrency}).
		Complete(r)
}

// Keep lifecycle events; status and unrelated metadata updates do not affect monitoring.
func podMonitorRayJobPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return e.ObjectOld.GetUID() != e.ObjectNew.GetUID() ||
				e.ObjectOld.GetAnnotations()[podMonitorOptOutAnnotation] != e.ObjectNew.GetAnnotations()[podMonitorOptOutAnnotation]
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func podMonitorRayJobIndexValues(obj client.Object) []string {
	if owner := podMonitorRayJobOwner(obj.(*rayv1.RayCluster)); owner != nil {
		return []string{string(owner.UID)}
	}
	return nil
}

func rayJobClusters(ctx context.Context, reader client.Reader, job client.Object) []ctrl.Request {
	clusters := &rayv1.RayClusterList{}
	if err := reader.List(ctx, clusters, client.InNamespace(job.GetNamespace()), client.MatchingFields{podMonitorRayJobIndex: string(job.GetUID())}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Unable to find RayClusters for RayJob monitoring")
		return nil
	}
	requests := make([]ctrl.Request, 0, len(clusters.Items))
	for _, cluster := range clusters.Items {
		requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&cluster)})
	}
	return requests
}

// CleanupWithManager removes existing owned monitors once on a disabled rollout.
// It uses direct reads without installing an optional-API informer; only failures retry.
func (r *PodMonitorController) CleanupWithManager(mgr ctrl.Manager) error {
	c, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme(), Mapper: mgr.GetRESTMapper()})
	if err != nil {
		return err
	}
	cleanup := *r
	cleanup.Client, cleanup.cleanupOnly = c, true
	return mgr.Add(&podMonitorTask{run: (&podMonitorCleanup{PodMonitorController: &cleanup, clusters: r.Client}).cleanup})
}

type podMonitorCleanup struct {
	*PodMonitorController
	clusters client.Reader
}

type podMonitorTask struct {
	run func(context.Context) error
}

func (r *podMonitorTask) NeedLeaderElection() bool { return true }

func (r *podMonitorTask) Start(ctx context.Context) error {
	for {
		err := r.run(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		ctrl.LoggerFrom(ctx).Error(err, "PodMonitor task failed; retrying without affecting workloads")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(podMonitorRetryInterval):
		}
	}
}

func (r *podMonitorCleanup) cleanup(ctx context.Context) error {
	clusters := &rayv1.RayClusterList{}
	if err := r.clusters.List(ctx, clusters); err != nil {
		return err
	}
	var failures []error
	for _, cluster := range clusters.Items {
		if utils.ManagedByExternalController(cluster.Spec.ManagedBy) != nil {
			continue
		}
		if err := r.reconcilePodMonitor(ctx, &cluster); err != nil {
			if errors.IsAlreadyExists(err) || errors.IsNotFound(err) {
				continue
			}
			if meta.IsNoMatchError(err) {
				return stderrors.Join(failures...)
			}
			r.Recorder.Eventf(&cluster, nil, corev1.EventTypeWarning, "PodMonitorReconcileFailed", string(utils.ReconcileAction), "%s", err)
			failures = append(failures, err)
		}
	}
	return stderrors.Join(failures...)
}
