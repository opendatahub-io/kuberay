package ray

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync" // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
	"time" // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features" // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
)

// NetworkPolicyController is an independent controller that watches RayCluster
// resources and manages NetworkPolicies for them.
type NetworkPolicyController struct {
	client.Client
	Scheme                 *runtime.Scheme
	Recorder               events.EventRecorder
	isOpenShift            bool                     // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
	legacyMigrationMutex   sync.Mutex               // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
	legacyMigrationRetries map[client.ObjectKey]int // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
}

// DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
const (
	legacyNetworkPolicyMigrationRequeueInterval = 10 * time.Second
	legacyNetworkPolicyMigrationMaxRetries      = 6
	legacyNetworkPolicyMigrationRetryInterval   = time.Minute
)

// DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
func NewNetworkPolicyController(mgr manager.Manager, openShift ...bool) (*NetworkPolicyController, error) {
	isOpenShift := false
	if len(openShift) > 0 {
		isOpenShift = openShift[0]
	}
	return &NetworkPolicyController{
		Client:                 mgr.GetClient(),
		Scheme:                 mgr.GetScheme(),
		Recorder:               mgr.GetEventRecorder("networkpolicy-controller"),
		isOpenShift:            isOpenShift, // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
		legacyMigrationRetries: make(map[client.ObjectKey]int),
	}, nil
}

// DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
func (r *NetworkPolicyController) shouldRequeueLegacyMigration(key client.ObjectKey) (requeue, warn bool) {
	r.legacyMigrationMutex.Lock()
	defer r.legacyMigrationMutex.Unlock()

	if r.legacyMigrationRetries == nil {
		r.legacyMigrationRetries = make(map[client.ObjectKey]int)
	}

	retries := r.legacyMigrationRetries[key]
	if retries >= legacyNetworkPolicyMigrationMaxRetries {
		// Emit the warning once per migration retry sequence, while continuing
		// the slower retries for as long as migration remains incomplete.
		if retries == legacyNetworkPolicyMigrationMaxRetries {
			r.legacyMigrationRetries[key] = retries + 1
			return false, true
		}
		return false, false
	}
	r.legacyMigrationRetries[key] = retries + 1
	return true, false
}

// DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
func (r *NetworkPolicyController) resetLegacyMigration(key client.ObjectKey) {
	r.legacyMigrationMutex.Lock()
	defer r.legacyMigrationMutex.Unlock()

	delete(r.legacyMigrationRetries, key)
}

// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *NetworkPolicyController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	instance := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			r.resetLegacyMigration(req.NamespacedName) // DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
			// RayCluster was deleted - NetworkPolicies will be garbage collected automatically
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if instance.DeletionTimestamp != nil {
		r.resetLegacyMigration(req.NamespacedName)
		return ctrl.Result{}, nil
	}

	if manager := utils.ManagedByExternalController(instance.Spec.ManagedBy); manager != nil {
		r.resetLegacyMigration(req.NamespacedName)
		logger.Info("Skipping RayCluster managed by a custom controller", "managed-by", manager)
		return ctrl.Result{}, nil
	}

	if instance.Spec.NetworkPolicy == nil {
		// DO NOT REMOVE FROM ODH FORK OF KUBERAY: this is only used to preserve legacy OpenShift NetworkPolicies during migration
		if r.isOpenShift && features.Enabled(features.RayClusterNetworkPolicy) &&
			utils.IsLegacySecurityEnabled(instance) {
			requeue, warn := r.shouldRequeueLegacyMigration(req.NamespacedName)
			if requeue {
				logger.Info("NetworkPolicy migration pending for legacy OpenShift RayCluster; preserving existing policies", "cluster", instance.Name, "namespace", instance.Namespace)
				return ctrl.Result{RequeueAfter: legacyNetworkPolicyMigrationRequeueInterval}, nil
			}
			// Keep the legacy policies indefinitely if the migration has not completed.
			// Deleting them after the retry window would leave the cluster without
			// network isolation when the legacy-security controller is delayed or
			// unavailable.
			if warn {
				logger.Info("NetworkPolicy migration grace period expired for legacy OpenShift RayCluster; preserving existing policies", "cluster", instance.Name, "namespace", instance.Namespace)
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, "LegacySecurityMigrationPending", string(utils.ReconcileAction),
					"Legacy security migration has not completed; existing NetworkPolicies are preserved. Check the CRDs and legacy-security controller")
			}
			return ctrl.Result{RequeueAfter: legacyNetworkPolicyMigrationRetryInterval}, nil
		}
		r.resetLegacyMigration(req.NamespacedName) // OPENSHIFT SPECIFIC
		logger.V(1).Info("NetworkPolicy not configured for RayCluster", "cluster", instance.Name, "namespace", instance.Namespace)
		// If NetworkPolicies exist but NetworkPolicy config is removed, clean them up
		_, err := r.deleteStaleNetworkPolicies(ctx, instance, nil)
		return ctrl.Result{}, err
	}
	r.resetLegacyMigration(req.NamespacedName) // OPENSHIFT SPECIFIC

	logger.Info("Reconciling NetworkPolicies for RayCluster", "cluster", instance.Name, "namespace", instance.Namespace)

	// Determine mode (default to DenyAll)
	mode := rayv1.NetworkPolicyDenyAll
	if instance.Spec.NetworkPolicy.Mode != nil {
		mode = *instance.Spec.NetworkPolicy.Mode
	}

	headNetworkPolicy := r.buildHeadNetworkPolicy(instance, mode)
	if err := r.createOrUpdateNetworkPolicy(ctx, instance, headNetworkPolicy); err != nil {
		return ctrl.Result{}, err
	}

	desiredNames := map[string]bool{headNetworkPolicy.Name: true}
	for _, group := range instance.Spec.WorkerGroupSpecs {
		groupNetworkPolicy := r.buildWorkerGroupNetworkPolicy(instance, mode, group.GroupName)
		desiredNames[groupNetworkPolicy.Name] = true
		if err := r.createOrUpdateNetworkPolicy(ctx, instance, groupNetworkPolicy); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Keep stale policies while they are the only isolation for live pods from
	// removed worker groups. UpgradeStrategy None can leave those pods running.
	retained, err := r.deleteStaleNetworkPolicies(ctx, instance, desiredNames)
	if err != nil {
		return ctrl.Result{}, err
	}
	if retained {
		return ctrl.Result{RequeueAfter: legacyNetworkPolicyMigrationRetryInterval}, nil
	}

	logger.Info("Successfully reconciled NetworkPolicies for RayCluster", "cluster", instance.Name, "namespace", instance.Namespace)
	return ctrl.Result{}, nil
}

func (r *NetworkPolicyController) createOrUpdateNetworkPolicy(ctx context.Context, instance *rayv1.RayCluster, networkPolicy *networkingv1.NetworkPolicy) error {
	logger := ctrl.LoggerFrom(ctx)

	// Set owner reference for garbage collection
	if err := controllerutil.SetControllerReference(instance, networkPolicy, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, networkPolicy); err != nil {
		if errors.IsAlreadyExists(err) {
			existing := &networkingv1.NetworkPolicy{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(networkPolicy), existing); err != nil {
				return err
			}

			if !metav1.IsControlledBy(existing, instance) {
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.NetworkPolicyNameCollision), string(utils.ValidateAction),
					"NetworkPolicy %s/%s already exists and is not owned by this RayCluster, network isolation will not be applied. "+
						"Rename the existing NetworkPolicy or use a different RayCluster name to avoid the collision",
					networkPolicy.Namespace, networkPolicy.Name)
				return fmt.Errorf("NetworkPolicy %s/%s already exists and is not owned by this RayCluster", networkPolicy.Namespace, networkPolicy.Name)
			}

			normalizeNetworkPolicyPorts(&networkPolicy.Spec)
			if reflect.DeepEqual(existing.Spec, networkPolicy.Spec) && reflect.DeepEqual(existing.Labels, networkPolicy.Labels) {
				logger.V(1).Info("NetworkPolicy already up to date, skipping update", "name", networkPolicy.Name)
				return nil
			}

			existing.Spec = networkPolicy.Spec
			existing.Labels = networkPolicy.Labels

			if err := r.Update(ctx, existing); err != nil {
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateNetworkPolicy), string(utils.UpdateAction),
					"Failed to update NetworkPolicy %s/%s: %v", networkPolicy.Namespace, networkPolicy.Name, err)
				return err
			}

			logger.Info("Successfully updated NetworkPolicy", "name", networkPolicy.Name)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.UpdatedNetworkPolicy), string(utils.UpdateAction),
				"Updated NetworkPolicy %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		} else {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateNetworkPolicy), string(utils.CreateAction),
				"Failed to create NetworkPolicy %s/%s: %v", networkPolicy.Namespace, networkPolicy.Name, err)
			return err
		}
	} else {
		logger.Info("Successfully created NetworkPolicy", "name", networkPolicy.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedNetworkPolicy), string(utils.CreateAction),
			"Created NetworkPolicy %s/%s", networkPolicy.Namespace, networkPolicy.Name)
	}

	return nil
}

func headNetworkPolicyName(clusterName string) string {
	return fmt.Sprintf("%s-head", clusterName)
}

func workerGroupNetworkPolicyName(clusterName, groupName string) string {
	return fmt.Sprintf("%s-workers-%s", clusterName, groupName)
}

func (r *NetworkPolicyController) buildHeadNetworkPolicy(instance *rayv1.RayCluster, mode rayv1.NetworkPolicyMode) *networkingv1.NetworkPolicy {
	var policyTypes []networkingv1.PolicyType
	var ingressRules []networkingv1.NetworkPolicyIngressRule
	var egressRules []networkingv1.NetworkPolicyEgressRule

	// Only restrict ingress when mode includes ingress denial.
	// Including PolicyTypeIngress causes K8s to deny all ingress not covered by a rule.
	if mode == rayv1.NetworkPolicyDenyAll || mode == rayv1.NetworkPolicyDenyAllIngress {
		policyTypes = append(policyTypes, networkingv1.PolicyTypeIngress)
		ingressRules = r.buildHeadIngressRules(instance)
		if instance.Spec.NetworkPolicy.Head != nil {
			ingressRules = append(ingressRules, instance.Spec.NetworkPolicy.Head.IngressRules...)
		}
	}

	// Only restrict egress when mode includes egress denial.
	if mode == rayv1.NetworkPolicyDenyAll || mode == rayv1.NetworkPolicyDenyAllEgress {
		policyTypes = append(policyTypes, networkingv1.PolicyTypeEgress)
		egressRules = r.buildBaseEgressRules(instance)
		if instance.Spec.NetworkPolicy.Head != nil {
			egressRules = append(egressRules, instance.Spec.NetworkPolicy.Head.EgressRules...)
		}
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headNetworkPolicyName(instance.Name),
			Namespace: instance.Namespace,
			Labels:    networkPolicyLabels(instance, utils.RayNodeHeadGroupLabelValue),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:  instance.Name,
					utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
				},
			},
			PolicyTypes: policyTypes,
			Ingress:     ingressRules,
			Egress:      egressRules,
		},
	}
}

func networkPolicyLabels(instance *rayv1.RayCluster, groupName string) map[string]string {
	return map[string]string{
		utils.RayClusterLabelKey:                instance.Name,
		utils.RayNodeGroupLabelKey:              groupName,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}
}

// workerGroupCustomRules returns the custom rules for a worker group:
// spec.networkPolicy.workerGroups entry if present, otherwise the spec.networkPolicy.worker.
func workerGroupCustomRules(networkPolicy *rayv1.NetworkPolicyConfig, groupName string) *rayv1.NetworkPolicyRules {
	for i := range networkPolicy.WorkerGroups {
		if networkPolicy.WorkerGroups[i].GroupName == groupName {
			return &networkPolicy.WorkerGroups[i].NetworkPolicyRules
		}
	}
	return networkPolicy.Worker
}

// buildWorkerGroupNetworkPolicy builds the NetworkPolicy for one worker group.
func (r *NetworkPolicyController) buildWorkerGroupNetworkPolicy(instance *rayv1.RayCluster, mode rayv1.NetworkPolicyMode, groupName string) *networkingv1.NetworkPolicy {
	custom := workerGroupCustomRules(instance.Spec.NetworkPolicy, groupName)

	var policyTypes []networkingv1.PolicyType
	var ingressRules []networkingv1.NetworkPolicyIngressRule
	var egressRules []networkingv1.NetworkPolicyEgressRule

	// Only restrict ingress when mode includes ingress denial.
	if mode == rayv1.NetworkPolicyDenyAll || mode == rayv1.NetworkPolicyDenyAllIngress {
		policyTypes = append(policyTypes, networkingv1.PolicyTypeIngress)
		ingressRules = r.buildBaseIngressRules(instance)
		if custom != nil {
			ingressRules = append(ingressRules, custom.IngressRules...)
		}
	}

	// Only restrict egress when mode includes egress denial.
	if mode == rayv1.NetworkPolicyDenyAll || mode == rayv1.NetworkPolicyDenyAllEgress {
		policyTypes = append(policyTypes, networkingv1.PolicyTypeEgress)
		egressRules = r.buildBaseEgressRules(instance)
		if custom != nil {
			egressRules = append(egressRules, custom.EgressRules...)
		}
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerGroupNetworkPolicyName(instance.Name, groupName),
			Namespace: instance.Namespace,
			Labels:    networkPolicyLabels(instance, groupName),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:   instance.Name,
					utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
					utils.RayNodeGroupLabelKey: groupName,
				},
			},
			PolicyTypes: policyTypes,
			Ingress:     ingressRules,
			Egress:      egressRules,
		},
	}
}

// buildBaseIngressRules returns the intra-cluster ingress rule shared by both
// head and worker NetworkPolicies: allow all ports from pods in the same RayCluster.
func (r *NetworkPolicyController) buildBaseIngressRules(instance *rayv1.RayCluster) []networkingv1.NetworkPolicyIngressRule {
	return []networkingv1.NetworkPolicyIngressRule{
		{
			From: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.RayClusterLabelKey: instance.Name,
						},
					},
				},
			},
		},
	}
}

// buildHeadIngressRules returns the base ingress rules for the head NetworkPolicy:
// intra-cluster communication, and (for K8sJobMode RayJob-owned clusters) the per-job
// submitter rule. Operator access is intentionally omitted here — platforms that need
// it should inject it via spec.networkPolicy.head.ingressRules (e.g. via a mutating webhook).
// For RayClusters that are NOT owned by a RayJob (e.g. clusterSelector use cases),
// users must allow their submitter pods explicitly via NetworkPolicy.Head.IngressRules
// (e.g. a podSelector matching the submitterPodTemplate labels). Users who need any
// other external access must likewise add explicit rules in Head.IngressRules.
func (r *NetworkPolicyController) buildHeadIngressRules(instance *rayv1.RayCluster) []networkingv1.NetworkPolicyIngressRule {
	tcpProtocol := corev1.ProtocolTCP
	dashboardPort := intstr.FromInt32(r.getHeadPort(instance, "dashboard-port", utils.DefaultDashboardPort))

	rules := r.buildBaseIngressRules(instance)

	if peer := r.buildRayJobPeer(instance); peer != nil {
		// Owned by a RayJob: allow the specific submitter pod.
		rules = append(rules, networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{*peer},
			Ports: []networkingv1.NetworkPolicyPort{
				{Protocol: &tcpProtocol, Port: &dashboardPort},
			},
		})
	}

	return rules
}

// buildRayJobPeer returns a NetworkPolicyPeer matching the submitter pod for
// the RayJob that owns this RayCluster, or nil if not applicable.
//
// Only K8sJobMode creates a standalone submitter Job pod that must reach the head
// dashboard from outside the cluster. Other submission modes (Sidecar, Interactive,
// HTTP) have no such pod — Sidecar runs inside the head pod (covered by the
// intra-cluster rule) and the others submit out-of-band — so no submitter ingress
// rule is needed. The submission mode label is stamped on the RayCluster by the
// RayJob controller (see constructRayClusterForRayJob).
func (r *NetworkPolicyController) buildRayJobPeer(instance *rayv1.RayCluster) *networkingv1.NetworkPolicyPeer {
	if instance.Labels[utils.RayJobSubmissionModeLabelKey] != string(rayv1.K8sJobMode) {
		return nil
	}
	for _, ownerRef := range instance.OwnerReferences {
		if ownerRef.Kind == "RayJob" &&
			ownerRef.Controller != nil && *ownerRef.Controller &&
			strings.HasPrefix(ownerRef.APIVersion, "ray.io/") {
			return &networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						utils.RayOriginatedFromCRNameLabelKey: ownerRef.Name,
						utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
					},
				},
			}
		}
	}
	return nil
}

// getHeadPort returns the port number for the given rayStartParams key,
// falling back to defaultPort if the key is absent or not a valid integer.
func (r *NetworkPolicyController) getHeadPort(instance *rayv1.RayCluster, rayStartParamKey string, defaultPort int32) int32 {
	if portStr, ok := instance.Spec.HeadGroupSpec.RayStartParams[rayStartParamKey]; ok {
		if port, err := strconv.ParseInt(portStr, 10, 32); err == nil {
			return int32(port)
		}
	}
	return defaultPort
}

// buildBaseEgressRules creates the base egress rule allowing intra-cluster
// pod-to-pod communication. DNS egress is intentionally NOT baked in: the
// operator does not assume how the cluster's DNS is deployed.
//
// IMPORTANT: under DenyAll/DenyAllEgress this denies DNS by default. Ray workers
// reach the head via its service FQDN (see GenerateFQDNServiceName), so users
// MUST add a DNS egress rule via Head.EgressRules / Worker.EgressRules or the
// cluster will fail to start. See the network-policy-deny-all sample.
func (r *NetworkPolicyController) buildBaseEgressRules(instance *rayv1.RayCluster) []networkingv1.NetworkPolicyEgressRule {
	return []networkingv1.NetworkPolicyEgressRule{
		// Intra-cluster egress (all ports) to pods in the same RayCluster.
		{
			To: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.RayClusterLabelKey: instance.Name,
						},
					},
				},
			},
			// No Ports specified = allow all ports
		},
	}
}

// normalizeNetworkPolicyPorts defaults nil Protocol fields to TCP, matching the
// API server's defaulting behavior. This prevents spurious updates when users
// omit the protocol in custom ingress/egress rules.
func normalizeNetworkPolicyPorts(spec *networkingv1.NetworkPolicySpec) {
	tcp := corev1.ProtocolTCP
	for i := range spec.Ingress {
		for j := range spec.Ingress[i].Ports {
			if spec.Ingress[i].Ports[j].Protocol == nil {
				spec.Ingress[i].Ports[j].Protocol = &tcp
			}
		}
	}
	for i := range spec.Egress {
		for j := range spec.Egress[i].Ports {
			if spec.Egress[i].Ports[j].Protocol == nil {
				spec.Egress[i].Ports[j].Protocol = &tcp
			}
		}
	}
}

// deleteStaleNetworkPolicies deletes NetworkPolicies owned by this RayCluster whose
// names are not in desiredNames.
func (r *NetworkPolicyController) deleteStaleNetworkPolicies(ctx context.Context, instance *rayv1.RayCluster, desiredNames map[string]bool) (bool, error) {
	logger := ctrl.LoggerFrom(ctx)
	retained := false

	networkPolicies := &networkingv1.NetworkPolicyList{}
	if err := r.List(ctx, networkPolicies, client.InNamespace(instance.Namespace),
		client.MatchingLabels{utils.RayClusterLabelKey: instance.Name}); err != nil {
		return retained, err
	}

	for i := range networkPolicies.Items {
		networkPolicy := &networkPolicies.Items[i]
		if desiredNames[networkPolicy.Name] {
			continue
		}
		if !metav1.IsControlledBy(networkPolicy, instance) {
			logger.V(1).Info("NetworkPolicy exists but is not owned by this RayCluster, skipping deletion", "name", networkPolicy.Name)
			continue
		}
		// A nil desired set means the user explicitly disabled NetworkPolicy;
		// only apply the isolation guard during migration or group removal.
		if desiredNames != nil {
			selector, err := metav1.LabelSelectorAsSelector(&networkPolicy.Spec.PodSelector)
			if err != nil {
				return retained, err
			}
			pods := &corev1.PodList{}
			if err := r.List(ctx, pods, client.InNamespace(instance.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
				return retained, err
			}
			uncovered := false
			for _, pod := range pods.Items {
				if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
					continue
				}
				name := ""
				switch rayv1.RayNodeType(pod.Labels[utils.RayNodeTypeLabelKey]) {
				case rayv1.HeadNode:
					name = headNetworkPolicyName(instance.Name)
				case rayv1.WorkerNode:
					name = workerGroupNetworkPolicyName(instance.Name, pod.Labels[utils.RayNodeGroupLabelKey])
				}
				if !desiredNames[name] {
					uncovered = true
					break
				}
			}
			if uncovered {
				retained = true
				logger.Info("Preserving stale NetworkPolicy while live pods still require it", "name", networkPolicy.Name)
				continue
			}
		}
		if err := r.Delete(ctx, networkPolicy); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			logger.Error(err, "Failed to delete NetworkPolicy", "name", networkPolicy.Name)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteNetworkPolicy), string(utils.DeleteAction),
				"Failed to delete NetworkPolicy %s/%s: %v", instance.Namespace, networkPolicy.Name, err)
			return retained, err
		}
		logger.Info("Deleted NetworkPolicy", "name", networkPolicy.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedNetworkPolicy), string(utils.DeleteAction),
			"Deleted NetworkPolicy %s/%s", instance.Namespace, networkPolicy.Name)
	}

	return retained, nil
}

func (r *NetworkPolicyController) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("networkpolicy").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("NetworkPolicy")
				if request != nil {
					logger = logger.WithValues("RayCluster", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}
