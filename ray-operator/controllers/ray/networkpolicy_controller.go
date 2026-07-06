package ray

import (
	"context"
	"fmt"
	"os"

	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// NetworkPolicyController is a completely independent controller that watches RayCluster
// resources and manages NetworkPolicies for them.
type NetworkPolicyController struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	RESTMapper meta.RESTMapper
}

// NewNetworkPolicyController creates a new independent NetworkPolicy controller
func NewNetworkPolicyController(mgr manager.Manager) *NetworkPolicyController {
	return &NetworkPolicyController{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Recorder:   mgr.GetEventRecorderFor("networkpolicy-controller"),
		RESTMapper: mgr.GetRESTMapper(),
	}
}

// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch

// Reconcile handles RayCluster resources and creates/manages NetworkPolicies
func (r *NetworkPolicyController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")

	// Fetch the RayCluster instance
	instance := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			// RayCluster was deleted - NetworkPolicies will be garbage collected automatically
			logger.Info("RayCluster not found, NetworkPolicies will be garbage collected")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if RayCluster is being deleted
	if instance.DeletionTimestamp != nil {
		logger.Info("RayCluster is being deleted, NetworkPolicies will be garbage collected")
		return ctrl.Result{}, nil
	}

	// Check if NetworkPolicy is enabled via annotation
	if !r.isSecureTrustedNetworkEnabled(instance) {
		logger.V(1).Info("NetworkPolicy not enabled for RayCluster", "cluster", instance.Name,
			"annotation", utils.EnableSecureTrustedNetworkAnnotationKey)
		// If NetworkPolicies exist but annotation is removed, clean them up
		return r.cleanupNetworkPoliciesIfNeeded(ctx, instance)
	}

	logger.Info("Reconciling NetworkPolicies for RayCluster", "cluster", instance.Name)

	// Get KubeRay operator namespaces
	kubeRayNamespaces := r.getKubeRayNamespaces(ctx)

	// Create or update head NetworkPolicy
	headNetworkPolicy := r.buildHeadNetworkPolicy(ctx, instance, kubeRayNamespaces)
	if err := r.createOrUpdateNetworkPolicy(ctx, instance, headNetworkPolicy); err != nil {
		return ctrl.Result{}, err
	}

	// Create or update worker NetworkPolicy
	workerNetworkPolicy := r.buildWorkerNetworkPolicy(instance)
	if err := r.createOrUpdateNetworkPolicy(ctx, instance, workerNetworkPolicy); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Successfully reconciled NetworkPolicies for RayCluster", "cluster", instance.Name)
	return ctrl.Result{}, nil
}

// getKubeRayNamespaces returns the list of KubeRay operator namespaces
// OpenShift: APPLICATION_NAMESPACE (from ODH operator) → POD_NAMESPACE → ODH/RHODS fallback
// Non-OpenShift: POD_NAMESPACE → ray-system fallback
func (r *NetworkPolicyController) getKubeRayNamespaces(ctx context.Context) []string {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")

	// On OpenShift, use stricter namespace detection
	if r.isOpenShift() {
		logger.V(1).Info("Detected OpenShift platform")

		// 1. Check APPLICATION_NAMESPACE env var (set by ODH operator via params.env)
		if appNs := os.Getenv("APPLICATION_NAMESPACE"); appNs != "" {
			logger.V(1).Info("Using APPLICATION_NAMESPACE from environment", "namespace", appNs)
			return []string{appNs}
		}

		// 2. Fallback to POD_NAMESPACE
		if podNs := os.Getenv("POD_NAMESPACE"); podNs != "" {
			logger.V(1).Info("Using POD_NAMESPACE", "namespace", podNs)
			return []string{podNs}
		}

		// 3. Final fallback for OpenShift
		logger.Info("Using default ODH/RHODS namespaces")
		return []string{"redhat-ods-applications", "opendatahub"}
	}

	// Non-OpenShift: simpler fallback chain
	if podNs := os.Getenv("POD_NAMESPACE"); podNs != "" {
		logger.V(1).Info("Using POD_NAMESPACE", "namespace", podNs)
		return []string{podNs}
	}

	logger.V(1).Info("Using default namespace", "namespace", "ray-system")
	return []string{"ray-system"}
}

// isOpenShift checks if the cluster is running on OpenShift
func (r *NetworkPolicyController) isOpenShift() bool {
	gvk := routev1.GroupVersion.WithKind("Route")
	_, err := r.RESTMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// createOrUpdateNetworkPolicy creates or updates a NetworkPolicy
func (r *NetworkPolicyController) createOrUpdateNetworkPolicy(ctx context.Context, instance *rayv1.RayCluster, networkPolicy *networkingv1.NetworkPolicy) error {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")

	// Set owner reference for garbage collection
	if err := controllerutil.SetControllerReference(instance, networkPolicy, r.Scheme); err != nil {
		return err
	}

	// Try to create the NetworkPolicy
	if err := r.Create(ctx, networkPolicy); err != nil {
		if errors.IsAlreadyExists(err) {
			// NetworkPolicy exists, update it
			existing := &networkingv1.NetworkPolicy{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(networkPolicy), existing); err != nil {
				return err
			}

			// Ensure controller owner reference is set
			if err := controllerutil.SetControllerReference(instance, existing, r.Scheme); err != nil {
				return err
			}

			// Update the existing NetworkPolicy
			existing.Spec = networkPolicy.Spec
			existing.Labels = networkPolicy.Labels

			if err := r.Update(ctx, existing); err != nil {
				r.Recorder.Eventf(instance, corev1.EventTypeWarning, string(utils.FailedToCreateNetworkPolicy),
					"Failed to update NetworkPolicy %s/%s: %v", networkPolicy.Namespace, networkPolicy.Name, err)
				return err
			}

			logger.Info("Successfully updated NetworkPolicy", "name", networkPolicy.Name)
			r.Recorder.Eventf(instance, corev1.EventTypeNormal, string(utils.CreatedNetworkPolicy),
				"Updated NetworkPolicy %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		}
	}

	return nil
}

// buildHeadNetworkPolicy creates a NetworkPolicy for Ray head pods
func (r *NetworkPolicyController) buildHeadNetworkPolicy(ctx context.Context, instance *rayv1.RayCluster, kubeRayNamespaces []string) *networkingv1.NetworkPolicy {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")
	labels := map[string]string{
		utils.RayClusterLabelKey:                instance.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}

	// Build secured ports - only mTLS port 8443 should be accessible from anywhere
	// Port 10001 is already covered by Rules 1-3 (intra-cluster, same-namespace, operator)
	allSecuredPorts := []networkingv1.NetworkPolicyPort{
		{
			Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
			Port:     &[]intstr.IntOrString{intstr.FromInt(8443)}[0],
		},
	}

	// Build ingress rules
	ingressRules := []networkingv1.NetworkPolicyIngressRule{
		// Rule 1: Intra-cluster communication - NO PORTS (allows all ports)
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
			// No Ports specified = allow all ports
		},
		// Rule 2: External access to dashboard and client ports from any pod in namespace
		{
			From: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{
						// Empty MatchLabels = any pod in same namespace
					},
				},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(10001)}[0], // Client
				},
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8265)}[0], // Dashboard
				},
			},
		},
		// Rule 3: KubeRay operator access
		{
			From: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
						},
					},
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      corev1.LabelMetadataName,
								Operator: metav1.LabelSelectorOpIn,
								Values:   kubeRayNamespaces,
							},
						},
					},
				},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8265)}[0], // Dashboard
				},
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(10001)}[0], // Client
				},
			},
		},
		// Rule 5: Secured ports - NO FROM (allows all)
		{
			Ports: allSecuredPorts,
			// No From specified = allow from anywhere
		},
	}

	// Rule 4: Monitoring access (optional, set by ODH operator via MONITORING_NAMESPACE env var)
	if monitoringNamespace := os.Getenv("MONITORING_NAMESPACE"); monitoringNamespace != "" {
		logger.V(1).Info("Adding monitoring access rule", "namespace", monitoringNamespace)
		monitoringRule := networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{
				{
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      corev1.LabelMetadataName,
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{monitoringNamespace},
							},
						},
					},
				},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8080)}[0], // Metrics
				},
			},
		}
		// Insert monitoring rule before secured ports rule (which is now last)
		ingressRules = append(ingressRules[:len(ingressRules)-1], monitoringRule, ingressRules[len(ingressRules)-1])
	} else {
		logger.V(1).Info("Skipping monitoring access rule - monitoring namespace not configured in DSCI")
	}

	// Rule 4b: Gateway access (optional, for Gateway API/RHOAI v3.0+ setups)
	// Allows Gateway pods to access dashboard on port 8265
	gatewayNamespace := os.Getenv("GATEWAY_NAMESPACE")
	if gatewayNamespace == "" && r.isOpenShift() {
		gatewayNamespace = "openshift-ingress"
	}

	if gatewayNamespace != "" {
		logger.V(1).Info("Adding Gateway access rule", "namespace", gatewayNamespace)
		gatewayRule := networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{
				{
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      corev1.LabelMetadataName,
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{gatewayNamespace},
							},
						},
					},
				},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8265)}[0], // Dashboard
				},
			},
		}
		// Insert gateway rule before secured ports rule (which is now last)
		ingressRules = append(ingressRules[:len(ingressRules)-1], gatewayRule, ingressRules[len(ingressRules)-1])
	}

	// Add RayJob submitter peer if RayCluster is owned by RayJob
	if rayJobPeer := r.buildRayJobPeer(instance); rayJobPeer != nil {
		ingressRules = append(ingressRules, networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{*rayJobPeer},
		})
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-head", instance.Name),
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:  instance.Name,
					utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     ingressRules,
		},
	}
}

// buildWorkerNetworkPolicy creates a NetworkPolicy for Ray worker pods
func (r *NetworkPolicyController) buildWorkerNetworkPolicy(instance *rayv1.RayCluster) *networkingv1.NetworkPolicy {
	labels := map[string]string{
		utils.RayClusterLabelKey:                instance.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-workers", instance.Name),
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:  instance.Name,
					utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode),
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
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
			},
		},
	}
}

// buildRayJobPeer creates a NetworkPolicy peer for RayJob submitter pods
// Returns nil if RayCluster is not owned by RayJob
func (r *NetworkPolicyController) buildRayJobPeer(instance *rayv1.RayCluster) *networkingv1.NetworkPolicyPeer {
	// Check if RayCluster is owned by RayJob
	for _, ownerRef := range instance.OwnerReferences {
		if ownerRef.Kind == "RayJob" {
			// Return peer for RayJob submitter pods
			return &networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"batch.kubernetes.io/job-name": ownerRef.Name,
					},
				},
			}
		}
	}
	// No RayJob owner = no RayJob submitter pods to allow
	return nil
}

// SetupWithManager sets up the controller with the Manager
func (r *NetworkPolicyController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayCluster{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("networkpolicy").
		Complete(r)
}

// isSecuredTrustedNetworkEnabled checks if NetworkPolicy is enabled for this RayCluster via annotation
func (r *NetworkPolicyController) isSecureTrustedNetworkEnabled(instance *rayv1.RayCluster) bool {
	if instance.Annotations == nil {
		return false
	}

	value, exists := instance.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey]
	if !exists {
		return false
	}

	// Accept "true", "1", "yes", "on" as truthy values (case-insensitive)
	switch value {
	case "true", "1", "yes", "on", "True", "TRUE", "Yes", "YES", "On", "ON":
		return true
	default:
		return false
	}
}

// cleanupNetworkPoliciesIfNeeded removes NetworkPolicies if they exist but annotation is disabled
func (r *NetworkPolicyController) cleanupNetworkPoliciesIfNeeded(ctx context.Context, instance *rayv1.RayCluster) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")

	// Try to delete head NetworkPolicy if it exists
	headNetworkPolicy := &networkingv1.NetworkPolicy{}
	headName := fmt.Sprintf("%s-head", instance.Name)
	headKey := client.ObjectKey{Namespace: instance.Namespace, Name: headName}

	if err := r.Get(ctx, headKey, headNetworkPolicy); err == nil {
		// NetworkPolicy exists, delete it
		if err := r.Delete(ctx, headNetworkPolicy); err != nil {
			logger.Error(err, "Failed to delete head NetworkPolicy", "name", headName)
			return ctrl.Result{}, err
		}
		logger.Info("Deleted head NetworkPolicy", "name", headName)
		r.Recorder.Eventf(instance, corev1.EventTypeNormal, string(utils.DeletedNetworkPolicy),
			"Deleted NetworkPolicy %s/%s", instance.Namespace, headName)
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// Try to delete worker NetworkPolicy if it exists
	workerNetworkPolicy := &networkingv1.NetworkPolicy{}
	workerName := fmt.Sprintf("%s-workers", instance.Name)
	workerKey := client.ObjectKey{Namespace: instance.Namespace, Name: workerName}

	if err := r.Get(ctx, workerKey, workerNetworkPolicy); err == nil {
		// NetworkPolicy exists, delete it
		if err := r.Delete(ctx, workerNetworkPolicy); err != nil {
			logger.Error(err, "Failed to delete worker NetworkPolicy", "name", workerName)
			return ctrl.Result{}, err
		}
		logger.Info("Deleted worker NetworkPolicy", "name", workerName)
		r.Recorder.Eventf(instance, corev1.EventTypeNormal, string(utils.DeletedNetworkPolicy),
			"Deleted NetworkPolicy %s/%s", instance.Namespace, workerName)
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}
