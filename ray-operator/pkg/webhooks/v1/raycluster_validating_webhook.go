package v1

import (
	"context"
	"fmt"
	"reflect"

	routev1 "github.com/openshift/api/route/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var rayClusterLog = logf.Log.WithName("raycluster-resource")

// SetupRayClusterWebhookWithManager registers the webhook for RayCluster in the manager.
func SetupRayClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayCluster{}).
		WithValidator(&RayClusterWebhook{RESTMapper: mgr.GetRESTMapper()}).
		Complete()
}

type RayClusterWebhook struct {
	RESTMapper meta.RESTMapper
}

//+kubebuilder:webhook:path=/validate-ray-io-v1-raycluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayclusters,verbs=create;update,versions=v1,name=vraycluster.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*rayv1.RayCluster] = &RayClusterWebhook{}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateCreate(_ context.Context, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate create", "name", rayCluster.Name)
	return nil, w.validateRayCluster(rayCluster)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateUpdate(_ context.Context, oldRayCluster, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate update", "name", rayCluster.Name)
	return nil, w.validateRayClusterUpdate(rayCluster, oldRayCluster)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayCluster) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayClusterWebhook) validateRayCluster(rayCluster *rayv1.RayCluster) error {
	return w.validateRayClusterUpdate(rayCluster, nil)
}

func (w *RayClusterWebhook) validateRayClusterUpdate(rayCluster, oldRayCluster *rayv1.RayCluster) error {
	var allErrs field.ErrorList

	// Detect the supported managed profile from the platform rather than using
	// the legacy annotation as a new opt-in contract. Existing in-place legacy
	// OpenShift clusters are still covered because the OpenShift API surface is
	// present even when their spec.networkPolicy has not been backfilled yet.
	isOpenShift, err := w.isOpenShift()
	if err != nil {
		// A discovery outage should not block unrelated clusters. Requests that
		// already carry the managed profile still fail closed because skipping
		// this validation could bypass the platform policy contract.
		if utils.IsLegacySecurityEnabled(rayCluster) || (oldRayCluster != nil && utils.IsLegacySecurityEnabled(oldRayCluster)) {
			return fmt.Errorf("detect OpenShift platform: %w", err)
		}
		isOpenShift = false
	}
	if isOpenShift {
		if utils.ManagedByExternalController(rayCluster.Spec.ManagedBy) != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec").Child("managedBy"), *rayCluster.Spec.ManagedBy,
				"externally managed RayClusters are not supported by the OpenShift platform profile because KubeRay skips NetworkPolicy reconciliation"))
		} else {
			if err := validateManagedOpenShiftNetworkPolicy(rayCluster, oldRayCluster); err != nil {
				allErrs = append(allErrs, err)
			}
			if err := validateManagedOpenShiftRayPorts(rayCluster); err != nil {
				allErrs = append(allErrs, err)
			}
		}
	}

	if err := utils.ValidateRayClusterMetadata(rayCluster.ObjectMeta); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), rayCluster.Name, err.Error()))
	}

	if err := w.validateWorkerGroups(rayCluster); err != nil {
		allErrs = append(allErrs, err)
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "RayCluster"},
		rayCluster.Name, allErrs)
}

// validateManagedOpenShiftRayPorts ensures a user-supplied dashboard or
// metrics RayStartParam has a matching declared head-container port. Services
// are built from declared container ports, while Routes and NetworkPolicies
// use the effective RayStartParam; accepting a mismatch creates a broken or
// unexpectedly exposed managed endpoint.
func validateManagedOpenShiftRayPorts(rayCluster *rayv1.RayCluster) *field.Error {
	if !utils.IsLegacySecurityEnabled(rayCluster) {
		return nil
	}
	params := rayCluster.Spec.HeadGroupSpec.RayStartParams
	metricsPort := utils.EffectiveRayStartParamPort(params, "metrics-export-port", utils.DefaultMetricsPort)
	if len(rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers) > utils.RayContainerIndex {
		metricsPort = utils.FindContainerPort(&rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex], utils.MetricsPortName, metricsPort)
	}
	dashboardPort := utils.EffectiveRayStartParamPort(params, "dashboard-port", utils.DefaultDashboardPort)
	if metricsPort == 8443 || metricsPort == utils.DefaultClientPort || metricsPort == dashboardPort {
		return field.Invalid(field.NewPath("spec").Child("headGroupSpec"), metricsPort,
			"managed head metrics port conflicts with a platform dashboard, Ray client, or 8443 compatibility ingress rule")
	}
	if len(params) == 0 {
		return nil
	}
	// If no head container is supplied, pod construction will inject the
	// operator's standard container and ports. A custom effective port would
	// therefore make the generated Service disagree with the Ray endpoint.
	if len(rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers) <= utils.RayContainerIndex {
		for _, port := range []struct {
			key         string
			defaultPort int32
		}{
			{key: "dashboard-port", defaultPort: utils.DefaultDashboardPort},
			{key: "metrics-export-port", defaultPort: utils.DefaultMetricsPort},
		} {
			if _, supplied := params[port.key]; supplied && utils.EffectiveRayStartParamPort(params, port.key, port.defaultPort) != port.defaultPort {
				return field.Invalid(field.NewPath("spec").Child("headGroupSpec").Child("rayStartParams").Key(port.key), params[port.key],
					fmt.Sprintf("managed OpenShift RayClusters must declare a head container when overriding port %d", port.defaultPort))
			}
		}
		return nil
	}

	headContainer := &rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex]
	for _, port := range []struct {
		key         string
		name        string
		defaultPort int32
	}{
		{key: "dashboard-port", name: utils.DashboardPortName, defaultPort: utils.DefaultDashboardPort},
		{key: "metrics-export-port", name: utils.MetricsPortName, defaultPort: utils.DefaultMetricsPort},
	} {
		value, supplied := params[port.key]
		if !supplied {
			continue
		}
		effective := utils.EffectiveRayStartParamPort(params, port.key, port.defaultPort)
		declared := utils.FindContainerPort(headContainer, port.name, -1)
		if declared != effective {
			return field.Invalid(field.NewPath("spec").Child("headGroupSpec").Child("rayStartParams").Key(port.key), value,
				fmt.Sprintf("managed OpenShift RayClusters must declare head container port %d named %q", effective, port.name))
		}
	}
	return nil
}

func (w *RayClusterWebhook) isOpenShift() (bool, error) {
	if w.RESTMapper == nil {
		return false, nil
	}
	_, err := w.RESTMapper.RESTMapping(routev1.GroupVersion.WithKind("Route").GroupKind(), routev1.GroupVersion.Version)
	if err == nil {
		return true, nil
	}
	if meta.IsNoMatchError(err) {
		return false, nil
	}
	return false, err
}

// validateManagedOpenShiftNetworkPolicy enforces the OpenShift/RHOAI platform
// policy contract. Managed clusters may not add or remove platform policy
// rules.
func validateManagedOpenShiftNetworkPolicy(rayCluster, oldRayCluster *rayv1.RayCluster) *field.Error {
	if !utils.IsLegacySecurityEnabled(rayCluster) {
		return nil
	}

	networkPolicyPath := field.NewPath("spec").Child("networkPolicy")
	networkPolicy := rayCluster.Spec.NetworkPolicy
	if networkPolicy == nil {
		// Existing pre-webhook clusters can temporarily reach this state while
		// LegacySecurityController backfills the native field. Do not block that
		// migration; the controller and NetworkPolicy preservation guard own it.
		// TODO(ODH NetworkPolicy): Add an admission/migration integration test
		// proving that validation cannot strand a legacy 1.6 cluster in this state.
		return nil
	}

	if networkPolicy.Mode == nil || *networkPolicy.Mode != rayv1.NetworkPolicyDenyAllIngress {
		var mode any
		if networkPolicy.Mode != nil {
			mode = *networkPolicy.Mode
		}
		return field.Invalid(networkPolicyPath.Child("mode"), mode,
			"managed OpenShift RayClusters must use DenyAllIngress; egress isolation is not part of the supported profile")
	}

	if networkPolicy.Head == nil {
		return field.Required(networkPolicyPath.Child("head"), "managed OpenShift RayClusters require the platform head ingress rules")
	}

	// Recompute the current platform defaults from the effective Ray ports.
	// During a port transition, previous platform rules remain temporarily
	// allowed in the spec ceiling so reconciliation can preserve connectivity,
	// but only the current rules are required in the new spec.
	defaults := rayCluster.DeepCopy()
	defaults.Spec.NetworkPolicy = nil
	utils.EnsureOpenShiftRayClusterNetworkPolicy(defaults)
	allowedHeadRules := defaults.Spec.NetworkPolicy.Head
	requiredHeadRules := defaults.Spec.NetworkPolicy.Head
	if oldRayCluster != nil && oldRayCluster.Spec.NetworkPolicy != nil && oldRayCluster.Spec.NetworkPolicy.Head != nil {
		allowedHeadRules = allowedHeadRules.DeepCopy()
		allowedHeadRules.IngressRules = appendUniqueManagedIngressRules(allowedHeadRules.IngressRules, oldRayCluster.Spec.NetworkPolicy.Head.IngressRules...)
	}
	if err := validateManagedRules(
		networkPolicy.Head,
		allowedHeadRules,
		requiredHeadRules,
		networkPolicyPath.Child("head"),
	); err != nil {
		return err
	}
	if err := validateManagedRules(networkPolicy.Worker, nil, nil, networkPolicyPath.Child("worker")); err != nil {
		return err
	}
	for i := range networkPolicy.WorkerGroups {
		if err := validateManagedRules(
			&networkPolicy.WorkerGroups[i].NetworkPolicyRules,
			nil,
			nil,
			networkPolicyPath.Child("workerGroups").Index(i),
		); err != nil {
			return err
		}
	}

	// TODO(ODH NetworkPolicy): The source-less 8443 compatibility rule and the
	// empty same-namespace dashboard/client rule are intentionally included in
	// the current baseline. Tighten them only after the gateway, kube-rbac-proxy,
	// RayJob submitter, and workbench caller contracts are validated.
	// When RayStartParams change, prior platform rules remain allowed temporarily
	// in the spec ceiling, while only current rules are required. The
	// NetworkPolicy controller regenerates the live managed policy from the
	// current platform rules rather than retaining prior spec rules.
	return nil
}

// validateManagedRules makes the OpenShift platform defaults an exact ceiling
// for the NetworkPolicy rule extensions in the RayCluster spec. The native
// KubeRay policy API remains unchanged for non-OpenShift clusters; this
// restriction applies only to the supported managed platform profile.
func validateManagedRules(rules, allowed, required *rayv1.NetworkPolicyRules, path *field.Path) *field.Error {
	if rules == nil {
		return nil
	}
	if len(rules.EgressRules) > 0 {
		return field.Forbidden(path.Child("egressRules"), "managed OpenShift RayClusters cannot add custom egress NetworkPolicy rules")
	}
	allowedIngress := []networkingv1.NetworkPolicyIngressRule(nil)
	if allowed != nil {
		allowedIngress = allowed.IngressRules
	}
	for _, rule := range rules.IngressRules {
		if !containsIngressRule(allowedIngress, rule) {
			return field.Forbidden(path.Child("ingressRules"), fmt.Sprintf("custom NetworkPolicy ingress rule %#v is not permitted for managed OpenShift RayClusters", rule))
		}
	}
	var requiredIngress []networkingv1.NetworkPolicyIngressRule
	if required != nil {
		requiredIngress = required.IngressRules
	}
	for _, requiredRule := range requiredIngress {
		if !containsIngressRule(rules.IngressRules, requiredRule) {
			return field.Invalid(path.Child("ingressRules"), rules.IngressRules,
				fmt.Sprintf("managed OpenShift RayClusters must retain platform ingress rule %#v", requiredRule))
		}
	}
	return nil
}

func containsIngressRule(rules []networkingv1.NetworkPolicyIngressRule, required networkingv1.NetworkPolicyIngressRule) bool {
	for _, rule := range rules {
		if reflect.DeepEqual(rule, required) {
			return true
		}
	}
	return false
}

func appendUniqueManagedIngressRules(rules []networkingv1.NetworkPolicyIngressRule, additions ...networkingv1.NetworkPolicyIngressRule) []networkingv1.NetworkPolicyIngressRule {
	for _, addition := range additions {
		if !containsIngressRule(rules, addition) {
			rules = append(rules, addition)
		}
	}
	return rules
}

func (w *RayClusterWebhook) validateWorkerGroups(rayCluster *rayv1.RayCluster) *field.Error {
	workerGroupNames := make(map[string]bool)

	for i, workerGroup := range rayCluster.Spec.WorkerGroupSpecs {
		if _, ok := workerGroupNames[workerGroup.GroupName]; ok {
			return field.Invalid(field.NewPath("spec").Child("workerGroupSpecs").Index(i), workerGroup, "worker group names must be unique")
		}
		workerGroupNames[workerGroup.GroupName] = true
	}

	return nil
}
