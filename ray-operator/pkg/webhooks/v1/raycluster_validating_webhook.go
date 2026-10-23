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
func (w *RayClusterWebhook) ValidateUpdate(_ context.Context, _ *rayv1.RayCluster, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate update", "name", rayCluster.Name)
	return nil, w.validateRayCluster(rayCluster)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayCluster) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayClusterWebhook) validateRayCluster(rayCluster *rayv1.RayCluster) error {
	var allErrs field.ErrorList

	// Detect the supported managed profile from the platform rather than using
	// the legacy annotation as a new opt-in contract. Existing in-place legacy
	// OpenShift clusters are still covered because the OpenShift API surface is
	// present even when their spec.networkPolicy has not been backfilled yet.
	if w.isOpenShift() {
		if utils.ManagedByExternalController(rayCluster.Spec.ManagedBy) != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec").Child("managedBy"), *rayCluster.Spec.ManagedBy,
				"externally managed RayClusters are not supported by the OpenShift platform profile because KubeRay skips NetworkPolicy reconciliation"))
		} else if err := validateManagedOpenShiftNetworkPolicy(rayCluster); err != nil {
			allErrs = append(allErrs, err)
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

func (w *RayClusterWebhook) isOpenShift() bool {
	if w.RESTMapper == nil {
		return false
	}
	_, err := w.RESTMapper.RESTMapping(routev1.GroupVersion.WithKind("Route").GroupKind(), routev1.GroupVersion.Version)
	return err == nil
}

// validateManagedOpenShiftNetworkPolicy enforces the minimum ingress posture
// for the supported OpenShift/RHOAI profile. The mutating webhook supplies the
// same rules for new clusters; validation prevents an explicit policy from
// removing that baseline while preserving additional native KubeRay rules.
func validateManagedOpenShiftNetworkPolicy(rayCluster *rayv1.RayCluster) *field.Error {
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

	// Recompute the current platform defaults from the effective Ray ports and
	// require them to remain present. This allows supported custom ports while
	// rejecting updates that change Ray ports without updating the mandatory
	// platform access rules in the same object.
	defaults := rayCluster.DeepCopy()
	defaults.Spec.NetworkPolicy = nil
	utils.EnsureOpenShiftRayClusterNetworkPolicy(defaults)
	if err := validateManagedRules(
		networkPolicy.Head,
		defaults.Spec.NetworkPolicy.Head,
		networkPolicyPath.Child("head"),
	); err != nil {
		return err
	}
	if err := validateManagedRules(networkPolicy.Worker, nil, networkPolicyPath.Child("worker")); err != nil {
		return err
	}
	for i := range networkPolicy.WorkerGroups {
		if err := validateManagedRules(
			&networkPolicy.WorkerGroups[i].NetworkPolicyRules,
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
	// TODO(ODH NetworkPolicy): Decide whether platform-default rules may be
	// removed when RayStartParams change. The current check requires the new
	// effective-port rule but does not reject a user-added stale rule because
	// native KubeRay policy extensions must remain supported.
	return nil
}

// validateManagedRules makes the OpenShift platform defaults an exact ceiling
// for the NetworkPolicy rule extensions in the RayCluster spec. The native
// KubeRay policy API remains unchanged for non-OpenShift clusters; this
// restriction applies only to the supported managed platform profile.
func validateManagedRules(rules, allowed *rayv1.NetworkPolicyRules, path *field.Path) *field.Error {
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
	for _, requiredRule := range allowedIngress {
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
