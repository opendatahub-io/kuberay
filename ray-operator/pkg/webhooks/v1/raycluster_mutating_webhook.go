package v1

import (
	"context"

	routev1 "github.com/openshift/api/route/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// RayClusterDefaulter mutates RayClusters
type RayClusterDefaulter struct {
	RESTMapper meta.RESTMapper
}

//+kubebuilder:webhook:path=/mutate-ray-io-v1-raycluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayclusters,verbs=create;update,versions=v1,name=mraycluster.kb.io,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &RayClusterDefaulter{}

// Default implements webhook.CustomDefaulter
func (d *RayClusterDefaulter) Default(_ context.Context, obj runtime.Object) error {
	rayCluster := obj.(*rayv1.RayCluster)

	rayclusterlog.Info("default", "name", rayCluster.Name)

	// Initialize annotations map if nil
	if rayCluster.Annotations == nil {
		rayCluster.Annotations = make(map[string]string)
	}

	// Set the secure network annotation based on platform
	if d.isOpenShift() {
		rayCluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey] = "true"
		rayclusterlog.Info("enforcing secure trusted network on OpenShift", "name", rayCluster.Name, "namespace", rayCluster.Namespace)

		// STRICT ENFORCEMENT: Always disable basic Route/Ingress creation on OpenShift
		// This enforces Gateway API access only - no exceptions
		// Authentication controller will create HTTPRoute for Gateway API authentication
		// This prevents direct Route access and enforces centralized authentication via Gateway
		falseValue := false
		if rayCluster.Spec.HeadGroupSpec.EnableIngress != nil && *rayCluster.Spec.HeadGroupSpec.EnableIngress {
			rayclusterlog.Info("overriding user-specified enableIngress from true to false to enforce Gateway-only access",
				"name", rayCluster.Name, "namespace", rayCluster.Namespace)
		}
		rayCluster.Spec.HeadGroupSpec.EnableIngress = &falseValue
	} else {
		rayCluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey] = "false"
	}

	return nil
}

// isOpenShift checks if the cluster is running on OpenShift
func (d *RayClusterDefaulter) isOpenShift() bool {
	gvk := routev1.GroupVersion.WithKind("Route")
	_, err := d.RESTMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// SetupRayClusterDefaulterWithManager registers the defaulting webhook for RayCluster
func SetupRayClusterDefaulterWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&rayv1.RayCluster{}).
		WithDefaulter(&RayClusterDefaulter{
			RESTMapper: mgr.GetRESTMapper(),
		}).
		Complete()
}
