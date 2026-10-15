package ray

import (
	"context"
	"reflect"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

const legacySecurityMigrationRequeueInterval = 10 * time.Second

// LegacySecurityController migrates the downstream OpenShift security annotation
// to upstream RayCluster fields. Resource ownership remains with the native
// mTLS and NetworkPolicy controllers.
type LegacySecurityController struct {
	client.Client
	isOpenShift bool
}

func NewLegacySecurityController(mgr manager.Manager, isOpenShift bool) *LegacySecurityController {
	return &LegacySecurityController{Client: mgr.GetClient(), isOpenShift: isOpenShift}
}

// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch;patch
func (r *LegacySecurityController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	cluster := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	if !r.isOpenShift || !features.Enabled(features.RayClusterMTLS) || !features.Enabled(features.RayClusterNetworkPolicy) ||
		!utils.IsLegacySecurityEnabled(cluster) {
		return reconcile.Result{}, nil
	}
	if !cluster.DeletionTimestamp.IsZero() || utils.ManagedByExternalController(cluster.Spec.ManagedBy) != nil {
		return reconcile.Result{}, nil
	}

	before := cluster.DeepCopy()
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	if reflect.DeepEqual(before.Spec, cluster.Spec) && reflect.DeepEqual(before.Annotations, cluster.Annotations) {
		return reconcile.Result{}, nil
	}

	if err := r.Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return reconcile.Result{}, err
	}
	// An older CRD can silently prune native fields from a successful patch.
	// Re-read on the next reconcile and retry until both fields are persisted.
	return reconcile.Result{RequeueAfter: legacySecurityMigrationRequeueInterval}, nil
}

func (r *LegacySecurityController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("raycluster-legacy-security").
		For(&rayv1.RayCluster{}).
		Complete(r)
}
