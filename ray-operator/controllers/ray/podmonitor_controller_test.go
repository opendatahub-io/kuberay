package ray

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// Exercise the real REST mapper and manager lifecycle: fake clients cannot
// reproduce startup failures caused by watches on an absent optional CRD.
func TestPodMonitorManagerWithoutOptionalAPI(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, rayv1.AddToScheme(s))
	require.NoError(t, monitoringv1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	require.NoError(t, authorizationv1.AddToScheme(s))
	c, err := client.New(config, client.Options{Scheme: s})
	require.NoError(t, err)
	roleData, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	require.NoError(t, err)
	role := &rbacv1.ClusterRole{}
	require.NoError(t, yaml.Unmarshal(roleData, role))
	require.NoError(t, c.Create(t.Context(), role))
	const controllerUser = "podmonitor-controller"
	require.NoError(t, c.Create(t.Context(), &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: controllerUser},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: "User", Name: controllerUser}},
	}))
	user, err := env.AddUser(envtest.User{Name: controllerUser}, config)
	require.NoError(t, err)
	// Use the shipped role, not envtest's administrator credentials, for the controller.
	mgr, err := ctrl.NewManager(user.Config(), ctrl.Options{
		Scheme: s, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)
	r := NewPodMonitorController(mgr)
	require.NoError(t, r.SetupWithManager(mgr, 1))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("PodMonitor manager failed to stop")
		}
	})
	syncCtx, syncCancel := context.WithTimeout(ctx, 10*time.Second)
	defer syncCancel()
	require.True(t, mgr.GetCache().WaitForCacheSync(syncCtx))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "podmonitor-test-"}}
	require.NoError(t, c.Create(ctx, ns))
	cluster := podMonitorTestCluster()
	cluster.UID = ""
	cluster.Namespace = ns.Name
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers = []corev1.Container{{Name: "ray-head", Image: "rayproject/ray:test"}}
	job := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "train-job", Namespace: ns.Name},
		Spec:       rayv1.RayJobSpec{RayClusterSpec: cluster.Spec.DeepCopy()},
	}
	require.NoError(t, c.Create(ctx, job))
	cluster.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: rayv1.GroupVersion.String(), Kind: "RayJob", Name: job.Name, UID: job.UID, Controller: new(true),
	}}
	require.NoError(t, c.Create(ctx, cluster))
	require.Eventually(t, func() bool {
		return r.Get(ctx, client.ObjectKeyFromObject(cluster), &rayv1.RayCluster{}) == nil
	}, 10*time.Second, 100*time.Millisecond)
	// The absent monitoring API must neither block the cache nor fail reconciliation.
	result, err := r.Reconcile(syncCtx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	require.NoError(t, err)
	assert.Equal(t, podMonitorRequeueInterval, result.RequeueAfter)
	select {
	case err := <-done:
		t.Fatalf("manager exited while PodMonitor CRD was absent: %v", err)
	default:
	}

	restricted, err := client.New(user.Config(), client.Options{Scheme: s})
	require.NoError(t, err)
	// Authorization checks do not require the optional monitoring CRD to exist.
	for _, verb := range []string{"get", "create", "update", "delete", "list", "watch", "patch"} {
		review := &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: ns.Name, Group: "monitoring.coreos.com", Resource: "podmonitors", Verb: verb,
			},
		}}
		require.NoError(t, restricted.Create(ctx, review))
		wantAllowed := verb == "get" || verb == "create" || verb == "update" || verb == "delete"
		assert.Equal(t, wantAllowed, review.Status.Allowed, "PodMonitor verb %s", verb)
	}
}
