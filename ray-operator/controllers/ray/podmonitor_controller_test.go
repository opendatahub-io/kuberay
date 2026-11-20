package ray

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func newScopedPodMonitorManager(t *testing.T, previous manager.Manager, namespace string) manager.Manager {
	t.Helper()
	mgr, err := ctrl.NewManager(previous.GetConfig(), ctrl.Options{
		Scheme:     previous.GetScheme(),
		Controller: controllerconfig.Controller{SkipNameValidation: new(true)},
		Cache:      cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
		Metrics:    metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)
	return mgr
}

func newPodMonitorTestManager(t *testing.T, monitoringAPI bool, verbs []string) (manager.Manager, client.Client, string) {
	t.Helper()
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if monitoringAPI {
		// Only the API shape needed to exercise real informer and owner events.
		env.CRDs = []*apiextensionsv1.CustomResourceDefinition{{
			ObjectMeta: metav1.ObjectMeta{Name: "podmonitors.monitoring.coreos.com"},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: "monitoring.coreos.com", Scope: apiextensionsv1.NamespaceScoped,
				Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "podmonitors", Kind: "PodMonitor", ListKind: "PodMonitorList"},
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
					Name: "v1", Served: true, Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {Type: "object", XPreserveUnknownFields: new(true)},
						},
					}},
				}},
			},
		}}
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
	if verbs != nil {
		for i := range role.Rules {
			if len(role.Rules[i].APIGroups) == 1 && role.Rules[i].APIGroups[0] == "monitoring.coreos.com" {
				role.Rules[i].Verbs = verbs
				if len(verbs) == 0 {
					role.Rules = append(role.Rules[:i], role.Rules[i+1:]...)
				}
				break
			}
		}
	}
	require.NoError(t, c.Create(t.Context(), role))
	const controllerUser = "podmonitor-controller"
	require.NoError(t, c.Create(t.Context(), &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: controllerUser},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: "User", Name: controllerUser}},
	}))
	user, err := env.AddUser(envtest.User{Name: controllerUser}, config)
	require.NoError(t, err)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "podmonitor-test-"}}
	require.NoError(t, c.Create(t.Context(), ns))
	mgr, err := ctrl.NewManager(user.Config(), ctrl.Options{Scheme: s, Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	return newScopedPodMonitorManager(t, mgr, ns.Name), c, ns.Name
}

func startPodMonitorTestManager(t *testing.T, mgr manager.Manager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	stop := sync.OnceFunc(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("manager stopped with an error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("PodMonitor manager failed to stop")
		}
	})
	t.Cleanup(stop)
	return stop
}

func podMonitorAPICluster(namespace string) *rayv1.RayCluster {
	cluster := podMonitorTestCluster()
	cluster.UID, cluster.Namespace = "", namespace
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers = []corev1.Container{{Name: "ray-head", Image: "rayproject/ray:test"}}
	return cluster
}

func TestPodMonitorManagerWithoutOptionalAPI(t *testing.T) {
	for _, tc := range []struct {
		name  string
		api   bool
		verbs []string
	}{
		{name: "missing-crd"},
		{name: "missing-rbac", api: true, verbs: []string{}},
		{name: "missing-watch", api: true, verbs: []string{"get", "list", "create", "update", "delete"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, c, namespace := newPodMonitorTestManager(t, tc.api, tc.verbs)
			r := NewPodMonitorController(mgr)
			require.NoError(t, r.SetupWithManager(mgr, 1, namespace))
			reconciled := make(chan struct{}, 1)
			require.NoError(t, ctrl.NewControllerManagedBy(mgr).Named("required-workloads").
				For(&rayv1.RayCluster{}).Complete(reconcile.Func(func(context.Context, ctrl.Request) (ctrl.Result, error) {
				select {
				case reconciled <- struct{}{}:
				default:
				}
				return ctrl.Result{}, nil
			})))
			startPodMonitorTestManager(t, mgr)
			require.NoError(t, c.Create(t.Context(), podMonitorAPICluster(namespace)))
			select {
			case <-reconciled:
			case <-time.After(10 * time.Second):
				t.Fatal("optional monitoring blocked the workload controller")
			}
		})
	}
}

func TestPodMonitorManagerEventsAndDisabledRollout(t *testing.T) {
	mgr, c, namespace := newPodMonitorTestManager(t, true, nil)
	r := NewPodMonitorController(mgr)
	require.NoError(t, r.SetupWithManager(mgr, 1, namespace))
	stop := startPodMonitorTestManager(t, mgr)
	cluster := podMonitorAPICluster(namespace)
	job := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "train-job", Namespace: namespace},
		Spec:       rayv1.RayJobSpec{RayClusterSpec: cluster.Spec.DeepCopy()},
	}
	require.NoError(t, c.Create(t.Context(), job))
	require.NoError(t, controllerutil.SetControllerReference(job, cluster, c.Scheme()))
	require.NoError(t, c.Create(t.Context(), cluster))
	key := client.ObjectKey{Namespace: namespace, Name: cluster.Name + "-podmonitor"}
	monitor := &monitoringv1.PodMonitor{}
	waitForMonitor := func() {
		t.Helper()
		require.Eventually(t, func() bool { return c.Get(t.Context(), key, monitor) == nil }, 10*time.Second, 20*time.Millisecond)
	}
	waitForAbsent := func() {
		t.Helper()
		require.Eventually(t, func() bool { return apierrors.IsNotFound(c.Get(t.Context(), key, &monitoringv1.PodMonitor{})) }, 10*time.Second, 20*time.Millisecond)
	}
	waitForMonitor()
	monitor.Spec.PodMetricsEndpoints[0].Port = new("dashboard")
	require.NoError(t, c.Update(t.Context(), monitor))
	require.Eventually(t, func() bool {
		return c.Get(t.Context(), key, monitor) == nil && *monitor.Spec.PodMetricsEndpoints[0].Port == "metrics"
	}, 10*time.Second, 20*time.Millisecond, "owned monitor edits must trigger repair without a timer")
	previousUID := monitor.UID
	require.NoError(t, c.Delete(t.Context(), monitor))
	require.Eventually(t, func() bool { return c.Get(t.Context(), key, monitor) == nil && monitor.UID != previousUID }, 10*time.Second, 20*time.Millisecond)
	job.Annotations = map[string]string{podMonitorOptOutAnnotation: "disabled"}
	require.NoError(t, c.Update(t.Context(), job))
	waitForAbsent()
	delete(job.Annotations, podMonitorOptOutAnnotation)
	require.NoError(t, c.Update(t.Context(), job))
	waitForMonitor()
	for _, verb := range []string{"get", "create", "update", "delete", "list", "watch", "patch"} {
		review := &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: namespace, Group: "monitoring.coreos.com", Resource: "podmonitors", Verb: verb},
		}}
		require.NoError(t, r.Create(t.Context(), review))
		assert.Equal(t, verb != "patch", review.Status.Allowed, "PodMonitor verb %s", verb)
	}
	stop()
	outside := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "outside-watch"}}
	require.NoError(t, c.Create(t.Context(), outside))
	outsideCluster := podMonitorAPICluster(outside.Name)
	require.NoError(t, c.Create(t.Context(), outsideCluster))
	outsideMonitor := desiredPodMonitor(outsideCluster)
	require.NoError(t, controllerutil.SetControllerReference(outsideCluster, outsideMonitor, c.Scheme(), controllerutil.WithBlockOwnerDeletion(false)))
	require.NoError(t, c.Create(t.Context(), outsideMonitor))
	disabled := newScopedPodMonitorManager(t, mgr, namespace)
	require.NoError(t, NewPodMonitorController(disabled).CleanupWithManager(disabled))
	stopDisabled := startPodMonitorTestManager(t, disabled)
	waitForAbsent()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(outsideMonitor), &monitoringv1.PodMonitor{}))
	unchanged := &rayv1.RayCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), unchanged))
	assert.Equal(t, cluster.Spec, unchanged.Spec)
	stopDisabled()
	enabled := newScopedPodMonitorManager(t, mgr, namespace)
	require.NoError(t, NewPodMonitorController(enabled).SetupWithManager(enabled, 1, namespace))
	startPodMonitorTestManager(t, enabled)
	waitForMonitor()
}

// Let the preflight succeed, then reject the informer's first request.
type failingPodMonitorTransport struct {
	http.RoundTripper
	requests *atomic.Int32
	failures chan struct{}
}

func (r failingPodMonitorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/podmonitors") && r.requests.Add(1) > 2 {
		select {
		case r.failures <- struct{}{}:
		default:
		}
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","message":"monitoring permission revoked after preflight","code":403}`)),
			Request:    req,
		}, nil
	}
	return r.RoundTripper.RoundTrip(req)
}

func TestPodMonitorInformerFailureDoesNotStopWorkloads(t *testing.T) {
	original, c, namespace := newPodMonitorTestManager(t, true, nil)
	cfg := rest.CopyConfig(original.GetConfig())
	requests := &atomic.Int32{}
	failures := make(chan struct{}, 1)
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return failingPodMonitorTransport{RoundTripper: base, requests: requests, failures: failures}
	})
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     original.GetScheme(),
		Cache:      cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
		Controller: controllerconfig.Controller{SkipNameValidation: new(true), CacheSyncTimeout: time.Second},
		Metrics:    metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)
	require.NoError(t, NewPodMonitorController(mgr).SetupWithManager(mgr, 1, namespace))
	reconciled := make(chan struct{}, 1)
	require.NoError(t, ctrl.NewControllerManagedBy(mgr).Named("required-workload-isolation").
		For(&rayv1.RayCluster{}).Complete(reconcile.Func(func(context.Context, ctrl.Request) (ctrl.Result, error) {
		select {
		case reconciled <- struct{}{}:
		default:
		}
		return ctrl.Result{}, nil
	})))
	startPodMonitorTestManager(t, mgr)
	select {
	case <-failures:
	case <-time.After(10 * time.Second):
		t.Fatal("monitoring informer never reached the injected permission failure")
	}
	// Wait beyond the failed optional controller's cache-sync deadline.
	time.Sleep(2 * time.Second)
	require.NoError(t, c.Create(t.Context(), podMonitorAPICluster(namespace)))
	select {
	case <-reconciled:
	case <-time.After(5 * time.Second):
		t.Fatal("optional informer failure stopped workload reconciliation")
	}
}
