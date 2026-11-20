package ray

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func podMonitorTestCluster() *rayv1.RayCluster {
	return &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "training", Namespace: "default", UID: "cluster-uid",
	}}
}

func newPodMonitorTestController(t *testing.T, objects ...client.Object) *PodMonitorController {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, rayv1.AddToScheme(s))
	require.NoError(t, monitoringv1.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).
		WithStatusSubresource(&rayv1.RayCluster{}, &rayv1.RayJob{}).
		WithIndex(&rayv1.RayCluster{}, podMonitorRayJobIndex, podMonitorRayJobIndexValues).Build()
	return &PodMonitorController{Client: c, Recorder: events.NewFakeRecorder(32)}
}

func reconcilePodMonitor(t *testing.T, r *PodMonitorController, cluster *rayv1.RayCluster) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	require.NoError(t, err)
	return result
}

func getTestPodMonitor(t *testing.T, r *PodMonitorController, cluster *rayv1.RayCluster) *monitoringv1.PodMonitor {
	t.Helper()
	monitor := &monitoringv1.PodMonitor{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKey{
		Namespace: cluster.Namespace, Name: cluster.Name + "-podmonitor",
	}, monitor))
	return monitor
}

func requirePodMonitorEvent(t *testing.T, r *PodMonitorController, reason string) {
	t.Helper()
	select {
	case event := <-r.Recorder.(*events.FakeRecorder).Events:
		require.Contains(t, event, reason)
	default:
		t.Fatal("expected a PodMonitor warning event")
	}
}

func TestPodMonitorCreationAndDrift(t *testing.T) {
	cluster := podMonitorTestCluster()
	r := newPodMonitorTestController(t, cluster)
	assert.Empty(t, reconcilePodMonitor(t, r, cluster))
	monitor := getTestPodMonitor(t, r, cluster)
	assert.True(t, metav1.IsControlledBy(monitor, cluster))
	assert.Equal(t, new(false), metav1.GetControllerOf(monitor).BlockOwnerDeletion)
	assert.Equal(t, "false", monitor.Labels[podMonitorScrapeLabel])
	assert.Equal(t, []string{utils.RayClusterLabelKey, utils.RayNodeTypeLabelKey, utils.RayNodeGroupLabelKey}, monitor.Spec.PodTargetLabels)
	require.Len(t, monitor.Spec.PodMetricsEndpoints, 1)
	endpoint := monitor.Spec.PodMetricsEndpoints[0]
	assert.Equal(t, "metrics", *endpoint.Port)
	assert.Equal(t, "/metrics", endpoint.Path)
	assert.Equal(t, monitoringv1.Scheme("http"), *endpoint.Scheme)
	assert.Nil(t, endpoint.PortNumber)
	assert.Empty(t, endpoint.RelabelConfigs)
	assert.False(t, monitor.Spec.NamespaceSelector.Any)
	assert.Empty(t, monitor.Spec.NamespaceSelector.MatchNames)

	selector, err := metav1.LabelSelectorAsSelector(&monitor.Spec.Selector)
	require.NoError(t, err)
	for _, nodeType := range []string{"head", "worker"} {
		assert.True(t, selector.Matches(labels.Set{
			utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: nodeType,
			utils.RayNodeGroupLabelKey: "autoscaled-group",
		}))
	}
	assert.False(t, selector.Matches(labels.Set{utils.RayClusterLabelKey: "other", utils.RayNodeTypeLabelKey: "worker"}))
	assert.False(t, selector.Matches(labels.Set{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: "redis-cleanup"}))

	version := monitor.ResourceVersion
	reconcilePodMonitor(t, r, cluster)
	assert.Equal(t, version, getTestPodMonitor(t, r, cluster).ResourceVersion, "unchanged monitors must not be rewritten")
	monitor.Labels[podMonitorScrapeLabel] = "true"
	monitor.Labels["user-label"] = "preserved"
	monitor.Annotations = map[string]string{"user-annotation": "preserved"}
	monitor.Spec.PodMetricsEndpoints[0].Port = new("dashboard")
	monitor.Spec.Selector.MatchLabels[utils.RayClusterLabelKey] = "other"
	require.NoError(t, r.Update(t.Context(), monitor))
	reconcilePodMonitor(t, r, cluster)
	repaired := getTestPodMonitor(t, r, cluster)
	assert.Equal(t, desiredPodMonitor(cluster).Spec, repaired.Spec)
	assert.Equal(t, "false", repaired.Labels[podMonitorScrapeLabel])
	assert.Equal(t, "preserved", repaired.Labels["user-label"])
	assert.Equal(t, "preserved", repaired.Annotations["user-annotation"])
}

func TestPodMonitorClusterIsolationAndRayService(t *testing.T) {
	first := podMonitorTestCluster()
	second := first.DeepCopy()
	second.Name, second.UID = "service-cluster", "service-cluster-uid"
	second.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: rayv1.GroupVersion.String(), Kind: "RayService", Name: "service", UID: "service-uid", Controller: new(true),
	}}
	r := newPodMonitorTestController(t, first, second)
	reconcilePodMonitor(t, r, first)
	reconcilePodMonitor(t, r, second)
	monitors := &monitoringv1.PodMonitorList{}
	require.NoError(t, r.List(t.Context(), monitors))
	require.Len(t, monitors.Items, 2)
	assert.True(t, metav1.IsControlledBy(getTestPodMonitor(t, r, second), second))
	assert.Empty(t, getTestPodMonitor(t, r, second).Spec.PodMetricsEndpoints[0].RelabelConfigs)
}

func TestPodMonitorOptOutExactValue(t *testing.T) {
	for _, value := range []string{"", "true", "Disabled", "disabled"} {
		t.Run(fmt.Sprintf("value=%q", value), func(t *testing.T) {
			cluster := podMonitorTestCluster()
			cluster.Annotations = map[string]string{podMonitorOptOutAnnotation: value}
			r := newPodMonitorTestController(t, cluster)
			reconcilePodMonitor(t, r, cluster)
			monitors := &monitoringv1.PodMonitorList{}
			require.NoError(t, r.List(t.Context(), monitors))
			if value == "disabled" {
				assert.Empty(t, monitors.Items)
			} else {
				assert.Len(t, monitors.Items, 1)
			}
		})
	}
}

func TestPodMonitorOptOutAndGateTogglesLeaveWorkloadsUnchanged(t *testing.T) {
	cluster := podMonitorTestCluster()
	cluster.Spec.RayVersion = "2.47.1"
	cluster.Status.State = rayv1.Ready
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "training-head", Namespace: cluster.Namespace, UID: "pod-uid"}}
	r := newPodMonitorTestController(t, cluster, pod)
	originalCluster := &rayv1.RayCluster{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), originalCluster))
	originalPod := &corev1.Pod{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pod), originalPod))
	reconcilePodMonitor(t, r, cluster)
	getTestPodMonitor(t, r, cluster)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	cluster.Annotations = map[string]string{podMonitorOptOutAnnotation: "disabled"}
	require.NoError(t, r.Update(t.Context(), cluster))
	reconcilePodMonitor(t, r, cluster)
	monitors := &monitoringv1.PodMonitorList{}
	require.NoError(t, r.List(t.Context(), monitors))
	assert.Empty(t, monitors.Items)
	delete(cluster.Annotations, podMonitorOptOutAnnotation)
	require.NoError(t, r.Update(t.Context(), cluster))
	reconcilePodMonitor(t, r, cluster)
	getTestPodMonitor(t, r, cluster)
	r.cleanupOnly = true
	reconcilePodMonitor(t, r, cluster)
	reconcilePodMonitor(t, r, cluster)
	require.NoError(t, r.List(t.Context(), monitors))
	assert.Empty(t, monitors.Items)
	r.cleanupOnly = false
	reconcilePodMonitor(t, r, cluster)
	getTestPodMonitor(t, r, cluster)
	currentCluster := &rayv1.RayCluster{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), currentCluster))
	assert.Equal(t, originalCluster.Spec, currentCluster.Spec)
	assert.Equal(t, originalCluster.Status, currentCluster.Status)
	currentPod := &corev1.Pod{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pod), currentPod))
	assert.Equal(t, originalPod, currentPod)
}

func TestPodMonitorTerminatingClustersAreDeleteOnly(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, ownership := range []string{"owned", "foreign", "absent"} {
			t.Run(fmt.Sprintf("enabled=%t/monitor=%s", enabled, ownership), func(t *testing.T) {
				cluster := podMonitorTestCluster()
				cluster.Finalizers = []string{authenticationFinalizer}
				r := newPodMonitorTestController(t, cluster)
				r.cleanupOnly = !enabled
				if ownership != "absent" {
					monitor := desiredPodMonitor(cluster)
					if ownership == "owned" {
						require.NoError(t, controllerutil.SetControllerReference(cluster, monitor, r.Scheme()))
					}
					require.NoError(t, r.Create(t.Context(), monitor))
				}
				require.NoError(t, r.Delete(t.Context(), cluster))
				before := &rayv1.RayCluster{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), before))
				require.NotNil(t, before.DeletionTimestamp)
				reconcilePodMonitor(t, r, cluster)
				reconcilePodMonitor(t, r, cluster)
				monitors := &monitoringv1.PodMonitorList{}
				require.NoError(t, r.List(t.Context(), monitors))
				if ownership == "foreign" {
					assert.Len(t, monitors.Items, 1)
					requirePodMonitorEvent(t, r, "PodMonitorOwnershipConflict")
				} else {
					assert.Empty(t, monitors.Items, "terminating clusters must not retain or recreate owned monitors")
				}
				after := &rayv1.RayCluster{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), after))
				assert.Equal(t, before, after, "monitor cleanup must not change the cluster or its finalizer")
			})
		}
	}
}

func TestPodMonitorRayJobLineageAndOptOut(t *testing.T) {
	for _, apiVersion := range []string{rayv1.GroupVersion.String(), "ray.io/v1alpha1"} {
		t.Run(apiVersion, func(t *testing.T) {
			cluster := podMonitorTestCluster()
			job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "train-job", Namespace: cluster.Namespace, UID: "job-uid"}}
			r := newPodMonitorTestController(t, job)
			require.NoError(t, controllerutil.SetControllerReference(job, cluster, r.Scheme()))
			cluster.OwnerReferences[0].APIVersion = apiVersion
			require.NoError(t, r.Create(t.Context(), cluster))
			reconcilePodMonitor(t, r, cluster)
			relabels := getTestPodMonitor(t, r, cluster).Spec.PodMetricsEndpoints[0].RelabelConfigs
			require.Len(t, relabels, 1)
			assert.Equal(t, "rayjob", relabels[0].TargetLabel)
			assert.Equal(t, job.Name, *relabels[0].Replacement)
			job.Annotations = map[string]string{podMonitorOptOutAnnotation: "disabled"}
			require.NoError(t, r.Update(t.Context(), job))
			reconcilePodMonitor(t, r, cluster)
			monitors := &monitoringv1.PodMonitorList{}
			require.NoError(t, r.List(t.Context(), monitors))
			assert.Empty(t, monitors.Items)
			delete(job.Annotations, podMonitorOptOutAnnotation)
			require.NoError(t, r.Update(t.Context(), job))
			reconcilePodMonitor(t, r, cluster)
			getTestPodMonitor(t, r, cluster)

			// A replacement RayJob with the same name must not control the old cluster.
			require.NoError(t, r.Delete(t.Context(), job))
			job.Annotations = map[string]string{podMonitorOptOutAnnotation: "disabled"}
			job.UID = "replacement-job-uid"
			job.ResourceVersion = ""
			require.NoError(t, r.Create(t.Context(), job))
			reconcilePodMonitor(t, r, cluster)
			getTestPodMonitor(t, r, cluster)
		})
	}
}

func TestPodMonitorRayJobInheritedOptOutCanBeRemoved(t *testing.T) {
	cluster := podMonitorTestCluster()
	cluster.Annotations = map[string]string{podMonitorOptOutAnnotation: "disabled"}
	job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{
		Name: "train-job", Namespace: cluster.Namespace, UID: "job-uid",
		Annotations: map[string]string{podMonitorOptOutAnnotation: "disabled"},
	}}
	r := newPodMonitorTestController(t, job)
	require.NoError(t, controllerutil.SetControllerReference(job, cluster, r.Scheme()))
	require.NoError(t, r.Create(t.Context(), cluster))
	reconcilePodMonitor(t, r, cluster)
	monitors := &monitoringv1.PodMonitorList{}
	require.NoError(t, r.List(t.Context(), monitors))
	assert.Empty(t, monitors.Items)
	delete(job.Annotations, podMonitorOptOutAnnotation)
	require.NoError(t, r.Update(t.Context(), job))
	reconcilePodMonitor(t, r, cluster)
	getTestPodMonitor(t, r, cluster)
	stored := &rayv1.RayCluster{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), stored))
	assert.Equal(t, "disabled", stored.Annotations[podMonitorOptOutAnnotation], "the controller must not rewrite workload metadata")
}

func TestPodMonitorForeignObjectsArePreserved(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, owner := range []string{"none", "other-cluster", "non-controller"} {
			t.Run(fmt.Sprintf("disabled=%t/owner=%s", disabled, owner), func(t *testing.T) {
				cluster := podMonitorTestCluster()
				r := newPodMonitorTestController(t, cluster)
				r.cleanupOnly = disabled
				foreign := desiredPodMonitor(cluster)
				if owner != "none" {
					foreign.OwnerReferences = []metav1.OwnerReference{{
						APIVersion: rayv1.GroupVersion.String(), Kind: "RayCluster", Name: cluster.Name,
						UID: cluster.UID, Controller: new(owner != "non-controller"),
					}}
					if owner == "other-cluster" {
						foreign.OwnerReferences[0].UID = "old-cluster-uid"
					}
				}
				require.NoError(t, r.Create(t.Context(), foreign))
				before := getTestPodMonitor(t, r, cluster)
				assert.Equal(t, podMonitorRetryInterval, reconcilePodMonitor(t, r, cluster).RequeueAfter)
				assert.Equal(t, before, getTestPodMonitor(t, r, cluster))
				requirePodMonitorEvent(t, r, "PodMonitorOwnershipConflict")
			})
		}
	}
}

func TestPodMonitorRepairsBlockingOwnerReferences(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "terminating"} {
		t.Run(mode, func(t *testing.T) {
			cluster := podMonitorTestCluster()
			cluster.Finalizers = []string{authenticationFinalizer}
			r := newPodMonitorTestController(t, cluster)
			monitor := desiredPodMonitor(cluster)
			require.NoError(t, controllerutil.SetControllerReference(cluster, monitor, r.Scheme()))
			monitor.Finalizers = []string{"ray.io/test-monitoring"}
			require.NoError(t, r.Create(t.Context(), monitor))
			switch mode {
			case "disabled":
				r.cleanupOnly = true
			case "terminating":
				require.NoError(t, r.Delete(t.Context(), cluster))
			}
			reconcilePodMonitor(t, r, cluster)
			stored := getTestPodMonitor(t, r, cluster)
			assert.Equal(t, new(false), metav1.GetControllerOf(stored).BlockOwnerDeletion)
			assert.Equal(t, monitor.Finalizers, stored.Finalizers)
			assert.Equal(t, mode != "enabled", stored.DeletionTimestamp != nil)
		})
	}
}

func TestPodMonitorFailuresAreIsolatedAndRetried(t *testing.T) {
	resource := schema.GroupResource{Group: monitoringv1.SchemeGroupVersion.Group, Resource: "podmonitors"}
	for _, operation := range []string{"get-forbidden", "get-no-match", "create-forbidden", "create-no-crd", "create-webhook", "update", "delete", "rayjob-get"} {
		t.Run(operation, func(t *testing.T) {
			cluster := podMonitorTestCluster()
			cluster.Status.State = rayv1.Ready
			if operation == "rayjob-get" {
				cluster.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: rayv1.GroupVersion.String(), Kind: "RayJob", Name: "train-job", UID: "job-uid", Controller: new(true),
				}}
			}
			r := newPodMonitorTestController(t, cluster)
			if operation == "update" || operation == "delete" {
				reconcilePodMonitor(t, r, cluster)
				monitor := getTestPodMonitor(t, r, cluster)
				monitor.Labels[podMonitorScrapeLabel] = "true"
				require.NoError(t, r.Update(t.Context(), monitor))
			}
			if operation == "delete" {
				r.cleanupOnly = true
			}
			base := r.Client.(client.WithWatch)
			denied := errors.NewForbidden(resource, cluster.Name+"-podmonitor", fmt.Errorf("injected permission failure"))
			createCalls := 0
			faulty := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*rayv1.RayJob); ok && operation == "rayjob-get" {
						return errors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayjobs"}, key.Name, fmt.Errorf("denied"))
					}
					if _, ok := obj.(*monitoringv1.PodMonitor); ok {
						switch operation {
						case "get-forbidden":
							return denied
						case "get-no-match":
							return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: monitoringv1.SchemeGroupVersion.Group, Kind: "PodMonitor"}}
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					createCalls++
					switch operation {
					case "create-forbidden":
						return denied
					case "create-no-crd":
						return errors.NewNotFound(resource, "")
					case "create-webhook":
						return errors.NewInternalError(fmt.Errorf("admission webhook rejected PodMonitor"))
					default:
						return c.Create(ctx, obj, opts...)
					}
				},
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error { return denied },
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return denied },
			})
			r.Client = faulty
			result := reconcilePodMonitor(t, r, cluster)
			if operation == "get-no-match" || operation == "create-no-crd" {
				assert.Empty(t, result)
			} else {
				assert.Equal(t, podMonitorRetryInterval, result.RequeueAfter)
			}
			if operation == "get-forbidden" || operation == "rayjob-get" {
				assert.Zero(t, createCalls, "failed lookups must not create a PodMonitor")
			}
			stored := &rayv1.RayCluster{}
			require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(cluster), stored))
			assert.Equal(t, rayv1.Ready, stored.Status.State)
			if operation == "get-no-match" || operation == "create-no-crd" {
				assert.Empty(t, r.Recorder.(*events.FakeRecorder).Events, "missing optional APIs should not emit warning events")
			} else {
				requirePodMonitorEvent(t, r, "PodMonitorReconcileFailed")
			}
			// The same controller must recover after the API/permissions/webhook recovers.
			r.Client = base
			reconcilePodMonitor(t, r, cluster)
			if operation != "delete" {
				assert.Equal(t, "false", getTestPodMonitor(t, r, cluster).Labels[podMonitorScrapeLabel])
			} else {
				monitors := &monitoringv1.PodMonitorList{}
				require.NoError(t, base.List(t.Context(), monitors))
				assert.Empty(t, monitors.Items)
			}
		})
	}
}

func TestPodMonitorDeletedAndExternallyManagedClusters(t *testing.T) {
	cluster := podMonitorTestCluster()
	cluster.Spec.ManagedBy = new("kueue.x-k8s.io/multikueue")
	r := newPodMonitorTestController(t, cluster)
	assert.Empty(t, reconcilePodMonitor(t, r, cluster))
	assert.Empty(t, reconcilePodMonitor(t, r, &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "missing", Namespace: cluster.Namespace,
	}}))
	monitors := &monitoringv1.PodMonitorList{}
	require.NoError(t, r.List(t.Context(), monitors))
	assert.Empty(t, monitors.Items)
}

func TestPodMonitorRayJobEventMapping(t *testing.T) {
	job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "train", Namespace: "default", UID: "job-uid"}}
	cluster := podMonitorTestCluster()
	r := newPodMonitorTestController(t)
	require.NoError(t, controllerutil.SetControllerReference(job, cluster, r.Scheme()))
	second := cluster.DeepCopy()
	second.Name, second.UID = "second", "second-uid"
	other := cluster.DeepCopy()
	other.Name, other.UID = "other", "other-uid"
	other.OwnerReferences[0].UID = "another-job-uid"
	outside := cluster.DeepCopy()
	outside.Namespace = "outside"
	for _, obj := range []*rayv1.RayCluster{cluster, second, other, outside} {
		require.NoError(t, r.Create(t.Context(), obj))
	}
	assert.ElementsMatch(t, []ctrl.Request{
		{NamespacedName: client.ObjectKeyFromObject(cluster)},
		{NamespacedName: client.ObjectKeyFromObject(second)},
	}, r.rayJobClusters(t.Context(), job))
}

func TestPodMonitorCleanupFailureCanRecover(t *testing.T) {
	cluster := podMonitorTestCluster()
	cluster.Name = "a-blocked"
	healthy := podMonitorTestCluster()
	healthy.Name, healthy.UID = "b-healthy", "healthy-uid"
	r := newPodMonitorTestController(t, cluster, healthy)
	reconcilePodMonitor(t, r, cluster)
	reconcilePodMonitor(t, r, healthy)
	base := r.Client.(client.WithWatch)
	r.cleanupOnly = true
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == cluster.Name+"-podmonitor" {
				return errors.NewForbidden(monitoringv1.Resource("podmonitors"), cluster.Name, fmt.Errorf("denied"))
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	cleanup := &podMonitorCleanup{PodMonitorController: r, clusters: base}
	require.True(t, errors.IsForbidden(cleanup.cleanup(t.Context())))
	getTestPodMonitor(t, r, cluster)
	assert.True(t, errors.IsNotFound(base.Get(t.Context(), client.ObjectKey{
		Namespace: healthy.Namespace, Name: healthy.Name + "-podmonitor",
	}, &monitoringv1.PodMonitor{})), "one rejected deletion must not block other clusters")
	requirePodMonitorEvent(t, r, "PodMonitorReconcileFailed")
	r.Client = base
	require.NoError(t, cleanup.cleanup(t.Context()))
	monitors := &monitoringv1.PodMonitorList{}
	require.NoError(t, base.List(t.Context(), monitors))
	assert.Empty(t, monitors.Items)
}

func TestPodMonitorTaskRetriesWithoutFailingManager(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := &atomic.Int32{}
		task := &podMonitorTask{run: func(context.Context) error {
			if calls.Add(1) == 1 {
				return fmt.Errorf("optional monitoring unavailable")
			}
			return nil
		}}
		require.True(t, task.NeedLeaderElection())
		done := make(chan error, 1)
		go func() { done <- task.Start(t.Context()) }()
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load())
		time.Sleep(podMonitorRetryInterval)
		synctest.Wait()
		require.Equal(t, int32(2), calls.Load())
		require.NoError(t, <-done)
	})
}
