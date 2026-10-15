package ray

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func legacyNetworkPolicyFixture(t *testing.T) (*runtime.Scheme, *rayv1.RayCluster, []*networkingv1.NetworkPolicy) {
	t.Helper()
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	scheme := newMTLSTestScheme()
	require.NoError(t, networkingv1.AddToScheme(scheme))
	cluster := newMTLSTestCluster("legacy-cluster")
	cluster.Annotations = map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"}
	var policies []*networkingv1.NetworkPolicy
	for _, nodeType := range []rayv1.RayNodeType{rayv1.HeadNode, rayv1.WorkerNode} {
		name := cluster.Name + "-head"
		if nodeType == rayv1.WorkerNode {
			name = cluster.Name + "-workers"
		}
		policy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: cluster.Namespace, UID: types.UID("legacy-" + name),
				Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name},
			},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{
					utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(nodeType),
				}},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
				Ingress: []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{utils.RayClusterLabelKey: cluster.Name}},
				}}}},
			},
		}
		require.NoError(t, controllerutil.SetControllerReference(cluster, policy, scheme))
		policies = append(policies, policy)
	}
	return scheme, cluster, policies
}

func TestNetworkPolicyRetainsIsolationForRemovedWorkerGroup(t *testing.T) {
	scheme, cluster, _ := legacyNetworkPolicyFixture(t)
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	worker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "old-worker", Namespace: cluster.Namespace,
			Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode), utils.RayNodeGroupLabelKey: "small-group"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, worker).Build()
	network := &NetworkPolicyController{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(100), isOpenShift: true}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	_, err := network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	cluster.Spec.WorkerGroupSpecs = nil
	require.NoError(t, c.Update(t.Context(), cluster))
	result, err := network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-workers-small-group"}
	require.NoError(t, c.Get(t.Context(), key, &networkingv1.NetworkPolicy{}), "live orphaned workers must remain isolated")
	assert.Positive(t, result.RequeueAfter, "retained policies must eventually be cleaned up")
	require.NoError(t, c.Delete(t.Context(), worker))
	_, err = network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.True(t, apierrors.IsNotFound(c.Get(t.Context(), key, &networkingv1.NetworkPolicy{})))
}

func TestLegacyWorkerPolicyRetainedUntilExistingPodsMatchReplacement(t *testing.T) {
	scheme, cluster, policies := legacyNetworkPolicyFixture(t)
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	worker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "unlabelled-worker", Namespace: cluster.Namespace,
			Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, policies[0], policies[1], worker).Build()
	network := &NetworkPolicyController{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(100), isOpenShift: true}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	result, err := network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policies[1]), &networkingv1.NetworkPolicy{}))
	worker.Labels[utils.RayNodeGroupLabelKey] = "small-group"
	require.NoError(t, c.Update(t.Context(), worker))
	_, err = network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(policies[1]), &networkingv1.NetworkPolicy{})))
}

func TestMigratedNetworkPoliciesRecoverFromExternalDeletion(t *testing.T) {
	scheme, cluster, policies := legacyNetworkPolicyFixture(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, policies[0], policies[1]).Build()
	migration := &LegacySecurityController{Client: c, isOpenShift: true}
	network := &NetworkPolicyController{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(100), isOpenShift: true}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	_, err := migration.Reconcile(t.Context(), request)
	require.NoError(t, err)
	_, err = network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	for _, name := range []string{cluster.Name + "-head", cluster.Name + "-workers-small-group"} {
		policy := &networkingv1.NetworkPolicy{}
		key := client.ObjectKey{Namespace: cluster.Namespace, Name: name}
		require.NoError(t, c.Get(t.Context(), key, policy))
		want := policy.Spec.DeepCopy()
		require.NoError(t, c.Delete(t.Context(), policy))
		_, err = network.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, c.Get(t.Context(), key, policy))
		assert.Equal(t, want, &policy.Spec)
		assert.True(t, metav1.IsControlledBy(policy, cluster))
	}
}

func TestLegacyNetworkPolicyDelayedMigrationAndRestart(t *testing.T) {
	for _, annotation := range []string{"true", "1", "yes", "ON"} {
		t.Run(annotation, func(t *testing.T) {
			scheme, cluster, policies := legacyNetworkPolicyFixture(t)
			cluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey] = annotation
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, policies[0], policies[1]).Build()
			recorder := events.NewFakeRecorder(100)
			newController := func() *NetworkPolicyController {
				return &NetworkPolicyController{Client: c, Scheme: scheme, Recorder: recorder, isOpenShift: true}
			}
			network := newController()
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
			for attempt := range 2*legacyNetworkPolicyMigrationMaxRetries + 3 {
				result, err := network.Reconcile(t.Context(), request)
				require.NoError(t, err)
				require.Positive(t, result.RequeueAfter, "migration must remain retryable")
				for _, policy := range policies {
					got := &networkingv1.NetworkPolicy{}
					require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), got))
					assert.Equal(t, policy.Spec, got.Spec)
					assert.Equal(t, policy.UID, got.UID)
				}
				if attempt == legacyNetworkPolicyMigrationMaxRetries+1 {
					network = newController() // Restart loses the in-memory retry count.
				}
			}
			require.Len(t, recorder.Events, 2, "warn once before restart and once after restart")
			for range 2 {
				assert.Contains(t, <-recorder.Events, "LegacySecurityMigrationPending")
			}
			migration := &LegacySecurityController{Client: c, isOpenShift: true}
			_, err := migration.Reconcile(t.Context(), request)
			require.NoError(t, err)
			_, err = network.Reconcile(t.Context(), request)
			require.NoError(t, err)
			assert.Empty(t, network.legacyMigrationRetries)
			worker := &networkingv1.NetworkPolicy{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-workers-small-group"}, worker))
			assert.Equal(t, "small-group", worker.Spec.PodSelector.MatchLabels[utils.RayNodeGroupLabelKey])
			assert.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(policies[1]), &networkingv1.NetworkPolicy{})))
		})
	}
}

func TestLegacyNetworkPolicyMigrationFailuresPreserveIsolation(t *testing.T) {
	for _, failure := range []string{"migration patch", "head update", "worker create"} {
		t.Run(failure, func(t *testing.T) {
			scheme, cluster, policies := legacyNetworkPolicyFixture(t)
			failed := false
			var changes []string
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, policies[0], policies[1]).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if failure == "migration patch" && !failed {
							failed = true
							return apierrors.NewConflict(schema.GroupResource{Group: "ray.io", Resource: "rayclusters"}, obj.GetName(), fmt.Errorf("concurrent update"))
						}
						return c.Patch(ctx, obj, patch, opts...)
					},
					Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
						if _, ok := obj.(*networkingv1.NetworkPolicy); ok && failure == "head update" && !failed {
							failed = true
							return apierrors.NewConflict(schema.GroupResource{Group: "networking.k8s.io", Resource: "networkpolicies"}, obj.GetName(), fmt.Errorf("concurrent update"))
						}
						return c.Update(ctx, obj, opts...)
					},
					Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if policy, ok := obj.(*networkingv1.NetworkPolicy); ok && policy.Name == cluster.Name+"-workers-small-group" {
							if failure == "worker create" && !failed {
								failed = true
								return apierrors.NewServiceUnavailable("temporary API failure")
							}
							changes = append(changes, "create worker")
						}
						return c.Create(ctx, obj, opts...)
					},
					Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
						if policy, ok := obj.(*networkingv1.NetworkPolicy); ok && policy.Name == policies[1].Name {
							// The replacement must exist in the API before the old policy is removed.
							require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-workers-small-group"}, &networkingv1.NetworkPolicy{}))
							changes = append(changes, "delete legacy worker")
						}
						return c.Delete(ctx, obj, opts...)
					},
				}).Build()
			migration := &LegacySecurityController{Client: c, isOpenShift: true}
			network := &NetworkPolicyController{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(100), isOpenShift: true}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
			_, err := migration.Reconcile(t.Context(), request)
			if failure == "migration patch" {
				require.Error(t, err)
				_, err = network.Reconcile(t.Context(), request)
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
				_, err = network.Reconcile(t.Context(), request)
				require.Error(t, err)
			}
			require.True(t, failed)
			for _, policy := range policies {
				got := &networkingv1.NetworkPolicy{}
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), got))
				assert.Equal(t, policy.UID, got.UID)
			}
			_, err = migration.Reconcile(t.Context(), request)
			require.NoError(t, err)
			_, err = network.Reconcile(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, []string{"create worker", "delete legacy worker"}, changes)
		})
	}
}
