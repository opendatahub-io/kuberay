/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ray

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// TestCleanupReferenceGrantWithMixedClusters tests that only clusters with auth are counted
func TestCleanupReferenceGrantWithMixedClusters(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Cluster with auth enabled (OIDC)
	clusterWithAuth := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-with-auth",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	// Cluster WITHOUT auth
	clusterWithoutAuth := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-without-auth",
			Namespace: "user-namespace",
			// No auth annotation
		},
	}

	// Another cluster being deleted (has auth)
	clusterBeingDeleted := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-being-deleted",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	// Create the shared ReferenceGrant
	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(clusterWithAuth, clusterWithoutAuth, clusterBeingDeleted, grant).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Delete clusterBeingDeleted
	// ReferenceGrant should be RETAINED because clusterWithAuth still exists
	// clusterWithoutAuth should NOT be counted (no auth)
	err := controller.cleanupReferenceGrant(ctx, clusterBeingDeleted, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ReferenceGrant still exists
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should still exist (clusterWithAuth remains)")
}

// TestCleanupReferenceGrantNotFound tests that cleanup doesn't error if grant doesn't exist
func TestCleanupReferenceGrantNotFound(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Try to cleanup when ReferenceGrant doesn't exist
	// Should not error (last cluster case)
	err := controller.cleanupReferenceGrant(ctx, cluster, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should not error when grant doesn't exist")
}

// TestCleanupReferenceGrantWithDifferentAuthModes tests OAuth and OIDC clusters both count
func TestCleanupReferenceGrantWithDifferentAuthModes(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Cluster with OIDC auth
	clusterOIDC := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-oidc",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	// Cluster with OAuth auth (different annotation but still auth)
	clusterOAuth := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-oauth",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true", // Using same annotation for test
			},
		},
	}

	// Cluster being deleted (also has auth)
	clusterBeingDeleted := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-deleted",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(clusterOIDC, clusterOAuth, clusterBeingDeleted, grant).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Delete one cluster - both other auth-enabled clusters should be counted
	err := controller.cleanupReferenceGrant(ctx, clusterBeingDeleted, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ReferenceGrant still exists (2 auth clusters remain)
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should still exist (2 clusters with auth remain)")
}

// TestReferenceGrantIdempotency tests that multiple clusters can safely call ensureReferenceGrant
func TestReferenceGrantIdempotency(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	cluster1 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-1",
			Namespace: "user-namespace",
		},
	}

	cluster2 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-2",
			Namespace: "user-namespace",
		},
	}

	cluster3 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-3",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster1, cluster2, cluster3).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// All three clusters call ensureReferenceGrant
	err := controller.ensureReferenceGrant(ctx, cluster1, ctrl.Log)
	require.NoError(t, err, "First call should succeed")

	err = controller.ensureReferenceGrant(ctx, cluster2, ctrl.Log)
	require.NoError(t, err, "Second call should succeed")

	err = controller.ensureReferenceGrant(ctx, cluster3, ctrl.Log)
	require.NoError(t, err, "Third call should succeed")

	// Verify exactly ONE grant exists
	grantList := &gatewayv1beta1.ReferenceGrantList{}
	err = fakeClient.List(ctx, grantList, client.InNamespace("user-namespace"))
	require.NoError(t, err, "List should succeed")

	assert.Len(t, grantList.Items, 1, "Should have exactly ONE shared grant")
	assert.Equal(t, "kuberay-gateway-access", grantList.Items[0].Name, "Grant should have shared name")
}

// TestCleanupReferenceGrantOnlyCountsAuthEnabledClusters tests the counting logic
func TestCleanupReferenceGrantOnlyCountsAuthEnabledClusters(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// 5 clusters total, but only 2 have auth enabled
	clusters := []*rayv1.RayCluster{
		// Auth enabled
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-1",
				Namespace: "user-namespace",
				Annotations: map[string]string{
					utils.EnableSecureTrustedNetworkAnnotationKey: "true",
				},
			},
		},
		// Auth enabled
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-2",
				Namespace: "user-namespace",
				Annotations: map[string]string{
					utils.EnableSecureTrustedNetworkAnnotationKey: "true",
				},
			},
		},
		// No auth
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-3",
				Namespace: "user-namespace",
			},
		},
		// Auth disabled
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-4",
				Namespace: "user-namespace",
				Annotations: map[string]string{
					utils.EnableSecureTrustedNetworkAnnotationKey: "false",
				},
			},
		},
		// Being deleted (has auth)
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cluster-5-deleted",
				Namespace: "user-namespace",
				Annotations: map[string]string{
					utils.EnableSecureTrustedNetworkAnnotationKey: "true",
				},
			},
		},
	}

	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access",
			Namespace: "user-namespace",
		},
	}

	objects := []client.Object{grant}
	for _, c := range clusters {
		objects = append(objects, c)
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objects...).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Delete cluster-5-deleted
	// Should count cluster-1 and cluster-2 (both have auth)
	// Should NOT count cluster-3 and cluster-4 (no auth)
	// Total auth clusters: 2, so grant should be RETAINED
	err := controller.cleanupReferenceGrant(ctx, clusters[4], utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ReferenceGrant still exists (2 auth-enabled clusters remain)
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should still exist (2 clusters with auth remain)")
}

// TestReferenceGrantDifferentNamespaces tests that grants are namespace-scoped
func TestReferenceGrantDifferentNamespaces(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Cluster in namespace-a
	clusterA := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-a",
			Namespace: "namespace-a",
		},
	}

	// Cluster in namespace-b
	clusterB := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-b",
			Namespace: "namespace-b",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(clusterA, clusterB).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Create grant in namespace-a
	err := controller.ensureReferenceGrant(ctx, clusterA, ctrl.Log)
	require.NoError(t, err, "Should create grant in namespace-a")

	// Create grant in namespace-b
	err = controller.ensureReferenceGrant(ctx, clusterB, ctrl.Log)
	require.NoError(t, err, "Should create grant in namespace-b")

	// Verify grant exists in namespace-a
	rgA := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "namespace-a",
	}, rgA)
	require.NoError(t, err, "Grant should exist in namespace-a")

	// Verify grant exists in namespace-b
	rgB := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "namespace-b",
	}, rgB)
	require.NoError(t, err, "Grant should exist in namespace-b")

	// Verify they're different resources
	assert.Equal(t, "namespace-a", rgA.Namespace)
	assert.Equal(t, "namespace-b", rgB.Namespace)
}

// TestCleanupReferenceGrantAllClustersWithoutAuth tests deletion when no auth clusters remain
func TestCleanupReferenceGrantAllClustersWithoutAuth(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Cluster being deleted (has auth)
	clusterWithAuth := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-with-auth",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	// Other cluster WITHOUT auth
	clusterWithoutAuth := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-without-auth",
			Namespace: "user-namespace",
			// No auth annotation
		},
	}

	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(clusterWithAuth, clusterWithoutAuth, grant).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Delete the cluster with auth
	// clusterWithoutAuth should NOT be counted
	// So grant should be DELETED (no auth clusters remain)
	err := controller.cleanupReferenceGrant(ctx, clusterWithAuth, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ReferenceGrant is deleted
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	assert.True(t, errors.IsNotFound(err), "ReferenceGrant should be deleted (no auth clusters remain)")
}

// TestEnsureReferenceGrantLabels tests that the grant has proper labels
func TestEnsureReferenceGrantLabels(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Create ReferenceGrant
	err := controller.ensureReferenceGrant(ctx, cluster, ctrl.Log)
	require.NoError(t, err, "Should create ReferenceGrant")

	// Verify labels
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should exist")

	assert.Equal(t, "kuberay-operator", rg.Labels["app.kubernetes.io/managed-by"],
		"Should have managed-by label")
	assert.Equal(t, "gateway-access", rg.Labels["app.kubernetes.io/component"],
		"Should have component label")
}

// TestCleanupOldHTTPRouteFromClusterNamespace tests migration from old implementation
func TestCleanupOldHTTPRouteFromClusterNamespace(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	namer := utils.NewResourceNamer(&rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	})

	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	}

	// Old HTTPRoute in cluster namespace (from previous implementation)
	oldHttpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",   // Old naming: just cluster name
			Namespace: "user-namespace", // Old location: cluster namespace
		},
	}

	// New HTTPRoute in platform namespace
	newHttpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "user-namespace-test-cluster", // New naming
			Namespace: "platform-namespace",          // New location
			Labels: map[string]string{
				"ray.io/cluster-namespace": "user-namespace",
				"ray.io/cluster-name":      "test-cluster",
			},
		},
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namer.ConfigMapName(),
			Namespace: "user-namespace",
		},
	}

	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namer.ServiceAccountName(utils.ModeOIDC),
			Namespace: "user-namespace",
		},
	}

	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster, oldHttpRoute, newHttpRoute, configMap, serviceAccount, grant).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Execute cleanup
	err := controller.cleanupOIDCResources(ctx, cluster, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify old HTTPRoute in cluster namespace is deleted
	oldHR := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "test-cluster",
		Namespace: "user-namespace",
	}, oldHR)
	assert.True(t, errors.IsNotFound(err), "Old HTTPRoute in cluster namespace should be deleted (migration)")

	// Verify new HTTPRoute in platform namespace is also deleted
	newHR := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "user-namespace-test-cluster",
		Namespace: "platform-namespace",
	}, newHR)
	assert.True(t, errors.IsNotFound(err), "New HTTPRoute in platform namespace should be deleted")
}

// TestCleanupOldHTTPRouteMigrationOnly tests migration when only old route exists
func TestCleanupOldHTTPRouteMigrationOnly(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	}

	// Old HTTPRoute with owner references (from previous implementation)
	oldHttpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       "RayCluster",
					Name:       "test-cluster",
					UID:        "test-uid",
				},
			},
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster, oldHttpRoute).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Execute migration cleanup
	err := controller.cleanupOldHTTPRouteFromClusterNamespace(ctx, cluster, ctrl.Log)
	require.NoError(t, err, "Migration cleanup should succeed")

	// Verify old HTTPRoute is deleted
	oldHR := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "test-cluster",
		Namespace: "user-namespace",
	}, oldHR)
	assert.True(t, errors.IsNotFound(err), "Old HTTPRoute should be deleted")
}

// TestMultipleNamespacesWithSharedGrants tests isolation between namespaces
func TestMultipleNamespacesWithSharedGrants(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Create clusters in different namespaces
	clusterNs1 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-1",
			Namespace: "namespace-1",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	clusterNs2 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-2",
			Namespace: "namespace-2",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(clusterNs1, clusterNs2).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Create grants in both namespaces
	err := controller.ensureReferenceGrant(ctx, clusterNs1, ctrl.Log)
	require.NoError(t, err, "Should create grant in namespace-1")

	err = controller.ensureReferenceGrant(ctx, clusterNs2, ctrl.Log)
	require.NoError(t, err, "Should create grant in namespace-2")

	// Verify grants exist in both namespaces
	grant1 := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "namespace-1",
	}, grant1)
	require.NoError(t, err, "Grant should exist in namespace-1")

	grant2 := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "namespace-2",
	}, grant2)
	require.NoError(t, err, "Grant should exist in namespace-2")

	// Delete cluster in namespace-1
	err = controller.cleanupReferenceGrant(ctx, clusterNs1, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify grant in namespace-1 is deleted
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "namespace-1",
	}, grant1)
	assert.True(t, errors.IsNotFound(err), "Grant in namespace-1 should be deleted")

	// Verify grant in namespace-2 still exists (isolation)
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "namespace-2",
	}, grant2)
	require.NoError(t, err, "Grant in namespace-2 should still exist (different namespace)")
}
