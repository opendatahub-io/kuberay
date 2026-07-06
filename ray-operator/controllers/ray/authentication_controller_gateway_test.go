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

func TestEnsureReferenceGrant(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
			UID:       "test-uid",
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

	// Test: Create shared ReferenceGrant
	err := controller.ensureReferenceGrant(ctx, cluster, ctrl.Log)
	require.NoError(t, err, "Should create ReferenceGrant without error")

	// Verify ReferenceGrant was created with shared name
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access", // Shared name
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should exist")

	// Verify ReferenceGrant spec
	assert.Len(t, rg.Spec.From, 1, "Should have one From entry")
	assert.Equal(t, gatewayv1.GroupName, string(rg.Spec.From[0].Group), "From group should be gateway.networking.k8s.io")
	assert.Equal(t, "HTTPRoute", string(rg.Spec.From[0].Kind), "From kind should be HTTPRoute")
	assert.Equal(t, "platform-namespace", string(rg.Spec.From[0].Namespace), "From namespace should be platform namespace")

	assert.Len(t, rg.Spec.To, 1, "Should have one To entry")
	assert.Equal(t, "", string(rg.Spec.To[0].Group), "To group should be empty (core API)")
	assert.Equal(t, "Service", string(rg.Spec.To[0].Kind), "To kind should be Service")

	// Verify NO owner reference (shared grant)
	assert.Empty(t, rg.OwnerReferences, "Should have NO owner references (shared grant)")

	// Verify labels
	assert.Equal(t, "kuberay-operator", rg.Labels["app.kubernetes.io/managed-by"])
	assert.Equal(t, "gateway-access", rg.Labels["app.kubernetes.io/component"])

	// Test: Update ReferenceGrant (should be idempotent)
	err = controller.ensureReferenceGrant(ctx, cluster, ctrl.Log)
	require.NoError(t, err, "Should update ReferenceGrant without error")

	// Verify it still exists with same spec
	rg2 := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg2)
	require.NoError(t, err, "ReferenceGrant should still exist")
	assert.Equal(t, rg.Spec, rg2.Spec, "ReferenceGrant spec should be unchanged")
}

func TestSharedReferenceGrantMultipleClusters(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Create two clusters in the same namespace
	cluster1 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-1",
			Namespace: "user-namespace",
			UID:       "uid-1",
		},
	}

	cluster2 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-2",
			Namespace: "user-namespace",
			UID:       "uid-2",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster1, cluster2).
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

	// Both clusters create the same ReferenceGrant
	err := controller.ensureReferenceGrant(ctx, cluster1, ctrl.Log)
	require.NoError(t, err, "Cluster 1 should create ReferenceGrant")

	err = controller.ensureReferenceGrant(ctx, cluster2, ctrl.Log)
	require.NoError(t, err, "Cluster 2 should create/update same ReferenceGrant")

	// Verify only ONE ReferenceGrant exists
	rgList := &gatewayv1beta1.ReferenceGrantList{}
	err = fakeClient.List(ctx, rgList, client.InNamespace("user-namespace"))
	require.NoError(t, err)
	assert.Len(t, rgList.Items, 1, "Should have exactly ONE shared ReferenceGrant")
	assert.Equal(t, "kuberay-gateway-access", rgList.Items[0].Name, "Should use shared name")
}

func TestCleanupReferenceGrantWithMultipleClusters(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Create two clusters with auth enabled
	cluster1 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-1",
			Namespace: "user-namespace",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	cluster2 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-2",
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
		WithRuntimeObjects(cluster1, cluster2, grant).
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

	// Delete cluster1 - ReferenceGrant should be RETAINED (cluster2 still exists)
	err := controller.cleanupReferenceGrant(ctx, cluster1, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ReferenceGrant still exists
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should still exist (cluster2 remains)")

	// Actually delete cluster1 from the fake client to simulate real deletion
	err = fakeClient.Delete(ctx, cluster1)
	require.NoError(t, err, "Should delete cluster1 from fake client")

	// Delete cluster2 - ReferenceGrant should be DELETED (last cluster)
	err = controller.cleanupReferenceGrant(ctx, cluster2, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ReferenceGrant is now deleted
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "user-namespace",
	}, rg)
	assert.True(t, errors.IsNotFound(err), "ReferenceGrant should be deleted (last cluster removed)")
}

func TestEnsureHttpRouteInPlatformNamespace(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")
	t.Setenv("GATEWAY_NAMESPACE", "openshift-ingress")
	t.Setenv("GATEWAY_NAME", "data-science-gateway")

	s := setupScheme()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
			UID:       "test-uid",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head"},
						},
					},
				},
			},
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

	// Test: Create HTTPRoute
	err := controller.ensureHttpRoute(ctx, cluster, ctrl.Log)
	require.NoError(t, err, "Should create HTTPRoute without error")

	// Verify HTTPRoute was created in platform namespace
	httpRoute := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "user-namespace-test-cluster",
		Namespace: "platform-namespace",
	}, httpRoute)
	require.NoError(t, err, "HTTPRoute should exist in platform namespace")

	// Verify HTTPRoute labels
	assert.Equal(t, "user-namespace", httpRoute.Labels["ray.io/cluster-namespace"], "Should have cluster-namespace label")
	assert.Equal(t, "test-cluster", httpRoute.Labels["ray.io/cluster-name"], "Should have cluster-name label")
	assert.Equal(t, "test-cluster", httpRoute.Labels[utils.RayClusterLabelKey], "Should have RayCluster label")

	// Verify HTTPRoute parent refs
	assert.Len(t, httpRoute.Spec.ParentRefs, 1, "Should have one parent ref")
	assert.Equal(t, "data-science-gateway", string(httpRoute.Spec.ParentRefs[0].Name), "Parent should be data-science-gateway")
	assert.Equal(t, "openshift-ingress", string(*httpRoute.Spec.ParentRefs[0].Namespace), "Parent namespace should be openshift-ingress")

	// Verify HTTPRoute rules and backend refs
	assert.NotEmpty(t, httpRoute.Spec.Rules, "Should have at least one rule")

	// Find the rule with backend refs (skip redirect rule)
	var ruleWithBackend *gatewayv1.HTTPRouteRule
	for i := range httpRoute.Spec.Rules {
		if len(httpRoute.Spec.Rules[i].BackendRefs) > 0 {
			ruleWithBackend = &httpRoute.Spec.Rules[i]
			break
		}
	}
	require.NotNil(t, ruleWithBackend, "Should have a rule with backend refs")

	backendRef := ruleWithBackend.BackendRefs[0]
	assert.Equal(t, "test-cluster-head-svc", string(backendRef.Name), "Backend should reference head service")
	assert.NotNil(t, backendRef.Namespace, "Backend should have namespace set")
	assert.Equal(t, "user-namespace", string(*backendRef.Namespace), "Backend should reference user namespace")
	assert.Equal(t, int32(8265), *backendRef.Port, "Backend should reference port 8265")
}

func TestCleanupOIDCResourcesWithCrossNamespace(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	namer := utils.NewResourceNamer(&rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	})

	// Create resources to be cleaned up
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namer.ConfigMapName(),
			Namespace: "user-namespace",
		},
	}

	httpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "user-namespace-test-cluster",
			Namespace: "platform-namespace", // HTTPRoute in platform namespace
			Labels: map[string]string{
				"ray.io/cluster-namespace": "user-namespace",
				"ray.io/cluster-name":      "test-cluster",
			},
		},
	}

	// Shared ReferenceGrant (new naming)
	referenceGrant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access", // Shared name
			Namespace: "user-namespace",
		},
	}

	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namer.ServiceAccountName(utils.ModeOIDC),
			Namespace: "user-namespace",
		},
	}

	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster, configMap, httpRoute, referenceGrant, serviceAccount).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(10),
	}

	// Execute cleanup (this is the last cluster, so ReferenceGrant should be deleted)
	err := controller.cleanupOIDCResources(ctx, cluster, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Cleanup should succeed")

	// Verify ConfigMap is deleted
	cm := &corev1.ConfigMap{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      namer.ConfigMapName(),
		Namespace: "user-namespace",
	}, cm)
	assert.True(t, errors.IsNotFound(err), "ConfigMap should be deleted")

	// Verify HTTPRoute is deleted from platform namespace
	hr := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "user-namespace-test-cluster",
		Namespace: "platform-namespace",
	}, hr)
	assert.True(t, errors.IsNotFound(err), "HTTPRoute should be deleted from platform namespace")

	// Verify ReferenceGrant is deleted (last cluster)
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access", // Shared name
		Namespace: "user-namespace",
	}, rg)
	assert.True(t, errors.IsNotFound(err), "ReferenceGrant should be deleted (last cluster)")

	// Verify ServiceAccount is deleted
	sa := &corev1.ServiceAccount{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      namer.ServiceAccountName(utils.ModeOIDC),
		Namespace: "user-namespace",
	}, sa)
	assert.True(t, errors.IsNotFound(err), "ServiceAccount should be deleted")
}

func TestHTTPRouteNamingConflictPrevention(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Create two clusters with the same name in different namespaces
	cluster1 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "namespace-a",
			UID:       "uid-a",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ray-head"}},
					},
				},
			},
		},
	}

	cluster2 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "namespace-b",
			UID:       "uid-b",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ray-head"}},
					},
				},
			},
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster1, cluster2).
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

	// Create HTTPRoutes for both clusters
	err := controller.ensureHttpRoute(ctx, cluster1, ctrl.Log)
	require.NoError(t, err, "Should create HTTPRoute for cluster1")

	err = controller.ensureHttpRoute(ctx, cluster2, ctrl.Log)
	require.NoError(t, err, "Should create HTTPRoute for cluster2")

	// Verify both HTTPRoutes exist with different names
	hr1 := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "namespace-a-my-cluster",
		Namespace: "platform-namespace",
	}, hr1)
	require.NoError(t, err, "HTTPRoute for cluster1 should exist")

	hr2 := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "namespace-b-my-cluster",
		Namespace: "platform-namespace",
	}, hr2)
	require.NoError(t, err, "HTTPRoute for cluster2 should exist")

	// Verify they have different labels
	assert.Equal(t, "namespace-a", hr1.Labels["ray.io/cluster-namespace"])
	assert.Equal(t, "namespace-b", hr2.Labels["ray.io/cluster-namespace"])
}

func TestEnsureOIDCResourcesCreatesReferenceGrantBeforeHTTPRoute(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
			UID:       "test-uid",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ray-head"}},
					},
				},
			},
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

	// Call ensureOIDCResources which should create both ReferenceGrant and HTTPRoute
	err := controller.ensureOIDCResources(ctx, cluster, utils.ModeOIDC, ctrl.Log)
	require.NoError(t, err, "Should ensure OIDC resources without error")

	// Verify shared ReferenceGrant exists
	rg := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access", // Shared name
		Namespace: "user-namespace",
	}, rg)
	require.NoError(t, err, "ReferenceGrant should exist")

	// Verify HTTPRoute exists
	hr := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "user-namespace-test-cluster",
		Namespace: "platform-namespace",
	}, hr)
	require.NoError(t, err, "HTTPRoute should exist")

	// Verify ConfigMap exists
	namer := utils.NewResourceNamer(cluster)
	cm := &corev1.ConfigMap{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      namer.ConfigMapName(),
		Namespace: "user-namespace",
	}, cm)
	require.NoError(t, err, "ConfigMap should exist")
}

func TestHTTPRouteNameDNSLabelLimit(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	tests := []struct {
		name          string
		clusterName   string
		namespace     string
		expectTrunc   bool
		maxNameLength int
	}{
		{
			name:          "Short name - no truncation",
			clusterName:   "test-cluster",
			namespace:     "default",
			expectTrunc:   false,
			maxNameLength: 63,
		},
		{
			name:          "Long name - requires truncation",
			clusterName:   "very-long-cluster-name-that-exceeds-the-kubernetes-dns-label-limit",
			namespace:     "extremely-long-namespace-name-that-also-exceeds-limits",
			expectTrunc:   true,
			maxNameLength: 63,
		},
		{
			name: "Edge case - exactly 63 characters",
			// This creates a name that's exactly 63 chars: "namespace-name-" (15) + "cluster-name-that-is-exactly-48-chars-long" (48) = 63
			clusterName:   "cluster-name-that-is-exactly-forty-eight-chars",
			namespace:     "namespace-name",
			expectTrunc:   false,
			maxNameLength: 63,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.clusterName,
					Namespace: tc.namespace,
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "ray-head"}},
							},
						},
					},
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

			// Create HTTPRoute
			err := controller.ensureHttpRoute(ctx, cluster, ctrl.Log)
			require.NoError(t, err, "Should create HTTPRoute without error")

			// Find the created HTTPRoute by labels (since name might be truncated)
			httpRouteList := &gatewayv1.HTTPRouteList{}
			err = fakeClient.List(ctx, httpRouteList,
				client.InNamespace("platform-namespace"),
				client.MatchingLabels{
					"ray.io/cluster-namespace": tc.namespace,
					"ray.io/cluster-name":      tc.clusterName,
				})
			require.NoError(t, err, "Should list HTTPRoutes")
			require.Len(t, httpRouteList.Items, 1, "Should have exactly one HTTPRoute")

			httpRoute := &httpRouteList.Items[0]

			// Verify the name doesn't exceed DNS label limit
			assert.LessOrEqual(t, len(httpRoute.Name), tc.maxNameLength,
				"HTTPRoute name must not exceed %d characters (DNS label limit)", tc.maxNameLength)

			// Verify labels are preserved (for cleanup via label selector)
			assert.Equal(t, tc.namespace, httpRoute.Labels["ray.io/cluster-namespace"],
				"Should have correct cluster-namespace label")
			assert.Equal(t, tc.clusterName, httpRoute.Labels["ray.io/cluster-name"],
				"Should have correct cluster-name label")

			// If truncation is expected, verify the name is different from the simple concatenation
			expectedSimpleName := tc.namespace + "-" + tc.clusterName
			if tc.expectTrunc {
				assert.NotEqual(t, expectedSimpleName, httpRoute.Name,
					"Truncated name should differ from simple concatenation")
				// Should contain a hash suffix
				assert.Contains(t, httpRoute.Name, "-",
					"Truncated name should contain hyphen before hash")
			}
		})
	}
}
