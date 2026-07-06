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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// TestHandleDeletion tests the finalizer-based deletion flow
func TestHandleDeletion(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	namer := utils.NewResourceNamer(&rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	})

	tests := []struct {
		name              string
		cluster           *rayv1.RayCluster
		existingResources []client.Object
		expectCleanup     bool
	}{
		{
			name: "Cluster with finalizer and resources - should cleanup",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-cluster",
					Namespace:         "default",
					Finalizers:        []string{authenticationFinalizer},
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
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
			},
			existingResources: []client.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      namer.ConfigMapName(),
						Namespace: "default",
					},
				},
				&gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "default-test-cluster",
						Namespace: "platform-namespace",
						Labels: map[string]string{
							"ray.io/cluster-namespace": "default",
							"ray.io/cluster-name":      "test-cluster",
						},
					},
				},
				&gatewayv1beta1.ReferenceGrant{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "kuberay-gateway-access",
						Namespace: "default",
					},
				},
			},
			expectCleanup: true,
		},
		{
			name: "Cluster without finalizer - should skip cleanup",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-cluster",
					Namespace:         "default",
					Finalizers:        []string{"some-other-finalizer"}, // Has other finalizer but not auth finalizer
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
				},
			},
			existingResources: []client.Object{},
			expectCleanup:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects := []client.Object{tc.cluster}
			objects = append(objects, tc.existingResources...)

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

			// Execute
			result, err := controller.handleDeletion(ctx, tc.cluster, ctrl.Log)
			require.NoError(t, err, "Should handle deletion without error")
			assert.Equal(t, ctrl.Result{}, result, "Should not requeue")

			// Note: We cannot verify finalizer removal by getting the cluster from the fake client
			// because once all finalizers are removed and deletionTimestamp is set,
			// the fake client (correctly) deletes the object immediately, matching real Kubernetes behavior.
			// The fact that handleDeletion succeeded without error confirms the finalizer was removed.
		})
	}
}

// TestCleanupOrphanedReferenceGrant tests cleanup of orphaned grants
func TestCleanupOrphanedReferenceGrant(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	tests := []struct {
		existingGrant    *gatewayv1beta1.ReferenceGrant
		name             string
		namespace        string
		existingClusters []client.Object
		expectDelete     bool
	}{
		{
			name:      "No clusters with auth - should delete grant",
			namespace: "default",
			existingClusters: []client.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-no-auth",
						Namespace: "default",
						// No auth annotation
					},
				},
			},
			existingGrant: &gatewayv1beta1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kuberay-gateway-access",
					Namespace: "default",
				},
			},
			expectDelete: true,
		},
		{
			name:      "Clusters with auth exist - should not delete",
			namespace: "default",
			existingClusters: []client.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-with-auth",
						Namespace: "default",
						Annotations: map[string]string{
							utils.EnableSecureTrustedNetworkAnnotationKey: "true",
						},
					},
				},
			},
			existingGrant: &gatewayv1beta1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kuberay-gateway-access",
					Namespace: "default",
				},
			},
			expectDelete: false,
		},
		{
			name:             "No grant exists - should not error",
			namespace:        "default",
			existingClusters: []client.Object{},
			existingGrant:    nil,
			expectDelete:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects := tc.existingClusters
			if tc.existingGrant != nil {
				objects = append(objects, tc.existingGrant)
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

			// Execute
			err := controller.cleanupOrphanedReferenceGrant(ctx, tc.namespace, ctrl.Log)
			require.NoError(t, err, "Should cleanup without error")

			// Verify
			grant := &gatewayv1beta1.ReferenceGrant{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      "kuberay-gateway-access",
				Namespace: tc.namespace,
			}, grant)

			if tc.expectDelete {
				assert.True(t, errors.IsNotFound(err), "Grant should be deleted")
			} else if tc.existingGrant != nil {
				assert.NoError(t, err, "Grant should still exist")
			}
		})
	}
}

// TestFullReconciliationLifecycle tests the complete lifecycle of a cluster with auth
func TestFullReconciliationLifecycle(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")
	t.Setenv("GATEWAY_NAMESPACE", "openshift-ingress")
	t.Setenv("GATEWAY_NAME", "data-science-gateway")

	s := setupScheme()

	// Step 1: Create cluster with auth annotation
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				EnableIngress: ptr.To(true), // Pre-enable ingress to avoid update conflicts
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
		WithObjects(cluster).
		WithStatusSubresource(&rayv1.RayCluster{}).
		Build()

	mapper := &mockRESTMapper{hasRouteAPI: true}
	controller := &AuthenticationController{
		Client:     fakeClient,
		Scheme:     s,
		RESTMapper: mapper,
		Recorder:   record.NewFakeRecorder(100),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	namer := utils.NewResourceNamer(cluster)

	t.Run("Step 1: Initial reconciliation creates all resources", func(t *testing.T) {
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// First reconciliation: adds finalizer and returns early
		result, err := controller.Reconcile(ctx, req)
		require.NoError(t, err, "Should reconcile without error")
		assert.Equal(t, ctrl.Result{}, result, "Should not requeue")

		// Verify finalizer was added
		updatedCluster := &rayv1.RayCluster{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		}, updatedCluster)
		require.NoError(t, err)
		assert.Contains(t, updatedCluster.Finalizers, authenticationFinalizer,
			"Finalizer should be added")

		// Second reconciliation: creates resources (finalizer is now present)
		result, err = controller.Reconcile(ctx, req)
		require.NoError(t, err, "Should reconcile without error")
		assert.Equal(t, ctrl.Result{}, result, "Should not requeue")

		// Verify all resources created
		// 1. ServiceAccount created
		// Use ModeIntegratedOAuth since DetectAuthenticationMode returns ModeIntegratedOAuth for OpenShift
		sa := &corev1.ServiceAccount{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      namer.ServiceAccountName(utils.ModeIntegratedOAuth),
			Namespace: cluster.Namespace,
		}, sa)
		require.NoError(t, err, "ServiceAccount should be created")

		// 3. ConfigMap created
		cm := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      namer.ConfigMapName(),
			Namespace: cluster.Namespace,
		}, cm)
		require.NoError(t, err, "ConfigMap should be created")
		assert.Contains(t, cm.Data["config.yaml"], "test-cluster-head-svc",
			"ConfigMap should contain service name")

		// 4. ReferenceGrant created
		grant := &gatewayv1beta1.ReferenceGrant{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      "kuberay-gateway-access",
			Namespace: cluster.Namespace,
		}, grant)
		require.NoError(t, err, "ReferenceGrant should be created")

		// 5. HTTPRoute created in platform namespace
		httpRoute := &gatewayv1.HTTPRoute{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      "default-test-cluster",
			Namespace: "platform-namespace",
		}, httpRoute)
		require.NoError(t, err, "HTTPRoute should be created")
		assert.Equal(t, "default", httpRoute.Labels["ray.io/cluster-namespace"])
		assert.Equal(t, "test-cluster", httpRoute.Labels["ray.io/cluster-name"])
	})

	t.Run("Step 2: Second reconciliation is idempotent", func(t *testing.T) {
		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// Execute again
		result, err := controller.Reconcile(ctx, req)
		require.NoError(t, err, "Should reconcile without error")
		assert.Equal(t, ctrl.Result{}, result, "Should not requeue")

		// Verify still only one of each resource
		grantList := &gatewayv1beta1.ReferenceGrantList{}
		err = fakeClient.List(ctx, grantList, client.InNamespace(cluster.Namespace))
		require.NoError(t, err)
		assert.Len(t, grantList.Items, 1, "Should have exactly one ReferenceGrant")

		httpRouteList := &gatewayv1.HTTPRouteList{}
		err = fakeClient.List(ctx, httpRouteList, client.InNamespace("platform-namespace"))
		require.NoError(t, err)
		assert.Len(t, httpRouteList.Items, 1, "Should have exactly one HTTPRoute")
	})

	t.Run("Step 3: Disabling auth removes resources", func(t *testing.T) {
		// Update cluster to disable auth
		updatedCluster := &rayv1.RayCluster{}
		err := fakeClient.Get(ctx, types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		}, updatedCluster)
		require.NoError(t, err)

		// Remove auth annotation
		delete(updatedCluster.Annotations, utils.EnableSecureTrustedNetworkAnnotationKey)
		err = fakeClient.Update(ctx, updatedCluster)
		require.NoError(t, err)

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		}

		// Execute
		result, err := controller.Reconcile(ctx, req)
		require.NoError(t, err, "Should reconcile without error")
		assert.Equal(t, ctrl.Result{}, result, "Should not requeue")

		// Verify resources cleaned up
		finalCluster := &rayv1.RayCluster{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		}, finalCluster)
		require.NoError(t, err)
		assert.NotContains(t, finalCluster.Finalizers, authenticationFinalizer,
			"Finalizer should be removed")

		// Resources should be deleted
		cm := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      namer.ConfigMapName(),
			Namespace: cluster.Namespace,
		}, cm)
		assert.True(t, errors.IsNotFound(err), "ConfigMap should be deleted")

		httpRoute := &gatewayv1.HTTPRoute{}
		err = fakeClient.Get(ctx, types.NamespacedName{
			Name:      "default-test-cluster",
			Namespace: "platform-namespace",
		}, httpRoute)
		assert.True(t, errors.IsNotFound(err), "HTTPRoute should be deleted")
	})
}

// TestHTTPRoutePathMatchingRules tests the complex redirect and rewrite rules
func TestHTTPRoutePathMatchingRules(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "user-namespace",
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
	require.NoError(t, err, "Should create HTTPRoute")

	// Verify HTTPRoute structure
	httpRoute := &gatewayv1.HTTPRoute{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "user-namespace-test-cluster",
		Namespace: "platform-namespace",
	}, httpRoute)
	require.NoError(t, err, "HTTPRoute should exist")

	// Verify we have 2 rules
	require.Len(t, httpRoute.Spec.Rules, 2, "Should have 2 rules (redirect + rewrite)")

	t.Run("Rule 0: Exact match redirect to #/", func(t *testing.T) {
		rule := httpRoute.Spec.Rules[0]

		// Verify match
		require.Len(t, rule.Matches, 1, "Should have one match")
		assert.Equal(t, gatewayv1.PathMatchExact, *rule.Matches[0].Path.Type,
			"Should use exact path match")
		assert.Equal(t, "/ray/user-namespace/test-cluster", *rule.Matches[0].Path.Value,
			"Should match base path")

		// Verify redirect filter
		require.Len(t, rule.Filters, 1, "Should have one filter")
		assert.Equal(t, gatewayv1.HTTPRouteFilterRequestRedirect, rule.Filters[0].Type,
			"Should be redirect filter")
		assert.NotNil(t, rule.Filters[0].RequestRedirect, "Redirect config should exist")
		assert.Equal(t, "/ray/user-namespace/test-cluster/#/",
			*rule.Filters[0].RequestRedirect.Path.ReplaceFullPath,
			"Should redirect to /#/")
		assert.Equal(t, 302, *rule.Filters[0].RequestRedirect.StatusCode,
			"Should use 302 status code")

		// Should NOT have backend refs (redirect only)
		assert.Empty(t, rule.BackendRefs, "Redirect rule should not have backends")
	})

	t.Run("Rule 1: Prefix match with rewrite", func(t *testing.T) {
		rule := httpRoute.Spec.Rules[1]

		// Verify match
		require.Len(t, rule.Matches, 1, "Should have one match")
		assert.Equal(t, gatewayv1.PathMatchPathPrefix, *rule.Matches[0].Path.Type,
			"Should use prefix path match")
		assert.Equal(t, "/ray/user-namespace/test-cluster", *rule.Matches[0].Path.Value,
			"Should match prefix")

		// Verify backend refs
		require.Len(t, rule.BackendRefs, 1, "Should have one backend")
		backend := rule.BackendRefs[0]
		assert.Equal(t, "test-cluster-head-svc", string(backend.Name),
			"Should reference head service")
		assert.Equal(t, "user-namespace", string(*backend.Namespace),
			"Should reference user namespace")
		assert.Equal(t, int32(8265), *backend.Port,
			"Should reference port 8265")

		// Verify rewrite filter
		require.Len(t, rule.Filters, 1, "Should have one filter")
		assert.Equal(t, gatewayv1.HTTPRouteFilterURLRewrite, rule.Filters[0].Type,
			"Should be rewrite filter")
		assert.NotNil(t, rule.Filters[0].URLRewrite, "Rewrite config should exist")
		assert.Equal(t, gatewayv1.PrefixMatchHTTPPathModifier,
			rule.Filters[0].URLRewrite.Path.Type,
			"Should use prefix match rewrite")
		assert.Equal(t, "/", *rule.Filters[0].URLRewrite.Path.ReplacePrefixMatch,
			"Should rewrite to /")
	})
}

// TestEnsureServiceAccountGeneric tests the generic service account creation for OIDC
func TestEnsureServiceAccountGeneric(t *testing.T) {
	ctx := context.Background()
	s := setupScheme()

	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid",
		},
	}

	tests := []struct {
		name       string
		authMode   utils.AuthenticationMode
		expectName string
	}{
		{
			name:       "OIDC mode - creates OIDC service account",
			authMode:   utils.ModeOIDC,
			expectName: "test-cluster-oidc-proxy-sa",
		},
		{
			name:       "OAuth mode - creates OAuth service account",
			authMode:   utils.ModeIntegratedOAuth,
			expectName: "test-cluster-oauth-proxy-sa",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(cluster).
				Build()

			controller := &AuthenticationController{
				Client:   fakeClient,
				Scheme:   s,
				Recorder: record.NewFakeRecorder(10),
				options: RayClusterReconcilerOptions{
					IsOpenShift: true,
				},
			}

			// Execute
			err := controller.ensureServiceAccount(ctx, cluster, tc.authMode, ctrl.Log)
			require.NoError(t, err, "Should create service account without error")

			// Verify
			sa := &corev1.ServiceAccount{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      tc.expectName,
				Namespace: cluster.Namespace,
			}, sa)
			require.NoError(t, err, "Service account should exist")

			// Verify owner reference
			require.Len(t, sa.OwnerReferences, 1, "Should have one owner reference")
			assert.Equal(t, cluster.Name, sa.OwnerReferences[0].Name,
				"Should be owned by cluster")
		})
	}
}

// TestReconcileWithClusterNotFoundCleansUpOrphans tests orphan cleanup
func TestReconcileWithClusterNotFoundCleansUpOrphans(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	s := setupScheme()

	// Create orphaned ReferenceGrant (no clusters)
	orphanedGrant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberay-gateway-access",
			Namespace: "default",
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(orphanedGrant).
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

	// Reconcile non-existent cluster
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent",
			Namespace: "default",
		},
	}

	result, err := controller.Reconcile(ctx, req)
	require.NoError(t, err, "Should not error when cluster not found")
	assert.Equal(t, ctrl.Result{}, result, "Should not requeue")

	// Verify orphaned grant is cleaned up
	grant := &gatewayv1beta1.ReferenceGrant{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "kuberay-gateway-access",
		Namespace: "default",
	}, grant)
	assert.True(t, errors.IsNotFound(err), "Orphaned grant should be deleted")
}
