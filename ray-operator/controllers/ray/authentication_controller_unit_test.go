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

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// setupScheme creates a scheme with all necessary types registered
func setupScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = rbacv1.AddToScheme(s)
	_ = rayv1.AddToScheme(s)
	_ = configv1.AddToScheme(s)
	_ = routev1.AddToScheme(s)
	_ = gatewayv1.AddToScheme(s)
	_ = gatewayv1beta1.AddToScheme(s)
	return s
}

// mockRESTMapper is a simple mock implementation of meta.RESTMapper for testing
type mockRESTMapper struct {
	hasRouteAPI bool
}

func (m *mockRESTMapper) KindFor(_ schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}

func (m *mockRESTMapper) KindsFor(_ schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return nil, nil
}

func (m *mockRESTMapper) ResourceFor(_ schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return schema.GroupVersionResource{}, nil
}

func (m *mockRESTMapper) ResourcesFor(_ schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return nil, nil
}

func (m *mockRESTMapper) RESTMapping(gk schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
	if gk.Group == routev1.GroupVersion.Group && gk.Kind == "Route" && !m.hasRouteAPI {
		return nil, &meta.NoKindMatchError{}
	}
	return &meta.RESTMapping{}, nil
}

func (m *mockRESTMapper) RESTMappings(_ schema.GroupKind, _ ...string) ([]*meta.RESTMapping, error) {
	return nil, nil
}

func (m *mockRESTMapper) ResourceSingularizer(_ string) (singular string, err error) {
	return "", nil
}

func TestDetectAuthenticationMode(t *testing.T) {
	tests := []struct {
		name     string
		expected utils.AuthenticationMode
		objects  []runtime.Object
		options  RayClusterReconcilerOptions
	}{
		{
			name: "Not OpenShift - returns ModeIntegratedOAuth (default)",
			options: RayClusterReconcilerOptions{
				IsOpenShift: false,
			},
			objects:  []runtime.Object{},
			expected: utils.ModeIntegratedOAuth,
		},
		{
			name: "OpenShift - returns ModeIntegratedOAuth (OIDC detection is commented out)",
			options: RayClusterReconcilerOptions{
				IsOpenShift: true,
			},
			objects: []runtime.Object{
				&configv1.Authentication{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster",
					},
					Spec: configv1.AuthenticationSpec{
						OIDCProviders: []configv1.OIDCProvider{
							{Name: "test-oidc"},
						},
					},
				},
			},
			expected: utils.ModeIntegratedOAuth, // Changed: OIDC detection is disabled
		},
		{
			name: "OpenShift with OAuth identity providers - returns ModeIntegratedOAuth",
			options: RayClusterReconcilerOptions{
				IsOpenShift: true,
			},
			objects: []runtime.Object{
				&configv1.Authentication{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster",
					},
					Spec: configv1.AuthenticationSpec{},
				},
				&configv1.OAuth{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster",
					},
					Spec: configv1.OAuthSpec{
						IdentityProviders: []configv1.IdentityProvider{
							{Name: "test-oauth"},
						},
					},
				},
			},
			expected: utils.ModeIntegratedOAuth,
		},
		{
			name: "OpenShift with empty OAuth spec - returns ModeIntegratedOAuth",
			options: RayClusterReconcilerOptions{
				IsOpenShift: true,
			},
			objects: []runtime.Object{
				&configv1.Authentication{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster",
					},
					Spec: configv1.AuthenticationSpec{},
				},
				&configv1.OAuth{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster",
					},
					Spec: configv1.OAuthSpec{},
				},
			},
			expected: utils.ModeIntegratedOAuth,
		},
		{
			name: "OpenShift without auth resources - returns ModeIntegratedOAuth (default)",
			options: RayClusterReconcilerOptions{
				IsOpenShift: true,
			},
			objects:  []runtime.Object{},
			expected: utils.ModeIntegratedOAuth,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Test
			result := utils.DetectAuthenticationMode(tc.options.IsOpenShift)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestShouldEnableOAuth(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *rayv1.RayCluster
		authMode utils.AuthenticationMode
		expected bool
	}{
		{
			name: "Annotation enabled with IntegratedOAuth - should enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						utils.EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: utils.ModeIntegratedOAuth,
			expected: true,
		},
		{
			name: "Annotation enabled with OIDC - should not enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						utils.EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: utils.ModeOIDC,
			expected: false,
		},
		{
			name: "Annotation disabled with IntegratedOAuth - should not enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						utils.EnableSecureTrustedNetworkAnnotationKey: "false",
					},
				},
			},
			authMode: utils.ModeIntegratedOAuth,
			expected: false,
		},
		{
			name: "No annotation with IntegratedOAuth - should not enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
			},
			authMode: utils.ModeIntegratedOAuth,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := utils.ShouldEnableOAuth(tc.cluster, tc.authMode)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestShouldEnableOIDC(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *rayv1.RayCluster
		authMode utils.AuthenticationMode
		expected bool
	}{
		{
			name: "Annotation enabled with OIDC - should enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						utils.EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: utils.ModeOIDC,
			expected: true,
		},
		{
			name: "Annotation enabled with IntegratedOAuth - should not enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						utils.EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: utils.ModeIntegratedOAuth,
			expected: false,
		},
		{
			name: "Annotation disabled with OIDC - should not enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						utils.EnableSecureTrustedNetworkAnnotationKey: "false",
					},
				},
			},
			authMode: utils.ModeOIDC,
			expected: false,
		},
		{
			name: "No annotation with OIDC - should not enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
			},
			authMode: utils.ModeOIDC,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := utils.ShouldEnableOIDC(tc.cluster, tc.authMode)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestEnsureOAuthServiceAccount(t *testing.T) {
	tests := []struct {
		existingSA     *corev1.ServiceAccount
		name           string
		expectCreation bool
		expectUpdate   bool
	}{
		{
			name:           "Service account doesn't exist - should create",
			existingSA:     nil,
			expectCreation: true,
			expectUpdate:   false,
		},
		{
			name: "Service account exists without annotation - should update",
			existingSA: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster-oauth-proxy-sa",
					Namespace: "default",
				},
			},
			expectCreation: false,
			expectUpdate:   true,
		},
		{
			name: "Service account exists with annotation - should not update",
			existingSA: &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster-oauth-proxy-sa",
					Namespace: "default",
					Annotations: map[string]string{
						"serviceaccounts.openshift.io/oauth-redirectreference.first": `{"kind":"OAuthRedirectReference","apiVersion":"v1","reference":{"kind":"Route","name":"test-cluster"}}`,
					},
				},
			},
			expectCreation: false,
			expectUpdate:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			ctx := context.Background()
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
			}

			objects := []runtime.Object{cluster}
			if tc.existingSA != nil {
				objects = append(objects, tc.existingSA)
			}

			s := setupScheme()
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objects...).
				Build()

			controller := &AuthenticationController{
				Client:   fakeClient,
				Scheme:   s,
				Recorder: record.NewFakeRecorder(10),
			}

			// Execute
			err := controller.ensureOAuthServiceAccount(ctx, cluster, ctrl.Log)
			require.NoError(t, err)

			// Verify
			sa := &corev1.ServiceAccount{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      "test-cluster-oauth-proxy-sa",
				Namespace: "default",
			}, sa)
			require.NoError(t, err)
			assert.NotNil(t, sa.Annotations)
			assert.Contains(t, sa.Annotations, "serviceaccounts.openshift.io/oauth-redirectreference.first")
		})
	}
}

func TestEnsureOIDCConfigMap(t *testing.T) {
	tests := []struct {
		existingConfigMap *corev1.ConfigMap
		name              string
		expectCreation    bool
		expectUpdate      bool
	}{
		{
			name:              "ConfigMap doesn't exist - should create",
			existingConfigMap: nil,
			expectCreation:    true,
			expectUpdate:      false,
		},
		{
			name: "ConfigMap exists with different data - should update",
			existingConfigMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kube-rbac-proxy-config-test-cluster",
					Namespace: "default",
				},
				Data: map[string]string{
					"config.yaml": "old-config",
				},
			},
			expectCreation: false,
			expectUpdate:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			ctx := context.Background()
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
			}

			objects := []runtime.Object{cluster}
			if tc.existingConfigMap != nil {
				objects = append(objects, tc.existingConfigMap)
			}

			s := setupScheme()
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objects...).
				Build()

			controller := &AuthenticationController{
				Client:   fakeClient,
				Scheme:   s,
				Recorder: record.NewFakeRecorder(10),
			}

			// Execute
			err := controller.ensureOIDCConfigMap(ctx, cluster, ctrl.Log)
			require.NoError(t, err)

			// Verify
			cm := &corev1.ConfigMap{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      "kube-rbac-proxy-config-test-cluster",
				Namespace: "default",
			}, cm)
			require.NoError(t, err)
			assert.Contains(t, cm.Data, "config.yaml")
			assert.Contains(t, cm.Data["config.yaml"], "test-cluster-head-svc")
			assert.Equal(t, "kube-rbac-proxy", cm.Labels["app"])
		})
	}
}

func TestEnsureHttpRoute(t *testing.T) {
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	tests := []struct {
		existingHttpRoute *gatewayv1.HTTPRoute
		name              string
		expectCreation    bool
		expectUpdate      bool
	}{
		{
			name:              "HTTPRoute doesn't exist - should create",
			existingHttpRoute: nil,
			expectCreation:    true,
			expectUpdate:      false,
		},
		{
			name: "HTTPRoute exists - should update",
			existingHttpRoute: &gatewayv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default-test-cluster",
					Namespace: "platform-namespace",
					Labels: map[string]string{
						"ray.io/cluster-namespace": "default",
						"ray.io/cluster-name":      "test-cluster",
					},
				},
			},
			expectCreation: false,
			expectUpdate:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			ctx := context.Background()
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						ServiceType: corev1.ServiceTypeClusterIP,
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

			objects := []runtime.Object{cluster}
			if tc.existingHttpRoute != nil {
				objects = append(objects, tc.existingHttpRoute)
			}

			s := setupScheme()

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objects...).
				Build()

			mapper := &mockRESTMapper{hasRouteAPI: true}
			controller := &AuthenticationController{
				Client:     fakeClient,
				Scheme:     s,
				RESTMapper: mapper,
				Recorder:   record.NewFakeRecorder(10),
			}

			// Execute
			err := controller.ensureHttpRoute(ctx, cluster, ctrl.Log)
			require.NoError(t, err)

			// Verify - HTTPRoute is now in platform namespace with new naming
			route := &gatewayv1.HTTPRoute{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      "default-test-cluster",
				Namespace: "platform-namespace",
			}, route)
			require.NoError(t, err)

			// Should have 2 rules: 1 for redirect, 1 for rewrite
			assert.Len(t, route.Spec.Rules, 2)

			// Rule 0: Exact match for redirect to #/
			assert.Equal(t, gatewayv1.PathMatchExact, *route.Spec.Rules[0].Matches[0].Path.Type)
			assert.Equal(t, "/ray/default/test-cluster", *route.Spec.Rules[0].Matches[0].Path.Value)
			assert.NotNil(t, route.Spec.Rules[0].Filters[0].RequestRedirect)
			assert.Contains(t, *route.Spec.Rules[0].Filters[0].RequestRedirect.Path.ReplaceFullPath, "/#/")

			// Rule 1: Prefix match for normal traffic
			assert.Equal(t, gatewayv1.PathMatchPathPrefix, *route.Spec.Rules[1].Matches[0].Path.Type)
			assert.Equal(t, "/ray/default/test-cluster", *route.Spec.Rules[1].Matches[0].Path.Value)
			assert.NotEmpty(t, route.Spec.Rules[1].BackendRefs)
		})
	}
}

func TestMapAuthResourceToRayClusters(t *testing.T) {
	tests := []struct {
		name             string
		clusters         []runtime.Object
		expectedRequests int
	}{
		{
			name:             "No clusters - should return empty list",
			clusters:         []runtime.Object{},
			expectedRequests: 0,
		},
		{
			name: "One cluster - should return one request",
			clusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster1",
						Namespace: "default",
					},
				},
			},
			expectedRequests: 1,
		},
		{
			name: "Multiple clusters - should return multiple requests",
			clusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster1",
						Namespace: "default",
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster2",
						Namespace: "default",
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster3",
						Namespace: "test-namespace",
					},
				},
			},
			expectedRequests: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			ctx := context.Background()
			s := setupScheme()

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(tc.clusters...).
				Build()

			controller := &AuthenticationController{
				Client:   fakeClient,
				Scheme:   s,
				Recorder: record.NewFakeRecorder(10),
			}

			// Create a fake auth resource to trigger mapping
			authResource := &configv1.Authentication{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster",
				},
			}

			// Execute
			requests := controller.mapAuthResourceToRayClusters(ctx, authResource)

			// Verify
			assert.Len(t, requests, tc.expectedRequests)

			// Verify request names match cluster names
			if tc.expectedRequests > 0 {
				requestNames := make(map[string]bool)
				for _, req := range requests {
					requestNames[req.Name] = true
				}

				for _, obj := range tc.clusters {
					cluster := obj.(*rayv1.RayCluster)
					assert.True(t, requestNames[cluster.Name], "Request should exist for cluster %s", cluster.Name)
				}
			}
		})
	}
}

func TestGetOAuthProxySidecar(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	container := GetOAuthProxySidecar(cluster)

	assert.Equal(t, oauthProxyContainerName, container.Name)
	assert.Equal(t, oauthProxyImage, container.Image)
	assert.NotEmpty(t, container.Args)
	assert.NotEmpty(t, container.Env)
	assert.NotEmpty(t, container.VolumeMounts)
	assert.NotEmpty(t, container.Ports)

	// Verify port configuration
	assert.Equal(t, int32(authProxyPort), container.Ports[0].ContainerPort)
	assert.Equal(t, oauthProxyPortName, container.Ports[0].Name)

	// Verify resource limits are set
	assert.NotNil(t, container.Resources.Requests)
	assert.NotNil(t, container.Resources.Limits)

	// Verify probes are configured
	assert.NotNil(t, container.LivenessProbe)
	assert.NotNil(t, container.ReadinessProbe)
}

func TestGetOAuthProxyVolumes(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	volumes := GetOAuthProxyVolumes(cluster)

	assert.Len(t, volumes, 2)

	// Verify volume names
	volumeNames := make(map[string]bool)
	for _, vol := range volumes {
		volumeNames[vol.Name] = true
	}

	assert.True(t, volumeNames[oauthConfigVolumeName])
	assert.True(t, volumeNames[oauthProxyVolumeName])
}

func TestGetOIDCProxySidecar(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	container := GetOIDCProxySidecar(cluster)

	assert.Equal(t, oidcProxyContainerName, container.Name)
	assert.Equal(t, oidcProxyContainerImage, container.Image)
	assert.NotEmpty(t, container.Args)
	assert.NotEmpty(t, container.VolumeMounts)
	assert.NotEmpty(t, container.Ports)

	// Verify port configuration
	assert.Equal(t, int32(authProxyPort), container.Ports[0].ContainerPort)
	assert.Equal(t, oidcProxyPortName, container.Ports[0].Name)

	// Verify volume mount
	assert.Equal(t, "kube-rbac-proxy-config-"+cluster.Name, container.VolumeMounts[0].Name)
}

func TestGetOIDCProxyVolumes(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	volumes := GetOIDCProxyVolumes(cluster)

	assert.Len(t, volumes, 1)
	assert.Equal(t, "kube-rbac-proxy-config-"+cluster.Name, volumes[0].Name)
	assert.NotNil(t, volumes[0].VolumeSource.ConfigMap)
	assert.Equal(t, "kube-rbac-proxy-config-"+cluster.Name, volumes[0].VolumeSource.ConfigMap.Name)
}

func TestHelperFunctions(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	t.Run("OAuth ServiceAccountName via ResourceNamer", func(t *testing.T) {
		namer := utils.NewResourceNamer(cluster)
		name := namer.ServiceAccountName(utils.ModeIntegratedOAuth)
		assert.Equal(t, "test-cluster-oauth-proxy-sa", name)
	})

	t.Run("OAuth SecretName via ResourceNamer", func(t *testing.T) {
		namer := utils.NewResourceNamer(cluster)
		name := namer.SecretName(utils.ModeIntegratedOAuth)
		assert.Equal(t, "test-cluster-oauth-config", name)
	})

	t.Run("OAuth TLSSecretName via ResourceNamer", func(t *testing.T) {
		namer := utils.NewResourceNamer(cluster)
		name := namer.TLSSecretName(utils.ModeIntegratedOAuth)
		assert.Equal(t, "test-cluster-oauth-tls", name)
	})
}

func TestGenerateSelfSignedCert(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	cert, key, err := generateSelfSignedCert(cluster)

	require.NoError(t, err)
	assert.NotEmpty(t, cert)
	assert.NotEmpty(t, key)

	// Verify cert and key are valid PEM -  Handled this way due to pre-commit hook false positive
	assert.Contains(t, string(cert), "BEGIN CERTIFICATE")
	assert.Contains(t, string(cert), "END CERTIFICATE")
	assert.Contains(t, string(key), "BEGIN "+"PRIVATE KEY")
	assert.Contains(t, string(key), "END "+"PRIVATE KEY")
}

func TestReconcile_RayClusterNotFound(t *testing.T) {
	// Setup
	ctx := context.Background()
	s := setupScheme()

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		Build()

	controller := &AuthenticationController{
		Client:   fakeClient,
		Scheme:   s,
		Recorder: record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: false,
		},
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent-cluster",
			Namespace: "default",
		},
	}

	// Execute
	result, err := controller.Reconcile(ctx, req)

	// Verify - should not error when cluster is not found
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestReconcile_ManagedByExternalController(t *testing.T) {
	// Setup
	ctx := context.Background()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			ManagedBy: ptr.To("external-controller"),
		},
	}

	s := setupScheme()
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster).
		Build()

	controller := &AuthenticationController{
		Client:   fakeClient,
		Scheme:   s,
		Recorder: record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: false,
		},
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	// Execute
	result, err := controller.Reconcile(ctx, req)

	// Verify - should skip reconciliation
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestCleanupOIDCResources(t *testing.T) {
	ctx := context.Background()
	t.Setenv("APPLICATION_NAMESPACE", "platform-namespace")

	namer := utils.NewResourceNamer(&rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	})

	tests := []struct {
		name              string
		existingResources []runtime.Object
		expectDeleted     []string
	}{
		{
			name: "Delete OIDC ConfigMap, HTTPRoute, and ServiceAccount",
			existingResources: []runtime.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      namer.ConfigMapName(),
						Namespace: "default",
					},
				},
				&gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "default-test-cluster", // Updated: namespace-cluster naming
						Namespace: "platform-namespace",   // Updated: in platform namespace
						Labels: map[string]string{
							"ray.io/cluster-namespace": "default",
							"ray.io/cluster-name":      "test-cluster",
						},
					},
				},
				&gatewayv1beta1.ReferenceGrant{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "kuberay-gateway-access", // Shared name
						Namespace: "default",
					},
				},
				&corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:      namer.ServiceAccountName(utils.ModeOIDC),
						Namespace: "default",
					},
				},
			},
			expectDeleted: []string{"configmap", "httproute", "referencegrant", "serviceaccount"},
		},
		{
			name:              "No resources to delete - should not error",
			existingResources: []runtime.Object{},
			expectDeleted:     []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := setupScheme()

			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
			}

			objects := append(tc.existingResources, cluster)
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objects...).
				Build()

			mapper := &mockRESTMapper{hasRouteAPI: true}
			controller := &AuthenticationController{
				Client:     fakeClient,
				Scheme:     s,
				RESTMapper: mapper,
				Recorder:   record.NewFakeRecorder(10),
			}

			// Execute cleanup
			err := controller.cleanupOIDCResources(ctx, cluster, utils.ModeOIDC, ctrl.Log)
			require.NoError(t, err)

			// Verify resources are deleted
			if len(tc.expectDeleted) > 0 {
				// Check ConfigMap is deleted
				configMap := &corev1.ConfigMap{}
				err = fakeClient.Get(ctx, types.NamespacedName{
					Name:      namer.ConfigMapName(),
					Namespace: "default",
				}, configMap)
				assert.True(t, errors.IsNotFound(err), "ConfigMap should be deleted")

				// Check HTTPRoute is deleted from platform namespace
				httpRoute := &gatewayv1.HTTPRoute{}
				err = fakeClient.Get(ctx, types.NamespacedName{
					Name:      "default-test-cluster", // Updated: namespace-cluster naming
					Namespace: "platform-namespace",   // Updated: platform namespace
				}, httpRoute)
				assert.True(t, errors.IsNotFound(err), "HTTPRoute should be deleted from platform namespace")

				// Check ReferenceGrant is deleted (last cluster)
				referenceGrant := &gatewayv1beta1.ReferenceGrant{}
				err = fakeClient.Get(ctx, types.NamespacedName{
					Name:      "kuberay-gateway-access", // Shared name
					Namespace: "default",
				}, referenceGrant)
				assert.True(t, errors.IsNotFound(err), "ReferenceGrant should be deleted (last cluster)")

				// Check ServiceAccount is deleted
				sa := &corev1.ServiceAccount{}
				err = fakeClient.Get(ctx, types.NamespacedName{
					Name:      namer.ServiceAccountName(utils.ModeOIDC),
					Namespace: "default",
				}, sa)
				assert.True(t, errors.IsNotFound(err), "ServiceAccount should be deleted")
			}
		})
	}
}

func TestEnforceEnableIngressFalseOnOpenShift(t *testing.T) {
	s := setupScheme()

	testCases := []struct {
		initialValue *bool
		name         string
		expectUpdate bool
	}{
		{
			name:         "enableIngress is nil - should set to false",
			initialValue: nil,
			expectUpdate: true,
		},
		{
			name:         "enableIngress is true - should override to false",
			initialValue: ptr.To(true),
			expectUpdate: true,
		},
		{
			name:         "enableIngress is already false - no update needed",
			initialValue: ptr.To(false),
			expectUpdate: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test cluster
			rayCluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						EnableIngress: tc.initialValue,
					},
				},
			}

			// Create fake client with the cluster
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithObjects(rayCluster).
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
			err := controller.enforceEnableIngressFalseOnOpenShift(
				context.Background(),
				rayCluster,
				ctrl.Log.WithName("test"))

			// Verify no error
			require.NoError(t, err, "Enforcement should not return error")

			if tc.expectUpdate {
				// Get updated cluster from fake client
				updated := &rayv1.RayCluster{}
				err = fakeClient.Get(context.Background(),
					types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
					updated)
				require.NoError(t, err, "Should be able to get updated cluster")

				// Verify enableIngress is now false
				require.NotNil(t, updated.Spec.HeadGroupSpec.EnableIngress,
					"EnableIngress should be set")
				assert.False(t, *updated.Spec.HeadGroupSpec.EnableIngress,
					"EnableIngress should be false")
			}
		})
	}
}

func TestEnforceEnableIngressFalseOnOpenShift_Idempotency(t *testing.T) {
	s := setupScheme()

	// Create test cluster with enableIngress: true
	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				EnableIngress: ptr.To(true),
			},
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithObjects(rayCluster).
		Build()

	controller := &AuthenticationController{
		Client:   fakeClient,
		Scheme:   s,
		Recorder: record.NewFakeRecorder(10),
		options: RayClusterReconcilerOptions{
			IsOpenShift: true,
		},
	}

	// Call enforcement multiple times
	for i := 0; i < 3; i++ {
		// Get latest version
		err := fakeClient.Get(context.Background(),
			types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
			rayCluster)
		require.NoError(t, err)

		// Enforce
		err = controller.enforceEnableIngressFalseOnOpenShift(
			context.Background(),
			rayCluster,
			ctrl.Log.WithName("test"))
		require.NoError(t, err, "Enforcement should not error on iteration %d", i)
	}

	// Verify final state
	updated := &rayv1.RayCluster{}
	err := fakeClient.Get(context.Background(),
		types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		updated)
	require.NoError(t, err)

	require.NotNil(t, updated.Spec.HeadGroupSpec.EnableIngress)
	assert.False(t, *updated.Spec.HeadGroupSpec.EnableIngress,
		"EnableIngress should be false after multiple enforcements")
}

func TestEnsureAutoscalerRoleBindingForAuthSA(t *testing.T) {
	tests := []struct {
		cluster        *rayv1.RayCluster
		existingRB     *rbacv1.RoleBinding
		name           string
		authMode       utils.AuthenticationMode
		expectSubjects int
		expectPending  bool
		expectAuthSA   bool
		expectNoUpdate bool
	}{
		{
			name: "Autoscaling disabled - no-op",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: ptr.To(false),
				},
			},
			existingRB:     nil,
			authMode:       utils.ModeIntegratedOAuth,
			expectSubjects: 0,
			expectAuthSA:   false,
			expectNoUpdate: true,
		},
		{
			name: "RoleBinding does not exist yet - pending",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: ptr.To(true),
				},
			},
			existingRB:     nil,
			authMode:       utils.ModeIntegratedOAuth,
			expectPending:  true,
			expectSubjects: 0,
			expectAuthSA:   false,
			expectNoUpdate: true,
		},
		{
			name: "RoleBinding exists - adds OAuth proxy SA",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: ptr.To(true),
				},
			},
			existingRB: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Subjects: []rbacv1.Subject{
					{
						Kind:      rbacv1.ServiceAccountKind,
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "Role",
					Name:     "test-cluster",
				},
			},
			authMode:       utils.ModeIntegratedOAuth,
			expectSubjects: 2,
			expectAuthSA:   true,
		},
		{
			name: "RoleBinding exists - adds OIDC proxy SA",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: ptr.To(true),
				},
			},
			existingRB: &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Subjects: []rbacv1.Subject{
					{
						Kind:      rbacv1.ServiceAccountKind,
						Name:      "test-cluster",
						Namespace: "default",
					},
				},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "Role",
					Name:     "test-cluster",
				},
			},
			authMode:       utils.ModeOIDC,
			expectSubjects: 2,
			expectAuthSA:   true,
		},
		{
			name: "Auth SA already present - idempotent",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: ptr.To(true),
				},
			},
			existingRB: func() *rbacv1.RoleBinding {
				cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"}}
				namer := utils.NewResourceNamer(cluster)
				return &rbacv1.RoleBinding{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: "default",
					},
					Subjects: []rbacv1.Subject{
						{
							Kind:      rbacv1.ServiceAccountKind,
							Name:      "test-cluster",
							Namespace: "default",
						},
						{
							Kind:      rbacv1.ServiceAccountKind,
							Name:      namer.ServiceAccountName(utils.ModeIntegratedOAuth),
							Namespace: "default",
						},
					},
					RoleRef: rbacv1.RoleRef{
						APIGroup: rbacv1.GroupName,
						Kind:     "Role",
						Name:     "test-cluster",
					},
				}
			}(),
			authMode:       utils.ModeIntegratedOAuth,
			expectSubjects: 2,
			expectAuthSA:   true,
			expectNoUpdate: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := setupScheme()

			objects := []runtime.Object{tc.cluster}
			if tc.existingRB != nil {
				objects = append(objects, tc.existingRB)
			}

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(s).
				WithRuntimeObjects(objects...).
				Build()

			controller := &AuthenticationController{
				Client:   fakeClient,
				Scheme:   s,
				Recorder: record.NewFakeRecorder(10),
			}

			pending, err := controller.ensureAutoscalerRoleBindingForAuthSA(ctx, tc.cluster, tc.authMode, ctrl.Log)
			require.NoError(t, err)
			assert.Equal(t, tc.expectPending, pending)

			if tc.existingRB == nil {
				rb := &rbacv1.RoleBinding{}
				err = fakeClient.Get(ctx, types.NamespacedName{
					Name:      tc.cluster.Name,
					Namespace: tc.cluster.Namespace,
				}, rb)
				assert.True(t, errors.IsNotFound(err), "RoleBinding should not be created")
				return
			}

			rb := &rbacv1.RoleBinding{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      tc.cluster.Name,
				Namespace: tc.cluster.Namespace,
			}, rb)
			require.NoError(t, err)
			assert.Len(t, rb.Subjects, tc.expectSubjects)

			if tc.expectNoUpdate {
				assert.Equal(t, tc.existingRB.Subjects, rb.Subjects, "Subjects should remain unchanged")
				return
			}

			if tc.expectAuthSA {
				namer := utils.NewResourceNamer(tc.cluster)
				expectedSA := namer.ServiceAccountName(tc.authMode)
				found := false
				for _, s := range rb.Subjects {
					if s.Kind == rbacv1.ServiceAccountKind &&
						s.Name == expectedSA &&
						s.Namespace == tc.cluster.Namespace {
						found = true
						break
					}
				}
				assert.True(t, found, "Auth SA %s should be in RoleBinding subjects", expectedSA)
			}
		})
	}
}
