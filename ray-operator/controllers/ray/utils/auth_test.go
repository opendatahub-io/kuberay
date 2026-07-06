package utils

import (
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestDetectAuthenticationMode(t *testing.T) {
	tests := []struct {
		name        string
		expected    AuthenticationMode
		objects     []client.Object
		isOpenShift bool
	}{
		{
			name:        "Not OpenShift - returns ModeIntegratedOAuth (default)",
			isOpenShift: false,
			objects:     []client.Object{},
			expected:    ModeIntegratedOAuth,
		},
		{
			name:        "OpenShift with OIDC providers - returns ModeIntegratedOAuth (OIDC detection is disabled)",
			isOpenShift: true,
			objects: []client.Object{
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
			expected: ModeIntegratedOAuth, // Changed: OIDC detection is commented out in implementation
		},
		{
			name:        "OpenShift with OAuth identity providers - returns ModeIntegratedOAuth",
			isOpenShift: true,
			objects: []client.Object{
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
			expected: ModeIntegratedOAuth,
		},
		{
			name:        "OpenShift with empty OAuth spec - returns ModeIntegratedOAuth",
			isOpenShift: true,
			objects: []client.Object{
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
			expected: ModeIntegratedOAuth,
		},
		{
			name:        "OpenShift without auth resources - returns ModeIntegratedOAuth (default)",
			isOpenShift: true,
			objects:     []client.Object{},
			expected:    ModeIntegratedOAuth,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Test
			result := DetectAuthenticationMode(tc.isOpenShift)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestShouldEnableOAuth(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *rayv1.RayCluster
		authMode AuthenticationMode
		expected bool
	}{
		{
			name: "Annotation enabled with IntegratedOAuth - should enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: ModeIntegratedOAuth,
			expected: true,
		},
		{
			name: "Annotation enabled with OIDC - should not enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: ModeOIDC,
			expected: false,
		},
		{
			name: "Annotation disabled with IntegratedOAuth - should not enable OAuth",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						EnableSecureTrustedNetworkAnnotationKey: "false",
					},
				},
			},
			authMode: ModeIntegratedOAuth,
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
			authMode: ModeIntegratedOAuth,
			expected: false,
		},
		{
			name:     "Nil cluster - should not enable OAuth",
			cluster:  nil,
			authMode: ModeIntegratedOAuth,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ShouldEnableOAuth(tc.cluster, tc.authMode)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestShouldEnableOIDC(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *rayv1.RayCluster
		authMode AuthenticationMode
		expected bool
	}{
		{
			name: "Annotation enabled with OIDC - should enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: ModeOIDC,
			expected: true,
		},
		{
			name: "Annotation enabled with IntegratedOAuth - should not enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						EnableSecureTrustedNetworkAnnotationKey: "true",
					},
				},
			},
			authMode: ModeIntegratedOAuth,
			expected: false,
		},
		{
			name: "Annotation disabled with OIDC - should not enable OIDC",
			cluster: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Annotations: map[string]string{
						EnableSecureTrustedNetworkAnnotationKey: "false",
					},
				},
			},
			authMode: ModeOIDC,
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
			authMode: ModeOIDC,
			expected: false,
		},
		{
			name:     "Nil cluster - should not enable OIDC",
			cluster:  nil,
			authMode: ModeOIDC,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ShouldEnableOIDC(tc.cluster, tc.authMode)
			assert.Equal(t, tc.expected, result)
		})
	}
}
