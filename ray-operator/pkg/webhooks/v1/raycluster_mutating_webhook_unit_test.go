package v1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// mockOpenShiftRESTMapperUnit is a mock RESTMapper for unit testing
type mockOpenShiftRESTMapperUnit struct {
	meta.RESTMapper
}

func TestWebhookDefaultsOpenShiftNativeSecurityFields(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	t.Setenv("APPLICATION_NAMESPACE", "opendatahub")
	t.Setenv("POD_NAMESPACE", "ray-system")
	t.Setenv("GATEWAY_NAMESPACE", "openshift-ingress")
	t.Setenv("MONITORING_NAMESPACE", "openshift-user-workload-monitoring")

	cluster := &rayv1.RayCluster{}
	defaulter := &RayClusterDefaulter{RESTMapper: &mockOpenShiftRESTMapperUnit{}}

	require.NoError(t, defaulter.Default(context.Background(), cluster))

	require.NotNil(t, cluster.Spec.TLSOptions)
	assert.True(t, *cluster.Spec.TLSOptions.Enabled)
	require.NotNil(t, cluster.Spec.NetworkPolicy)
	assert.Equal(t, rayv1.NetworkPolicyDenyAllIngress, *cluster.Spec.NetworkPolicy.Mode)
	require.NotNil(t, cluster.Spec.NetworkPolicy.Head)
	assert.Len(t, cluster.Spec.NetworkPolicy.Head.IngressRules, 6)
	tcp := corev1.ProtocolTCP
	dashboardPort := intstr.FromInt32(8265)
	assert.Contains(t, cluster.Spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			utils.RayOriginatedFromCRDLabelKey: utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
		}}}},
		Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &dashboardPort}},
	})
	assert.Equal(t, "true", cluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey])
}

// OPENSHIFT SPECIFIC
func TestWebhookOpenShiftNetworkPolicyPreservesExplicitConfig(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	mode := rayv1.NetworkPolicyDenyAllEgress
	customRule := networkingv1.NetworkPolicyIngressRule{}
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: &mode,
				Head: &rayv1.NetworkPolicyRules{
					IngressRules: []networkingv1.NetworkPolicyIngressRule{customRule},
				},
			},
		},
	}
	defaulter := &RayClusterDefaulter{RESTMapper: &mockOpenShiftRESTMapperUnit{}}

	require.NoError(t, defaulter.Default(context.Background(), cluster))
	require.NoError(t, defaulter.Default(context.Background(), cluster))

	assert.Equal(t, mode, *cluster.Spec.NetworkPolicy.Mode)
	assert.Equal(t, []networkingv1.NetworkPolicyIngressRule{customRule}, cluster.Spec.NetworkPolicy.Head.IngressRules)
}

func TestWebhookOpenShiftPreservesExplicitTLSAndNetworkPolicy(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	mode := rayv1.NetworkPolicyDenyAllEgress
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			TLSOptions: &rayv1.TLSOptions{Enabled: new(false)},
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: &mode,
				Head: &rayv1.NetworkPolicyRules{
					IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
				},
			},
		},
	}
	wantNetworkPolicy := cluster.Spec.NetworkPolicy.DeepCopy()

	defaulter := &RayClusterDefaulter{RESTMapper: &mockOpenShiftRESTMapperUnit{}}
	require.NoError(t, defaulter.Default(context.Background(), cluster))

	require.NotNil(t, cluster.Spec.TLSOptions)
	assert.False(t, *cluster.Spec.TLSOptions.Enabled)
	assert.Equal(t, wantNetworkPolicy, cluster.Spec.NetworkPolicy)
}

func (m *mockOpenShiftRESTMapperUnit) RESTMapping(_ schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
	return &meta.RESTMapping{}, nil
}

// mockKubernetesRESTMapper is a mock RESTMapper that simulates non-OpenShift
type mockKubernetesRESTMapper struct {
	meta.RESTMapper
}

func (m *mockKubernetesRESTMapper) RESTMapping(_ schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
	return nil, &meta.NoKindMatchError{}
}

// TestWebhookEnforcesEnableIngressFalseOnOpenShift tests strict enforcement
func TestWebhookEnforcesEnableIngressFalseOnOpenShift(t *testing.T) {
	tests := []struct {
		userSetValue   *bool
		name           string
		expectedLogMsg string
		expectedValue  bool
		isOpenShift    bool
	}{
		{
			name:           "OpenShift: user sets nothing - webhook sets false",
			userSetValue:   nil,
			expectedValue:  false,
			expectedLogMsg: "setting to false",
			isOpenShift:    true,
		},
		{
			name:           "OpenShift: user sets false - webhook keeps false",
			userSetValue:   new(false),
			expectedValue:  false,
			expectedLogMsg: "already false",
			isOpenShift:    true,
		},
		{
			name:           "OpenShift: user sets true - webhook OVERRIDES to false",
			userSetValue:   new(true),
			expectedValue:  false,
			expectedLogMsg: "overriding",
			isOpenShift:    true,
		},
		{
			name:           "Kubernetes: user sets true - webhook respects",
			userSetValue:   new(true),
			expectedValue:  true,
			expectedLogMsg: "",
			isOpenShift:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			rayCluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						EnableIngress: tt.userSetValue,
					},
				},
			}

			var restMapper meta.RESTMapper
			if tt.isOpenShift {
				restMapper = &mockOpenShiftRESTMapperUnit{}
			} else {
				restMapper = &mockKubernetesRESTMapper{}
			}

			defaulter := &RayClusterDefaulter{
				RESTMapper: restMapper,
			}

			// Execute
			err := defaulter.Default(context.TODO(), rayCluster)
			require.NoError(t, err)

			// Verify
			if tt.isOpenShift {
				require.NotNil(t, rayCluster.Spec.HeadGroupSpec.EnableIngress,
					"EnableIngress should be set on OpenShift")
				assert.Equal(t, tt.expectedValue, *rayCluster.Spec.HeadGroupSpec.EnableIngress,
					"EnableIngress value should match expected")
			} else if tt.userSetValue != nil {
				// On Kubernetes, value should remain as user set it
				assert.Equal(t, tt.expectedValue, *rayCluster.Spec.HeadGroupSpec.EnableIngress)
			}
		})
	}
}

// TestWebhookStrictEnforcementOnOpenShift specifically tests the strict enforcement behavior
func TestWebhookStrictEnforcementOnOpenShift(t *testing.T) {
	ctx := context.Background()

	// Setup OpenShift-like environment
	defaulter := &RayClusterDefaulter{
		RESTMapper: &mockOpenShiftRESTMapperUnit{},
	}

	t.Run("User explicitly sets enableIngress: true - should be overridden", func(t *testing.T) {
		rayCluster := &rayv1.RayCluster{
			Spec: rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					EnableIngress: new(true), // User wants Routes
				},
			},
		}

		// Execute webhook
		err := defaulter.Default(ctx, rayCluster)
		require.NoError(t, err)

		// Verify strict enforcement
		require.NotNil(t, rayCluster.Spec.HeadGroupSpec.EnableIngress)
		assert.False(t, *rayCluster.Spec.HeadGroupSpec.EnableIngress,
			"Webhook should OVERRIDE user's true to false (strict enforcement)")
	})

	t.Run("Multiple webhook calls should be idempotent", func(t *testing.T) {
		rayCluster := &rayv1.RayCluster{
			Spec: rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					EnableIngress: new(true),
				},
			},
		}

		// Call webhook multiple times
		for range 3 {
			err := defaulter.Default(ctx, rayCluster)
			require.NoError(t, err)
			assert.False(t, *rayCluster.Spec.HeadGroupSpec.EnableIngress,
				"EnableIngress should always be false after webhook")
		}
	})
}
