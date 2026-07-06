package v1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// mockOpenShiftRESTMapperUnit is a mock RESTMapper for unit testing
type mockOpenShiftRESTMapperUnit struct {
	meta.RESTMapper
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
			userSetValue:   ptr.To(false),
			expectedValue:  false,
			expectedLogMsg: "already false",
			isOpenShift:    true,
		},
		{
			name:           "OpenShift: user sets true - webhook OVERRIDES to false",
			userSetValue:   ptr.To(true),
			expectedValue:  false,
			expectedLogMsg: "overriding",
			isOpenShift:    true,
		},
		{
			name:           "Kubernetes: user sets true - webhook respects",
			userSetValue:   ptr.To(true),
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
					EnableIngress: ptr.To(true), // User wants Routes
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
					EnableIngress: ptr.To(true),
				},
			},
		}

		// Call webhook multiple times
		for i := 0; i < 3; i++ {
			err := defaulter.Default(ctx, rayCluster)
			require.NoError(t, err)
			assert.False(t, *rayCluster.Spec.HeadGroupSpec.EnableIngress,
				"EnableIngress should always be false after webhook")
		}
	})
}
