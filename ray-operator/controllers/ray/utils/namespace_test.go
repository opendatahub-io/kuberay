package utils

import (
	"testing"

	routev1 "github.com/openshift/api/route/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

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

func TestGetPlatformNamespace_OpenShift_WithApplicationNamespace(t *testing.T) {
	// Test with APPLICATION_NAMESPACE set (highest priority on OpenShift)
	t.Setenv("APPLICATION_NAMESPACE", "custom-odh-namespace")
	t.Setenv("POD_NAMESPACE", "should-not-use-this")

	mapper := &mockRESTMapper{hasRouteAPI: true} // Simulate OpenShift
	namespace, isOpenShift := GetPlatformNamespace(mapper)

	assert.True(t, isOpenShift, "Should detect OpenShift")
	assert.Equal(t, "custom-odh-namespace", namespace, "Should use APPLICATION_NAMESPACE")
}

func TestGetPlatformNamespace_OpenShift_WithPodNamespace(t *testing.T) {
	// Test with only POD_NAMESPACE set (fallback on OpenShift)
	t.Setenv("POD_NAMESPACE", "custom-pod-namespace")

	mapper := &mockRESTMapper{hasRouteAPI: true}
	namespace, isOpenShift := GetPlatformNamespace(mapper)

	assert.True(t, isOpenShift, "Should detect OpenShift")
	assert.Equal(t, "custom-pod-namespace", namespace, "Should use POD_NAMESPACE as fallback")
}

func TestGetPlatformNamespace_OpenShift_FallbackToDefault(t *testing.T) {
	// Test with no env vars set (final fallback on OpenShift)
	// (t.Setenv not called, both remain unset)

	mapper := &mockRESTMapper{hasRouteAPI: true}
	namespace, isOpenShift := GetPlatformNamespace(mapper)

	assert.True(t, isOpenShift, "Should detect OpenShift")
	assert.Equal(t, "redhat-ods-applications", namespace, "Should use default OpenShift namespace")
}

func TestGetPlatformNamespace_NonOpenShift_WithPodNamespace(t *testing.T) {
	// Test non-OpenShift with POD_NAMESPACE set
	t.Setenv("POD_NAMESPACE", "custom-namespace")

	mapper := &mockRESTMapper{hasRouteAPI: false} // Simulate non-OpenShift
	namespace, isOpenShift := GetPlatformNamespace(mapper)

	assert.False(t, isOpenShift, "Should not detect OpenShift")
	assert.Equal(t, "custom-namespace", namespace, "Should use POD_NAMESPACE")
}

func TestGetPlatformNamespace_NonOpenShift_FallbackToDefault(t *testing.T) {
	// Test non-OpenShift with no env vars set
	// (t.Setenv not called, both remain unset)

	mapper := &mockRESTMapper{hasRouteAPI: false}
	namespace, isOpenShift := GetPlatformNamespace(mapper)

	assert.False(t, isOpenShift, "Should not detect OpenShift")
	assert.Equal(t, "ray-system", namespace, "Should use default non-OpenShift namespace")
}

func TestGetPlatformNamespace_NilRESTMapper(t *testing.T) {
	// Test with nil RESTMapper (should assume non-OpenShift)
	t.Setenv("POD_NAMESPACE", "test-namespace")

	namespace, isOpenShift := GetPlatformNamespace(nil)

	assert.False(t, isOpenShift, "Should not detect OpenShift with nil mapper")
	assert.Equal(t, "test-namespace", namespace, "Should use POD_NAMESPACE")
}

func TestGetGatewayNamespace_Default(t *testing.T) {
	// Test with no env var set
	// (t.Setenv not called, GATEWAY_NAMESPACE remains unset)

	namespace := GetGatewayNamespace()
	assert.Equal(t, "openshift-ingress", namespace, "Should return default gateway namespace")
}

func TestGetGatewayNamespace_CustomValue(t *testing.T) {
	// Test with custom env var
	t.Setenv("GATEWAY_NAMESPACE", "custom-gateway-namespace")

	namespace := GetGatewayNamespace()
	assert.Equal(t, "custom-gateway-namespace", namespace, "Should return custom gateway namespace")
}

func TestGetGatewayName_Default(t *testing.T) {
	// Test with no env var set
	// (t.Setenv not called, GATEWAY_NAME remains unset)

	name := GetGatewayName()
	assert.Equal(t, "data-science-gateway", name, "Should return default gateway name")
}

func TestGetGatewayName_CustomValue(t *testing.T) {
	// Test with custom env var
	t.Setenv("GATEWAY_NAME", "custom-gateway")

	name := GetGatewayName()
	assert.Equal(t, "custom-gateway", name, "Should return custom gateway name")
}

func TestIsOpenShiftCluster(t *testing.T) {
	tests := []struct {
		name        string
		hasRouteAPI bool
		expected    bool
	}{
		{
			name:        "OpenShift cluster with Route API",
			hasRouteAPI: true,
			expected:    true,
		},
		{
			name:        "Non-OpenShift cluster without Route API",
			hasRouteAPI: false,
			expected:    false,
		},
		{
			name:        "Nil RESTMapper",
			hasRouteAPI: false,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mapper meta.RESTMapper
			if tt.name != "Nil RESTMapper" {
				mapper = &mockRESTMapper{hasRouteAPI: tt.hasRouteAPI}
			}

			result := isOpenShiftCluster(mapper)
			assert.Equal(t, tt.expected, result, "OpenShift detection should match expected")
		})
	}
}

func TestEnvironmentVariablePriority(t *testing.T) {
	// Test that APPLICATION_NAMESPACE takes priority over POD_NAMESPACE on OpenShift
	t.Setenv("APPLICATION_NAMESPACE", "priority-namespace")
	t.Setenv("POD_NAMESPACE", "fallback-namespace")

	mapper := &mockRESTMapper{hasRouteAPI: true}
	namespace, _ := GetPlatformNamespace(mapper)

	assert.Equal(t, "priority-namespace", namespace,
		"APPLICATION_NAMESPACE should take priority over POD_NAMESPACE on OpenShift")
}

func TestOpenShiftVsNonOpenShiftBehavior(t *testing.T) {
	// Set both env vars
	t.Setenv("APPLICATION_NAMESPACE", "openshift-app-ns")
	t.Setenv("POD_NAMESPACE", "generic-pod-ns")

	// Test OpenShift behavior - uses APPLICATION_NAMESPACE
	openshiftMapper := &mockRESTMapper{hasRouteAPI: true}
	openshiftNs, openshiftDetected := GetPlatformNamespace(openshiftMapper)

	assert.True(t, openshiftDetected, "Should detect OpenShift")
	assert.Equal(t, "openshift-app-ns", openshiftNs, "OpenShift should use APPLICATION_NAMESPACE")

	// Test non-OpenShift behavior - uses POD_NAMESPACE (ignores APPLICATION_NAMESPACE)
	nonOpenshiftMapper := &mockRESTMapper{hasRouteAPI: false}
	nonOpenshiftNs, nonOpenshiftDetected := GetPlatformNamespace(nonOpenshiftMapper)

	assert.False(t, nonOpenshiftDetected, "Should not detect OpenShift")
	assert.Equal(t, "generic-pod-ns", nonOpenshiftNs, "Non-OpenShift should use POD_NAMESPACE")
}
