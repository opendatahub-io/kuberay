package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestOpenShiftDefaultsDoNotAllowMonitoringNamespace(t *testing.T) {
	for _, namespace := range []string{"", "openshift-user-workload-monitoring", "custom-monitoring"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			t.Setenv("MONITORING_NAMESPACE", namespace)
			cluster := &rayv1.RayCluster{Spec: rayv1.RayClusterSpec{HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"dashboard-port": "8080", "metrics-export-port": "9090"},
			}}}
			EnsureOpenShiftRayClusterSecurity(cluster)
			want := cluster.Spec.NetworkPolicy.DeepCopy()
			EnsureOpenShiftRayClusterSecurity(cluster)
			assert.Equal(t, want, cluster.Spec.NetworkPolicy, "defaulting must be idempotent")

			for _, rule := range cluster.Spec.NetworkPolicy.Head.IngressRules {
				for _, peer := range rule.From {
					if peer.NamespaceSelector == nil {
						continue
					}
					selector, err := metav1.LabelSelectorAsSelector(peer.NamespaceSelector)
					require.NoError(t, err)
					assert.False(t, selector.Matches(labels.Set{corev1.LabelMetadataName: namespace}), "defaults must not grant the configured monitoring namespace access on any port")
				}
			}
		})
	}
}

func TestOpenShiftMonitoringPreservesExplicitPolicy(t *testing.T) {
	t.Setenv("MONITORING_NAMESPACE", "openshift-user-workload-monitoring")
	mode := rayv1.NetworkPolicyDenyAll
	cluster := &rayv1.RayCluster{Spec: rayv1.RayClusterSpec{NetworkPolicy: &rayv1.NetworkPolicyConfig{Mode: &mode}}}
	want := cluster.Spec.NetworkPolicy.DeepCopy()
	EnsureOpenShiftRayClusterSecurity(cluster)
	assert.Equal(t, want, cluster.Spec.NetworkPolicy, "explicit policies must not acquire platform default rules")
}

func TestOpenShiftNetworkPolicyUsesEffectiveDashboardPort(t *testing.T) {
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{
					"dashboard-port": "9365",
				},
			},
		},
	}

	EnsureOpenShiftRayClusterSecurity(cluster)
	rules := cluster.Spec.NetworkPolicy.Head.IngressRules

	assert.Contains(t, rulePorts(rules[0]), int32(9365), "dashboard access should use the configured dashboard port")
	assert.Contains(t, rulePorts(rules[0]), int32(DefaultClientPort), "client access should retain the client port")
	assert.Contains(t, rulePorts(rules[1]), int32(9365), "RayJob submission should use the configured dashboard port")
}

func TestOpenShiftNetworkPolicyInvalidRayPortsUseDefaults(t *testing.T) {
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{
					"dashboard-port":      "not-a-port",
					"metrics-export-port": "70000",
				},
			},
		},
	}

	EnsureOpenShiftRayClusterSecurity(cluster)
	rules := cluster.Spec.NetworkPolicy.Head.IngressRules

	assert.Contains(t, rulePorts(rules[0]), int32(DefaultDashboardPort))
	assert.Contains(t, rulePorts(rules[1]), int32(DefaultDashboardPort))
}

func TestOpenShiftGatewayIngressUsesGatewayPodSelector(t *testing.T) {
	t.Setenv("GATEWAY_NAMESPACE", "openshift-ingress")
	t.Setenv("GATEWAY_NAME", "data-science-gateway")
	cluster := &rayv1.RayCluster{}
	EnsureOpenShiftRayClusterSecurity(cluster)

	found := false
	for _, rule := range cluster.Spec.NetworkPolicy.Head.IngressRules {
		for _, port := range rule.Ports {
			if port.Port == nil || *port.Port != intstr.FromInt32(DefaultDashboardPort) || len(rule.From) != 1 {
				continue
			}
			peer := rule.From[0]
			if peer.NamespaceSelector == nil || peer.PodSelector == nil {
				continue
			}
			if peer.PodSelector.MatchLabels["gateway.networking.k8s.io/gateway-name"] == "data-science-gateway" {
				found = true
			}
		}
	}
	assert.True(t, found, "dashboard ingress must be restricted to the named Gateway dataplane")
}

func rulePorts(rule networkingv1.NetworkPolicyIngressRule) []int32 {
	ports := make([]int32, 0, len(rule.Ports))
	for _, port := range rule.Ports {
		if port.Port != nil && port.Port.Type == intstr.Int {
			ports = append(ports, port.Port.IntVal)
		}
	}
	return ports
}

func TestLegacySecurityAnnotationParser(t *testing.T) {
	assert.False(t, IsLegacySecurityEnabled(nil))
	for _, value := range []string{"", "false", "0", "no", "off", " true ", "enabled"} {
		t.Run(value, func(t *testing.T) {
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{EnableSecureTrustedNetworkAnnotationKey: value}}}
			assert.False(t, IsLegacySecurityEnabled(cluster))
		})
	}
}
