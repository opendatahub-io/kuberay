package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestOpenShiftMonitoringIngress(t *testing.T) {
	for _, namespace := range []string{"", "openshift-user-workload-monitoring", "custom-monitoring"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			t.Setenv("MONITORING_NAMESPACE", namespace)
			cluster := &rayv1.RayCluster{}
			EnsureOpenShiftRayClusterSecurity(cluster)
			want := cluster.Spec.NetworkPolicy.DeepCopy()
			EnsureOpenShiftRayClusterSecurity(cluster)
			assert.Equal(t, want, cluster.Spec.NetworkPolicy, "defaulting must be idempotent")

			var monitoringRules []networkingv1.NetworkPolicyIngressRule
			for _, rule := range cluster.Spec.NetworkPolicy.Head.IngressRules {
				for _, port := range rule.Ports {
					if port.Port != nil && *port.Port == intstr.FromInt32(8080) {
						monitoringRules = append(monitoringRules, rule)
					}
				}
			}
			if namespace == "" {
				assert.Empty(t, monitoringRules, "unset monitoring namespace must not expose metrics")
				return
			}
			require.Len(t, monitoringRules, 1)
			tcp := corev1.ProtocolTCP
			metrics := intstr.FromInt32(8080)
			assert.Equal(t, networkingv1.NetworkPolicyIngressRule{
				From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{namespace},
					}},
				}}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &metrics}},
			}, monitoringRules[0])
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

func TestLegacySecurityAnnotationParser(t *testing.T) {
	assert.False(t, IsLegacySecurityEnabled(nil))
	for _, value := range []string{"", "false", "0", "no", "off", " true ", "enabled"} {
		t.Run(value, func(t *testing.T) {
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{EnableSecureTrustedNetworkAnnotationKey: value}}}
			assert.False(t, IsLegacySecurityEnabled(cluster))
		})
	}
}
