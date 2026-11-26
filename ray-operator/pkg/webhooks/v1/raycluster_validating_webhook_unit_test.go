package v1

import (
	"errors"
	"strconv"
	"testing"

	routev1 "github.com/openshift/api/route/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

type failingRESTMapper struct {
	meta.RESTMapper
	err error
}

func (m *failingRESTMapper) RESTMapping(_ schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
	return nil, m.err
}

func managedOpenShiftWebhook() *RayClusterWebhook {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{routev1.SchemeGroupVersion})
	mapper.Add(routev1.SchemeGroupVersion.WithKind("Route"), meta.RESTScopeNamespace)
	return &RayClusterWebhook{RESTMapper: mapper}
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicy(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "managed-cluster",
			Namespace: "user-ns",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)

	webhook := managedOpenShiftWebhook()
	require.NoError(t, webhook.validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftNetworkPolicyLegacyMonitoringRule(t *testing.T) {
	cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{
		Name:        "legacy-monitoring",
		Annotations: map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"},
	}}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	cluster.Spec.NetworkPolicy.Head.IngressRules = append(cluster.Spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{"openshift-user-workload-monitoring"},
		}}}}},
		Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(8080))}},
	})
	webhook := managedOpenShiftWebhook()
	require.NoError(t, webhook.validateRayClusterUpdate(cluster.DeepCopy(), cluster), "an existing broad rule must not block unrelated updates")
	require.Error(t, webhook.validateRayCluster(cluster), "new managed clusters must not add broad monitoring access")
	updated := cluster.DeepCopy()
	updated.Spec.NetworkPolicy.Head.IngressRules = updated.Spec.NetworkPolicy.Head.IngressRules[:len(updated.Spec.NetworkPolicy.Head.IngressRules)-1]
	require.NoError(t, webhook.validateRayClusterUpdate(updated, cluster), "the legacy rule may be removed from the spec")
}

func TestValidateManagedOpenShiftMetricsPortIsolation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		metrics      int32
		dashboard    int32
		declaredOnly bool
		allowed      bool
	}{
		{name: "default", metrics: 8080, allowed: true},
		{name: "custom", metrics: 9090, allowed: true},
		{name: "8443 compatibility rule", metrics: 8443},
		{name: "8443 without RayStartParams", metrics: 8443, declaredOnly: true},
		{name: "Ray client", metrics: 10001},
		{name: "dashboard", metrics: 8265},
		{name: "custom dashboard", metrics: 9365, dashboard: 9365},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{
				Name: "metrics-ports", Annotations: map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"},
			}}
			head := &cluster.Spec.HeadGroupSpec
			head.RayStartParams = map[string]string{}
			if !tc.declaredOnly {
				head.RayStartParams["metrics-export-port"] = strconv.Itoa(int(tc.metrics))
			}
			head.Template.Spec.Containers = []corev1.Container{{Name: "ray-head", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: tc.metrics}}}}
			if tc.dashboard != 0 {
				head.RayStartParams["dashboard-port"] = strconv.Itoa(int(tc.dashboard))
				head.Template.Spec.Containers[0].Ports = append(head.Template.Spec.Containers[0].Ports, corev1.ContainerPort{Name: utils.DashboardPortName, ContainerPort: tc.dashboard})
			}
			utils.EnsureOpenShiftRayClusterSecurity(cluster)
			err := managedOpenShiftWebhook().validateRayCluster(cluster)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "metrics port conflicts")
			}
		})
	}
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRejectsEgressIsolation(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{Mode: func() *rayv1.NetworkPolicyMode {
				mode := rayv1.NetworkPolicyDenyAllEgress
				return &mode
			}()},
		},
	}

	webhook := managedOpenShiftWebhook()
	require.Error(t, webhook.validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRequiresExplicitIngressMode(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{NetworkPolicy: &rayv1.NetworkPolicyConfig{}},
	}

	require.Error(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyAllowsLegacyBackfillWindow(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}

	require.NoError(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRejectsMissingBaselineRule(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	cluster.Spec.NetworkPolicy.Head.IngressRules = cluster.Spec.NetworkPolicy.Head.IngressRules[1:]

	webhook := managedOpenShiftWebhook()
	require.Error(t, webhook.validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRejectsCustomRule(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	port := intstr.FromInt(9999)
	protocol := corev1.ProtocolTCP
	cluster.Spec.NetworkPolicy.Head.IngressRules = append(
		cluster.Spec.NetworkPolicy.Head.IngressRules,
		networkingv1.NetworkPolicyIngressRule{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocol, Port: &port}}},
	)

	require.Error(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRejectsStalePortRules(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"dashboard-port": "9365"},
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Ports: []corev1.ContainerPort{{Name: utils.DashboardPortName, ContainerPort: 9365}},
				}}}},
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	require.NoError(t, managedOpenShiftWebhook().validateRayCluster(cluster))

	cluster.Spec.HeadGroupSpec.RayStartParams["dashboard-port"] = "9465"
	require.Error(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyAllowsPortUpdateForReconciliation(t *testing.T) {
	oldCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"dashboard-port": "9365"},
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Ports: []corev1.ContainerPort{{Name: utils.DashboardPortName, ContainerPort: 9365}},
				}}}},
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(oldCluster)
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.HeadGroupSpec.RayStartParams["dashboard-port"] = "9465"
	newCluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Ports[0].ContainerPort = 9465
	currentRules := newCluster.DeepCopy()
	currentRules.Spec.NetworkPolicy = nil
	utils.EnsureOpenShiftRayClusterNetworkPolicy(currentRules)
	// The old 9365 rule is intentionally omitted: it remains in the allowed
	// ceiling for this transition, but is not part of the new-spec floor.
	newCluster.Spec.NetworkPolicy.Head.IngressRules = currentRules.Spec.NetworkPolicy.Head.IngressRules

	require.NoError(t, managedOpenShiftWebhook().validateRayClusterUpdate(newCluster, oldCluster))
}

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRejectsRemovingCurrentBaselineRuleOnUpdate(t *testing.T) {
	oldCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(oldCluster)
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.NetworkPolicy.Head.IngressRules = newCluster.Spec.NetworkPolicy.Head.IngressRules[1:]

	require.Error(t, managedOpenShiftWebhook().validateRayClusterUpdate(newCluster, oldCluster))
}

func TestValidateManagedOpenShiftRayPortsRequiresMatchingContainerPort(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"dashboard-port": "9365"},
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "ray-head",
					Ports: []corev1.ContainerPort{{Name: utils.DashboardPortName, ContainerPort: 8265}},
				}}}},
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)

	require.Error(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayPortsAcceptsMatchingContainerPort(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"dashboard-port": "9365"},
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "ray-head",
					Ports: []corev1.ContainerPort{{Name: utils.DashboardPortName, ContainerPort: 9365}},
				}}}},
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)

	require.NoError(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateManagedOpenShiftRayPortsRejectsCustomPortWithoutHeadContainer(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			utils.EnableSecureTrustedNetworkAnnotationKey: "true",
		}},
		Spec: rayv1.RayClusterSpec{HeadGroupSpec: rayv1.HeadGroupSpec{
			RayStartParams: map[string]string{"dashboard-port": "9365"},
		}},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)

	require.Error(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}

func TestValidateUnmanagedRayClusterPreservesNativeNetworkPolicy(t *testing.T) {
	mode := rayv1.NetworkPolicyDenyAllEgress
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "native-cluster"},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{Mode: &mode},
		},
	}

	webhook := &RayClusterWebhook{}
	require.NoError(t, webhook.validateRayCluster(cluster))
}

func TestValidateLegacyAnnotationDoesNotEnableManagedProfileOffOpenShift(t *testing.T) {
	mode := rayv1.NetworkPolicyDenyAllEgress
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "native-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{Mode: &mode},
		},
	}

	// A nil RESTMapper represents a non-OpenShift/unit-test environment. The
	// legacy annotation is not a new admission opt-in for the managed profile.
	require.NoError(t, (&RayClusterWebhook{}).validateRayCluster(cluster))
}

func TestValidateOpenShiftDiscoveryErrorOnlyFailsClosedForManagedProfile(t *testing.T) {
	webhook := &RayClusterWebhook{RESTMapper: &failingRESTMapper{err: errors.New("discovery unavailable")}}
	cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged-cluster"}}

	require.NoError(t, webhook.validateRayCluster(cluster))

	cluster.Annotations = map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"}

	require.Error(t, webhook.validateRayCluster(cluster))
}

func TestValidateExternallyManagedOpenShiftRayClusterStillRequiresPlatformPolicy(t *testing.T) {
	mode := rayv1.NetworkPolicyDenyAllEgress
	controller := "example-controller"
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "external-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			ManagedBy:     &controller,
			NetworkPolicy: &rayv1.NetworkPolicyConfig{Mode: &mode},
		},
	}

	require.Error(t, managedOpenShiftWebhook().validateRayCluster(cluster))
}
