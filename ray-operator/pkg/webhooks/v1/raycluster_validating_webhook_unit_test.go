package v1

import (
	"testing"

	routev1 "github.com/openshift/api/route/v1"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

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

func TestValidateManagedOpenShiftRayClusterNetworkPolicyRejectsStalePortRules(t *testing.T) {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "managed-cluster",
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{RayStartParams: map[string]string{"dashboard-port": "9365"}},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	require.NoError(t, managedOpenShiftWebhook().validateRayCluster(cluster))

	cluster.Spec.HeadGroupSpec.RayStartParams["dashboard-port"] = "9465"
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
