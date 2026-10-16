// TODO: This file is a transitional shim for OpenShift-specific Route creation. Once Gateway API
// support is mature and the Route-based backward compatibility window closes, this file and all
// associated OpenShift-specific code paths should be removed. See the roadmap summary at:
// https://github.com/ray-project/kuberay/pull/4365#issuecomment-4143407845

package utils

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// openShiftAPIGroups lists the API groups used to detect an OpenShift cluster.
// We check for specific well-known groups rather than using a broad suffix match
// (e.g. strings.HasSuffix(".openshift.io")) to avoid false positives from custom
// CRDs that happen to use the openshift.io domain but don't indicate a real
// OpenShift installation. Any one of these groups being present is sufficient.
var openShiftAPIGroups = []string{
	"route.openshift.io",
	"security.openshift.io",
	"config.openshift.io",
}

// IsOpenShiftCluster detects if the cluster is OpenShift by checking for OpenShift-specific API groups.
// This function is called once at operator startup and the result is stored in reconciler options.
// Returns an error if cluster type cannot be determined, since downstream behavior (e.g. Route
// vs Ingress creation) depends on this check being accurate.
func IsOpenShiftCluster(config *rest.Config) (bool, error) {
	if config == nil {
		return false, fmt.Errorf("REST config is nil")
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return false, fmt.Errorf("failed to create discovery client: %w", err)
	}
	apiGroups, err := discoveryClient.ServerGroups()
	if err != nil {
		return false, fmt.Errorf("failed to retrieve server API groups: %w", err)
	}
	for _, group := range apiGroups.Groups {
		if slices.Contains(openShiftAPIGroups, group.Name) {
			return true, nil
		}
	}
	return false, nil
}

// EnsureOpenShiftRayClusterSecurity applies the OpenShift security contract to
// a RayCluster. It is idempotent and preserves explicit native fields.
func EnsureOpenShiftRayClusterSecurity(cluster *rayv1.RayCluster) {
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[EnableSecureTrustedNetworkAnnotationKey] = "true"
	EnsureOpenShiftRayClusterMTLS(cluster)
	EnsureOpenShiftRayClusterNetworkPolicy(cluster)
	if cluster.Spec.HeadGroupSpec.EnableIngress == nil {
		disabled := false
		cluster.Spec.HeadGroupSpec.EnableIngress = &disabled
	}
}

// EnsureOpenShiftRayClusterMTLS enables mTLS by default without overriding an
// explicit TLSOptions value.
func EnsureOpenShiftRayClusterMTLS(cluster *rayv1.RayCluster) {
	if cluster.Spec.TLSOptions == nil {
		enabled := true
		cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: &enabled}
	}
}

// EnsureOpenShiftRayClusterNetworkPolicy adds the default OpenShift policy
// without overriding an explicit NetworkPolicy value.
func EnsureOpenShiftRayClusterNetworkPolicy(cluster *rayv1.RayCluster) {
	if cluster.Spec.NetworkPolicy == nil {
		ensureOpenShiftNetworkPolicy(&cluster.Spec)
	}
}

func ensureOpenShiftNetworkPolicy(spec *rayv1.RayClusterSpec) {
	mode := rayv1.NetworkPolicyDenyAllIngress
	spec.NetworkPolicy = &rayv1.NetworkPolicyConfig{Mode: &mode, Head: &rayv1.NetworkPolicyRules{}}
	tcp := corev1.ProtocolTCP
	dashboardPort := effectiveHeadPort(spec, "dashboard-port", DefaultDashboardPort)
	metricsPort := effectiveHeadPort(spec, "metrics-export-port", DefaultMetricsPort)
	port := func(value int32) networkingv1.NetworkPolicyPort {
		p := intstr.FromInt32(value)
		return networkingv1.NetworkPolicyPort{Protocol: &tcp, Port: &p}
	}
	appendIngress := func(rules []networkingv1.NetworkPolicyIngressRule, rule networkingv1.NetworkPolicyIngressRule) []networkingv1.NetworkPolicyIngressRule {
		for _, existing := range rules {
			if reflect.DeepEqual(existing, rule) {
				return rules
			}
		}
		return append(rules, rule)
	}
	// TODO(ODH NetworkPolicy): The same-namespace 10001 allowance is retained
	// while the platform client-source contract is unresolved. Users may install
	// Jupyter workbenches in arbitrary namespaces, so this rule must not be
	// broadened to all namespaces. A future cross-namespace rule requires a
	// protected namespace label contract or explicit per-RayCluster source rules.
	spec.NetworkPolicy.Head.IngressRules = appendIngress(spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}},
		Ports: []networkingv1.NetworkPolicyPort{port(dashboardPort), port(DefaultClientPort)},
	})
	spec.NetworkPolicy.Head.IngressRules = appendIngress(spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				RayOriginatedFromCRDLabelKey: RayOriginatedFromCRDLabelValue(RayJobCRD),
			},
		}}},
		Ports: []networkingv1.NetworkPolicyPort{port(dashboardPort)},
	})
	spec.NetworkPolicy.Head.IngressRules = appendIngress(spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		// TODO(ODH NetworkPolicy): The supported caller for the kube-rbac-proxy
		// port is not yet established. The Gateway HTTPRoute currently targets
		// the dashboard service port, so do not guess a gateway peer here until
		// the 8443 data path is confirmed; changing this rule may break auth.
		Ports: []networkingv1.NetworkPolicyPort{port(8443)},
	})
	namespaces := []string{}
	appendNamespace := func(namespace string) {
		if namespace != "" && !slices.Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	appendNamespace(os.Getenv("APPLICATION_NAMESPACE"))
	appendNamespace(os.Getenv("POD_NAMESPACE"))
	if len(namespaces) == 0 {
		namespaces = []string{"redhat-ods-applications", "opendatahub"}
	}
	operatorPeer := networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{KubernetesApplicationNameLabelKey: ApplicationName}},
		NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: namespaces,
		}}},
	}
	spec.NetworkPolicy.Head.IngressRules = appendIngress(spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{operatorPeer}, Ports: []networkingv1.NetworkPolicyPort{port(8265), port(10001)},
	})
	// Preserve the 1.6.2 monitoring contract. An unset namespace must not
	// expose metrics to other namespaces.
	if monitoringNamespace := os.Getenv("MONITORING_NAMESPACE"); monitoringNamespace != "" {
		spec.NetworkPolicy.Head.IngressRules = appendIngress(spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{monitoringNamespace},
			}}}}},
			Ports: []networkingv1.NetworkPolicyPort{port(metricsPort)},
		})
	}
	// TODO(ODH NetworkPolicy): Confirm whether the supported monitoring
	// deployment scrapes worker Pods before adding worker-group metrics rules.
	gatewayNamespace := os.Getenv("GATEWAY_NAMESPACE")
	if gatewayNamespace == "" {
		gatewayNamespace = "openshift-ingress"
	}
	spec.NetworkPolicy.Head.IngressRules = appendIngress(spec.NetworkPolicy.Head.IngressRules, networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{gatewayNamespace},
		}}}}},
		Ports: []networkingv1.NetworkPolicyPort{port(8265)},
	})
}

// effectiveHeadPort returns a valid integer Ray start parameter or its default.
// RayCluster pod construction applies the same fallback behavior when it builds
// the Ray command, so NetworkPolicy ports stay aligned with the effective head
// configuration.
func effectiveHeadPort(spec *rayv1.RayClusterSpec, key string, defaultPort int32) int32 {
	if value, ok := spec.HeadGroupSpec.RayStartParams[key]; ok {
		if port, err := strconv.ParseInt(value, 10, 32); err == nil && port > 0 && port <= 65535 {
			return int32(port)
		}
	}
	return defaultPort
}

// ShouldUseIngressOnOpenShift determines if Ingress should be used instead of Route on OpenShift.
func ShouldUseIngressOnOpenShift() bool {
	return strings.ToLower(os.Getenv(USE_INGRESS_ON_OPENSHIFT)) == "true"
}
