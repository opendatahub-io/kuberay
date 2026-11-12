package support

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
)

// NewRayClusterSpecOpenShiftE2E matches Konflux/Jenkins build-image head/worker resources on RHOAI.
func NewRayClusterSpecOpenShiftE2E(options ...SupportOption[rayv1ac.RayClusterSpecApplyConfiguration]) *rayv1ac.RayClusterSpecApplyConfiguration {
	return NewRayClusterSpec(append(options, func(spec *rayv1ac.RayClusterSpecApplyConfiguration) *rayv1ac.RayClusterSpecApplyConfiguration {
		spec.HeadGroupSpec.Template = PodTemplateSpecApplyConfiguration(spec.HeadGroupSpec.Template, openShiftE2EHeadResources())
		if len(spec.WorkerGroupSpecs) > 0 {
			spec.WorkerGroupSpecs[0].Template = PodTemplateSpecApplyConfiguration(spec.WorkerGroupSpecs[0].Template, openShiftE2EWorkerResources())
		}
		return spec
	})...)
}

func openShiftE2EHeadResources() SupportOption[corev1ac.PodTemplateSpecApplyConfiguration] {
	return func(pt *corev1ac.PodTemplateSpecApplyConfiguration) *corev1ac.PodTemplateSpecApplyConfiguration {
		for i := range pt.Spec.Containers {
			if pt.Spec.Containers[i].Name != nil && *pt.Spec.Containers[i].Name == common.RayHeadContainer {
				updated := pt.Spec.Containers[i].
					WithResources(corev1ac.ResourceRequirements().
						WithRequests(corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("6G"),
						}).
						WithLimits(corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2000m"),
							corev1.ResourceMemory: resource.MustParse("10G"),
						}))
				pt.Spec.Containers[i] = *updated
				break
			}
		}
		return pt
	}
}

func openShiftE2EWorkerResources() SupportOption[corev1ac.PodTemplateSpecApplyConfiguration] {
	return func(pt *corev1ac.PodTemplateSpecApplyConfiguration) *corev1ac.PodTemplateSpecApplyConfiguration {
		for i := range pt.Spec.Containers {
			if pt.Spec.Containers[i].Name != nil && *pt.Spec.Containers[i].Name == "ray-worker" {
				updated := pt.Spec.Containers[i].
					WithResources(corev1ac.ResourceRequirements().
						WithRequests(corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("1G"),
						}).
						WithLimits(corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1000m"),
							corev1.ResourceMemory: resource.MustParse("3G"),
						}))
				pt.Spec.Containers[i] = *updated
				break
			}
		}
		return pt
	}
}

var establishedHTTPStatusCode = regexp.MustCompile(`^[1-5]\d\d$`)

// AssertOpenShiftManagedRayClusterSpec checks post-admission / post-migration native fields.
// Platform ingress rules come from the deployed webhook (operator env); do not re-derive them
// in the test process because APPLICATION_NAMESPACE differs from openshift.go fallbacks.
func AssertOpenShiftManagedRayClusterSpec(g Gomega, cluster *rayv1.RayCluster) {
	g.Expect(utils.IsLegacySecurityEnabled(cluster)).To(BeTrue(),
		"expected %q annotation to be enabled", utils.EnableSecureTrustedNetworkAnnotationKey)

	g.Expect(cluster.Spec.NetworkPolicy).NotTo(BeNil())
	g.Expect(cluster.Spec.NetworkPolicy.Mode).NotTo(BeNil())
	g.Expect(*cluster.Spec.NetworkPolicy.Mode).To(Equal(rayv1.NetworkPolicyDenyAllIngress),
		"OpenShift managed profile must use DenyAllIngress")

	g.Expect(cluster.Spec.NetworkPolicy.Head).NotTo(BeNil())
	AssertOpenShiftPlatformHeadIngressRules(g, cluster.Spec.NetworkPolicy.Head.IngressRules)

	g.Expect(cluster.Spec.TLSOptions).NotTo(BeNil())
	g.Expect(cluster.Spec.TLSOptions.Enabled).NotTo(BeNil())
	g.Expect(*cluster.Spec.TLSOptions.Enabled).To(BeTrue())

	g.Expect(cluster.Spec.HeadGroupSpec.EnableIngress).NotTo(BeNil())
	g.Expect(*cluster.Spec.HeadGroupSpec.EnableIngress).To(BeFalse(),
		"OpenShift mutating webhook must force enableIngress=false")
}

// AssertOpenShiftPlatformHeadIngressRules checks the OpenShift head rule contract without
// comparing operator namespace lists to test-local EnsureOpenShift defaults.
func AssertOpenShiftPlatformHeadIngressRules(g Gomega, rules []networkingv1.NetworkPolicyIngressRule) {
	g.Expect(rules).NotTo(BeEmpty())
	g.Expect(headNPIngressHasNamespaceDashboardAndClientPorts(rules, utils.DefaultDashboardPort, utils.DefaultClientPort)).To(BeTrue(),
		"expected same-namespace dashboard (%d) and client (%d) ingress", utils.DefaultDashboardPort, utils.DefaultClientPort)
	g.Expect(headNPIngressHasRayJobDashboardRule(rules, utils.DefaultDashboardPort)).To(BeTrue(),
		"expected RayJob submitter dashboard ingress")
	g.Expect(headNPIngressHasPortOnlyRule(rules, 8443)).To(BeTrue(),
		"expected kube-rbac-proxy / mTLS port 8443 ingress rule")
	g.Expect(headNPIngressHasOperatorPeer(rules)).To(BeTrue(),
		"expected operator namespace/pod selector ingress rule")
	g.Expect(headNPIngressHasGatewayPeer(rules, utils.GetGatewayNamespace(), utils.GetGatewayName())).To(BeTrue(),
		"expected Gateway dataplane ingress rule")
}

func headNPIngressHasRayJobDashboardRule(rules []networkingv1.NetworkPolicyIngressRule, dashboardPort int32) bool {
	for _, rule := range rules {
		for _, from := range rule.From {
			if from.PodSelector == nil {
				continue
			}
			if from.PodSelector.MatchLabels[utils.RayOriginatedFromCRDLabelKey] != utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD) {
				continue
			}
			if portSetIncludes(rule.Ports, dashboardPort) {
				return true
			}
		}
	}
	return false
}

// AssertOpenShiftHeadNetworkPolicy checks the reconciled head NetworkPolicy object.
func AssertOpenShiftHeadNetworkPolicy(g Gomega, headNP *networkingv1.NetworkPolicy, cluster *rayv1.RayCluster) {
	g.Expect(headNP.Spec.PolicyTypes).To(Equal([]networkingv1.PolicyType{networkingv1.PolicyTypeIngress}),
		"DenyAllIngress must restrict ingress only (no egress policy type)")
	g.Expect(headNP.Spec.PolicyTypes).NotTo(ContainElement(networkingv1.PolicyTypeEgress))

	intraCluster := networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{utils.RayClusterLabelKey: cluster.Name},
			},
		}},
	}
	g.Expect(headNP.Spec.Ingress).To(ContainElement(intraCluster))

	platformRules := cluster.Spec.NetworkPolicy.Head.IngressRules
	for _, want := range platformRules {
		g.Expect(headNP.Spec.Ingress).To(ContainElement(want),
			"reconciled head NetworkPolicy must include platform rule %#v", want)
	}

	AssertOpenShiftPlatformHeadIngressRules(g, headNP.Spec.Ingress)
}

func headNPIngressHasNamespaceDashboardAndClientPorts(rules []networkingv1.NetworkPolicyIngressRule, dashboardPort, clientPort int32) bool {
	for _, rule := range rules {
		if len(rule.From) != 1 || rule.From[0].PodSelector == nil {
			continue
		}
		if len(rule.From[0].PodSelector.MatchLabels) != 0 {
			continue
		}
		if portSetIncludes(rule.Ports, dashboardPort) && portSetIncludes(rule.Ports, clientPort) {
			return true
		}
	}
	return false
}

func headNPIngressHasPortOnlyRule(rules []networkingv1.NetworkPolicyIngressRule, port int32) bool {
	for _, rule := range rules {
		if len(rule.From) != 0 {
			continue
		}
		if len(rule.Ports) == 1 && portEquals(rule.Ports[0], port) {
			return true
		}
	}
	return false
}

func headNPIngressHasOperatorPeer(rules []networkingv1.NetworkPolicyIngressRule) bool {
	for _, rule := range rules {
		for _, from := range rule.From {
			if from.PodSelector == nil || from.NamespaceSelector == nil {
				continue
			}
			if from.PodSelector.MatchLabels[utils.KubernetesApplicationNameLabelKey] != utils.ApplicationName {
				continue
			}
			for _, expr := range from.NamespaceSelector.MatchExpressions {
				if expr.Key == corev1.LabelMetadataName && expr.Operator == metav1.LabelSelectorOpIn && len(expr.Values) > 0 {
					return true
				}
			}
		}
	}
	return false
}

func headNPIngressHasGatewayPeer(rules []networkingv1.NetworkPolicyIngressRule, gatewayNamespace, gatewayName string) bool {
	for _, rule := range rules {
		for _, from := range rule.From {
			if from.PodSelector == nil || from.NamespaceSelector == nil {
				continue
			}
			if from.PodSelector.MatchLabels["gateway.networking.k8s.io/gateway-name"] != gatewayName {
				continue
			}
			for _, expr := range from.NamespaceSelector.MatchExpressions {
				if expr.Key == corev1.LabelMetadataName && expr.Operator == metav1.LabelSelectorOpIn &&
					sliceContains(expr.Values, gatewayNamespace) {
					return true
				}
			}
		}
	}
	return false
}

func sliceContains(values []string, target string) bool {
	return slices.Contains(values, target)
}

func portSetIncludes(ports []networkingv1.NetworkPolicyPort, want int32) bool {
	for _, p := range ports {
		if portEquals(p, want) {
			return true
		}
	}
	return false
}

func portEquals(port networkingv1.NetworkPolicyPort, want int32) bool {
	return port.Port != nil && port.Port.Type == intstr.Int && port.Port.IntVal == want
}

// EventuallySameNamespaceDashboardHTTP polls until curl gets a real HTTP status from the dashboard URL.
func EventuallySameNamespaceDashboardHTTP(g Gomega, test Test, curlPod *corev1.Pod, url string) {
	g.Eventually(func(g Gomega) {
		code, _ := CurlHTTPStatus(test, curlPod, "curl", url)
		g.Expect(IsEstablishedHTTPResponse(code)).To(BeTrue(),
			"same-namespace dashboard should return an HTTP status (got %q)", code)
	}, TestTimeoutLong, 5*time.Second).Should(Succeed())
}

// WaitForServiceReadyEndpoints blocks until the service has at least one ready endpoint slice target.
func WaitForServiceReadyEndpoints(g Gomega, test Test, namespace, serviceName string) {
	g.Eventually(func(g Gomega) {
		endpoints, err := GetReadyEndpointsFromSlices(test.Ctx(), test.Client(), namespace, serviceName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(endpoints).NotTo(BeEmpty(), "service %s/%s should have ready endpoints", namespace, serviceName)
	}, TestTimeoutMedium, 5*time.Second).Should(Succeed())
}

// CurlHTTPStatus runs curl -w %{http_code} inside a pod.
func CurlHTTPStatus(test Test, pod *corev1.Pod, containerName, url string) (string, error) {
	stdout, _, err := ExecPodCmdWithError(test, pod, containerName, []string{
		"curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
		"--connect-timeout", "5", "--max-time", "15", url,
	})
	return strings.TrimSpace(stdout.String()), err
}

// IsEstablishedHTTPResponse reports whether curl obtained a normal HTTP status line (not 000).
func IsEstablishedHTTPResponse(status string) bool {
	return establishedHTTPStatusCode.MatchString(status)
}

// HeadDashboardURL returns the cluster DNS name for the Ray dashboard service.
func HeadDashboardURL(clusterName, namespace string) string {
	headSvc := fmt.Sprintf("%s-head-svc", clusterName)
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/", headSvc, namespace, utils.DefaultDashboardPort)
}
