package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const defaultE2EWorkerGroupName = "small-group"

func legacySecureNetworkAnnotations() map[string]string {
	return map[string]string{
		utils.EnableSecureTrustedNetworkAnnotationKey: "true",
	}
}

// TestRayClusterOpenShiftNetworkPolicy exercises the RHOAI OpenShift-managed NetworkPolicy
// contract on a live cluster: admission validation (create-time smoke only; update/migration
// matrix is covered in raycluster_validating_webhook_unit_test.go and
// security_migration_integration_test.go), mutating webhook defaults, LegacySecurityController
// backfill, reconciled NetworkPolicy objects, and same-/cross-namespace dashboard probes.
func TestRayClusterOpenShiftNetworkPolicy(t *testing.T) {
	RequireOpenShift(t, With(t))

	t.Run("validationRejectsUnsupportedPolicyModes", func(t *testing.T) {
		test := With(t)
		namespace := test.NewTestNamespace()

		for _, tc := range []struct {
			name string
			mode rayv1.NetworkPolicyMode
		}{
			{name: "DenyAll", mode: rayv1.NetworkPolicyDenyAll},
			{name: "DenyAllEgress", mode: rayv1.NetworkPolicyDenyAllEgress},
		} {
			t.Run(tc.name, func(t *testing.T) {
				g := NewWithT(t)
				clusterName := fmt.Sprintf("invalid-np-%s", strings.ToLower(string(tc.mode)))

				rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
					WithAnnotations(legacySecureNetworkAnnotations()).
					WithSpec(NewRayClusterSpec().
						WithNetworkPolicy(rayv1ac.NetworkPolicyConfig().WithMode(tc.mode)))

				_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
				g.Expect(err).To(HaveOccurred())
				g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid for mode %q: %v", tc.mode, err)
			})
		}
	})

	t.Run("validationRejectsUnauthorizedHeadIngress", func(t *testing.T) {
		test := With(t)
		g := NewWithT(t)
		namespace := test.NewTestNamespace()

		tcp := corev1.ProtocolTCP
		customPort := intstr.FromInt32(9999)
		rayClusterAC := rayv1ac.RayCluster("invalid-custom-ingress", namespace.Name).
			WithAnnotations(legacySecureNetworkAnnotations()).
			WithSpec(NewRayClusterSpec().
				WithNetworkPolicy(rayv1ac.NetworkPolicyConfig().
					WithMode(rayv1.NetworkPolicyDenyAllIngress).
					WithHead(rayv1ac.NetworkPolicyRules().WithIngressRules(networkingv1.NetworkPolicyIngressRule{
						Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &customPort}},
					}))))

		_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid for custom head ingress rule: %v", err)
	})

	t.Run("mutatingWebhookAppliesOpenShiftDefaults", func(t *testing.T) {
		test := With(t)
		g := NewWithT(t)
		namespace := test.NewTestNamespace()
		clusterName := "webhook-defaults-cluster"

		// No legacy annotation on input — the mutating webhook must inject OpenShift defaults.
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecOpenShiftE2E(func(spec *rayv1ac.RayClusterSpecApplyConfiguration) *rayv1ac.RayClusterSpecApplyConfiguration {
				spec.HeadGroupSpec = spec.HeadGroupSpec.WithEnableIngress(true)
				return spec
			}))

		_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(func(gg Gomega) {
			cluster, err := GetRayCluster(test, namespace.Name, clusterName)
			gg.Expect(err).NotTo(HaveOccurred())
			AssertOpenShiftManagedRayClusterSpec(gg, cluster)
		}, TestTimeoutMedium).Should(Succeed())
	})

	t.Run("legacySecurityControllerRestoresNativeFieldsAfterPatch", func(t *testing.T) {
		test := With(t)
		g := NewWithT(t)
		namespace := test.NewTestNamespace()
		clusterName := "legacy-migration-cluster"

		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithAnnotations(legacySecureNetworkAnnotations()).
			WithSpec(NewRayClusterSpecOpenShiftE2E())

		_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(func(gg Gomega) {
			cluster, err := GetRayCluster(test, namespace.Name, clusterName)
			gg.Expect(err).NotTo(HaveOccurred())
			AssertOpenShiftManagedRayClusterSpec(gg, cluster)
		}, TestTimeoutMedium).Should(Succeed())

		// Simulate a pre-migration cluster object: legacy annotation without native fields.
		patch := []byte(`{"spec":{"networkPolicy":null,"tlsOptions":null}}`)
		_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Patch(
			test.Ctx(), clusterName, types.MergePatchType, patch, metav1.PatchOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(func(gg Gomega) {
			cluster, err := GetRayCluster(test, namespace.Name, clusterName)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(utils.IsLegacySecurityEnabled(cluster)).To(BeTrue())
			AssertOpenShiftManagedRayClusterSpec(gg, cluster)
		}, TestTimeoutMedium).Should(Succeed())
	})

	t.Run("reconcilesExpectedNetworkPolicies", func(t *testing.T) {
		test := With(t)
		g := NewWithT(t)
		namespace := test.NewTestNamespace()
		clusterName := "np-reconcile-cluster"

		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecOpenShiftE2E())

		_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		headPolicyName := clusterName + "-head"
		workerPolicyName := fmt.Sprintf("%s-workers-%s", clusterName, defaultE2EWorkerGroupName)

		g.Eventually(func(gg Gomega) {
			cluster, err := GetRayCluster(test, namespace.Name, clusterName)
			gg.Expect(err).NotTo(HaveOccurred())
			AssertOpenShiftManagedRayClusterSpec(gg, cluster)
			gg.Expect(cluster.UID).NotTo(BeEmpty())

			headNP, err := test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
				Get(test.Ctx(), headPolicyName, metav1.GetOptions{})
			gg.Expect(err).NotTo(HaveOccurred())
			AssertOpenShiftHeadNetworkPolicy(gg, headNP, cluster)
			gg.Expect(metav1.IsControlledBy(headNP, cluster)).To(BeTrue())

			workerNP, err := test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
				Get(test.Ctx(), workerPolicyName, metav1.GetOptions{})
			gg.Expect(err).NotTo(HaveOccurred())

			workerLabels := workerNP.Spec.PodSelector.MatchLabels
			gg.Expect(workerLabels).To(HaveKeyWithValue(utils.RayClusterLabelKey, clusterName))
			gg.Expect(workerLabels).To(HaveKeyWithValue(utils.RayNodeTypeLabelKey, string(rayv1.WorkerNode)))
			gg.Expect(workerLabels).To(HaveKeyWithValue(utils.RayNodeGroupLabelKey, defaultE2EWorkerGroupName))
			gg.Expect(workerNP.Spec.PolicyTypes).To(Equal([]networkingv1.PolicyType{networkingv1.PolicyTypeIngress}))
			gg.Expect(workerNP.Spec.PolicyTypes).NotTo(ContainElement(networkingv1.PolicyTypeEgress))
			gg.Expect(metav1.IsControlledBy(workerNP, cluster)).To(BeTrue())
		}, TestTimeoutMedium).Should(Succeed())
	})

	t.Run("dashboardIngressAllowedInNamespaceDeniedAcrossNamespaces", func(t *testing.T) {
		test := With(t)
		g := NewWithT(t)
		workloadNS := test.NewTestNamespace()
		probeNS := test.NewTestNamespace()
		clusterName := "np-ingress-probe"

		rayClusterAC := rayv1ac.RayCluster(clusterName, workloadNS.Name).
			WithSpec(NewRayClusterSpecOpenShiftE2E())

		_, err := test.Client().Ray().RayV1().RayClusters(workloadNS.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		headSvc := fmt.Sprintf("%s-head-svc", clusterName)
		WaitForServiceReadyEndpoints(g, test, workloadNS.Name, headSvc)

		targetURL := HeadDashboardURL(clusterName, workloadNS.Name)

		sameNSCurl, err := CreateCurlPod(g, test, "np-probe-same-ns", "curl", workloadNS.Name)
		g.Expect(err).NotTo(HaveOccurred())

		EventuallySameNamespaceDashboardHTTP(g, test, sameNSCurl, targetURL)

		crossNSCurl, err := CreateCurlPod(g, test, "np-probe-cross-ns", "curl", probeNS.Name)
		g.Expect(err).NotTo(HaveOccurred())

		g.Consistently(func(gg Gomega) {
			code, _ := CurlHTTPStatus(test, crossNSCurl, "curl", targetURL)
			gg.Expect(IsEstablishedHTTPResponse(code)).To(BeFalse(),
				"cross-namespace dashboard should not return a normal HTTP response (got %q)", code)
		}, 30*time.Second, 5*time.Second).Should(Succeed())
	})
}
