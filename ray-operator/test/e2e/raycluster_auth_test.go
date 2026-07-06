package e2e

import (
	"fmt"
	"testing"

	. "github.com/onsi/gomega"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	oidcProxySidecarName = "kube-rbac-proxy"
	authProxyPortNum     = 8443
)

func TestRayClusterAuthentication(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := rayv1ac.RayCluster("raycluster-auth", namespace.Name).
		WithAnnotations(map[string]string{
			utils.EnableSecureTrustedNetworkAnnotationKey: "true",
		}).
		WithSpec(newRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with authentication enabled", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod).NotTo(BeNil())

	test.T().Run("Auth proxy sidecar is injected into head pod", func(_ *testing.T) {
		var found bool
		for _, c := range headPod.Spec.Containers {
			if c.Name == oidcProxySidecarName {
				found = true
				break
			}
		}
		g.Expect(found).To(BeTrue(), "Head pod should have %s sidecar container", oidcProxySidecarName)

		var statusFound bool
		for _, cs := range headPod.Status.ContainerStatuses {
			if cs.Name == oidcProxySidecarName {
				statusFound = true
				g.Expect(cs.Ready).To(BeTrue(), "%s container should be ready", oidcProxySidecarName)
			}
		}
		g.Expect(statusFound).To(BeTrue(), "ContainerStatuses should include %s", oidcProxySidecarName)
	})

	test.T().Run("Unauthenticated request to auth proxy is rejected", func(_ *testing.T) {
		pythonScript := fmt.Sprintf(`
import urllib.request, ssl
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
try:
    urllib.request.urlopen(urllib.request.Request('https://localhost:%d/'), context=ctx, timeout=5)
    print('STATUS:200')
except urllib.error.HTTPError as e:
    print('STATUS:' + str(e.code))
except Exception as e:
    print('ERROR:' + str(e))
`, authProxyPortNum)

		cmd := []string{"python3", "-c", pythonScript}

		g.Eventually(func(g Gomega) {
			stdout, _ := ExecPodCmd(test, headPod, "ray-head", cmd)
			output := stdout.String()
			g.Expect(output).To(SatisfyAny(
				ContainSubstring("STATUS:401"),
				ContainSubstring("STATUS:403"),
			), "Unauthenticated request should be rejected, got: %s", output)
		}, TestTimeoutShort).Should(Succeed())
	})
}
