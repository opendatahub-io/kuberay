package e2e

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	redisPassword = "5241590000000000"
	redisAddress  = "redis:6379"
)

func TestRayClusterGCSFaultTolerance(t *testing.T) {
	_ = WithParallel(t)

	// Create a namespace and ConfigMap
	test := WithParallel(t)
	namespace := test.WithNamespace()
	_, testScript := test.WithConfigMaps("test_detached_actor_1.py", "test_detached_actor_2.py")

	t.Run("Test Detached Actor", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		checkRedisDBSize := deployRedis(subtest, namespace.Name, redisPassword)
		defer g.Eventually(checkRedisDBSize, time.Second*30, time.Second).Should(BeEquivalentTo("0"))

		rayClusterSpecAC := rayv1ac.RayClusterSpec().
			WithGcsFaultToleranceOptions(
				rayv1ac.GcsFaultToleranceOptions().
					WithRedisAddress(redisAddress).
					WithRedisPassword(rayv1ac.RedisCredential().WithValue(redisPassword)),
			).
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{
					"num-cpus": "0",
				}).
				WithTemplate(headPodTemplateApplyConfiguration()),
			).
			WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithRayStartParams(map[string]string{
					"num-cpus": "1",
				}).
				WithGroupName("small-group").
				WithReplicas(1).
				WithMinReplicas(1).
				WithMaxReplicas(2).
				WithTemplate(workerPodTemplateApplyConfiguration()),
			)
		rayClusterAC := rayv1ac.RayCluster("raycluster-gcsft", namespace.Name).
			WithSpec(apply(rayClusterSpecAC, mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](testScript, "/home/ray/samples")))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)

		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))

		headPod, err := GetHeadPod(subtest, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(t, "HeadPod Name: %s", headPod.Name)

		rayNamespace := "testing-ray-namespace"
		LogWithTimestamp(t, "Ray namespace: %s", rayNamespace)

		ExecPodCmd(subtest, headPod, common.RayHeadContainer, []string{"python", "samples/test_detached_actor_1.py", rayNamespace})

		// [Test 1: Kill GCS process to "restart" the head Pod]
		// Assertion is implement in python, so no furthur handling needed here, and so are other ExecPodCmd
		stdout, stderr := ExecPodCmd(subtest, headPod, common.RayHeadContainer, []string{"pkill", "gcs_server"})
		LogWithTimestamp(t, "pkill gcs_server output - stdout: %s, stderr: %s", stdout.String(), stderr.String())

		// Restart count should eventually become 1, not creating a new pod
		HeadPodRestartCount := func(p *corev1.Pod) int32 { return p.Status.ContainerStatuses[0].RestartCount }
		HeadPodContainerReady := func(p *corev1.Pod) bool { return p.Status.ContainerStatuses[0].Ready }

		g.Eventually(HeadPod(subtest, rayCluster), TestTimeoutMedium).
			Should(WithTransform(HeadPodRestartCount, Equal(int32(1))))
		g.Eventually(HeadPod(subtest, rayCluster), TestTimeoutMedium).
			Should(WithTransform(HeadPodContainerReady, Equal(true)))

		// Pod Status should eventually become Running
		PodState := func(p *corev1.Pod) string { return string(p.Status.Phase) }
		g.Eventually(HeadPod(subtest, rayCluster)).
			Should(WithTransform(PodState, Equal("Running")))

		headPod, err = GetHeadPod(subtest, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())

		expectedOutput := "3"
		ExecPodCmd(subtest, headPod, common.RayHeadContainer, []string{"python", "samples/test_detached_actor_2.py", rayNamespace, expectedOutput})

		// Test 2: Delete the head Pod
		err = subtest.Client().Core().CoreV1().Pods(namespace.Name).Delete(subtest.Ctx(), headPod.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		PodUID := func(p *corev1.Pod) string { return string(p.UID) }
		g.Eventually(HeadPod(subtest, rayCluster), TestTimeoutMedium).
			ShouldNot(WithTransform(PodUID, Equal(string(headPod.UID)))) // Use UID to check if the new head pod is created.

		g.Eventually(HeadPod(subtest, rayCluster), TestTimeoutMedium).
			Should(WithTransform(PodState, Equal("Running")))

		headPod, err = GetHeadPod(subtest, rayCluster) // Replace the old head pod
		g.Expect(err).NotTo(HaveOccurred())

		expectedOutput = "4"

		ExecPodCmd(subtest, headPod, common.RayHeadContainer, []string{"python", "samples/test_detached_actor_2.py", rayNamespace, expectedOutput})

		err = subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(subtest.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
	})
}

func TestGcsFaultToleranceOptions(t *testing.T) {
	// Each test uses a separate namespace to utilize different Redis instances
	// for better isolation.
	testCases := []struct {
		rayClusterFn  func(namespace string) *rayv1ac.RayClusterApplyConfiguration
		name          string
		redisPassword string
		createSecret  bool
	}{
		{
			name:          "No Redis Password",
			redisPassword: "",
			rayClusterFn: func(namespace string) *rayv1ac.RayClusterApplyConfiguration {
				return rayv1ac.RayCluster("raycluster-gcsft", namespace).WithSpec(
					newRayClusterSpec().WithGcsFaultToleranceOptions(
						rayv1ac.GcsFaultToleranceOptions().
							WithRedisAddress(redisAddress),
					),
				)
			},
			createSecret: false,
		},
		{
			name:          "Redis Password",
			redisPassword: redisPassword,
			rayClusterFn: func(namespace string) *rayv1ac.RayClusterApplyConfiguration {
				return rayv1ac.RayCluster("raycluster-gcsft", namespace).WithSpec(
					newRayClusterSpec().WithGcsFaultToleranceOptions(
						rayv1ac.GcsFaultToleranceOptions().
							WithRedisAddress(redisAddress).
							WithRedisPassword(rayv1ac.RedisCredential().WithValue(redisPassword)),
					),
				)
			},
			createSecret: false,
		},
		{
			name:          "Redis Password and Username",
			redisPassword: redisPassword,
			rayClusterFn: func(namespace string) *rayv1ac.RayClusterApplyConfiguration {
				return rayv1ac.RayCluster("raycluster-gcsft", namespace).WithSpec(
					newRayClusterSpec().WithGcsFaultToleranceOptions(
						rayv1ac.GcsFaultToleranceOptions().
							WithRedisAddress(redisAddress).
							WithRedisUsername(rayv1ac.RedisCredential().WithValue("default")).
							WithRedisPassword(rayv1ac.RedisCredential().WithValue(redisPassword)),
					),
				)
			},
			createSecret: false,
		},
		{
			name:          "Redis Password In Secret",
			redisPassword: redisPassword,
			rayClusterFn: func(namespace string) *rayv1ac.RayClusterApplyConfiguration {
				return rayv1ac.RayCluster("raycluster-gcsft", namespace).WithSpec(
					newRayClusterSpec().WithGcsFaultToleranceOptions(
						rayv1ac.GcsFaultToleranceOptions().
							WithRedisAddress(redisAddress).
							WithRedisPassword(rayv1ac.RedisCredential().
								WithValueFrom(corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: "redis-password-secret"},
										Key:                  "password",
									},
								}),
							),
					),
				)
			},
			createSecret: true,
		},
		{
			name:          "Long RayCluster Name",
			redisPassword: "",
			rayClusterFn: func(namespace string) *rayv1ac.RayClusterApplyConfiguration {
				// Intentionally using a long name to test job name trimming
				return rayv1ac.RayCluster("raycluster-with-a-very-long-name-exceeding-k8s-limit", namespace).WithSpec(
					newRayClusterSpec().WithGcsFaultToleranceOptions(
						rayv1ac.GcsFaultToleranceOptions().
							WithRedisAddress(redisAddress),
					),
				)
			},
			createSecret: false,
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			test := WithParallel(t)
			g := test.Gomega()
			namespace := test.WithNamespace()

			checkRedisDBSize := deployRedis(test, namespace.Name, tc.redisPassword)
			defer g.Eventually(checkRedisDBSize, time.Second*30, time.Second).Should(BeEquivalentTo("0"))

			if tc.createSecret {
				LogWithTimestamp(t, "Creating Redis password secret")
				_, err := test.Client().Core().CoreV1().Secrets(namespace.Name).Apply(
					test.Ctx(),
					corev1ac.Secret("redis-password-secret", namespace.Name).
						WithStringData(map[string]string{"password": tc.redisPassword}),
					TestApplyOptions,
				)
				g.Expect(err).NotTo(HaveOccurred())
			}

			rayClusterAC := tc.rayClusterFn(namespace.Name)
			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(HaveOccurred())
			LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))

			LogWithTimestamp(t, "Verifying environment variables on Head Pod")
			rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			headPod, err := test.Client().Core().CoreV1().Pods(namespace.Name).Get(test.Ctx(), rayCluster.Status.Head.PodName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(utils.EnvVarExists(utils.RAY_REDIS_ADDRESS, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeTrue())
			g.Expect(utils.EnvVarExists(utils.RAY_EXTERNAL_STORAGE_NS, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeTrue())
			if tc.redisPassword == "" {
				g.Expect(utils.EnvVarExists(utils.REDIS_PASSWORD, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeFalse())
			} else {
				g.Expect(utils.EnvVarExists(utils.REDIS_PASSWORD, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeTrue())
			}

			err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		})
	}
}

func TestGcsFaultToleranceAnnotations(t *testing.T) {
	tests := []struct {
		name                          string
		storageNS                     string
		redisPasswordEnv              string
		redisPasswordInRayStartParams string
	}{
		{
			name:                          "GCS FT without redis password",
			storageNS:                     "",
			redisPasswordEnv:              "",
			redisPasswordInRayStartParams: "",
		},
		{
			name:                          "GCS FT with redis password in ray start params",
			storageNS:                     "",
			redisPasswordEnv:              "",
			redisPasswordInRayStartParams: redisPassword,
		},
		{
			name:                          "GCS FT with redis password in ray start params referring to env",
			storageNS:                     "",
			redisPasswordEnv:              redisPassword,
			redisPasswordInRayStartParams: "$REDIS_PASSWORD",
		},
		{
			name:                          "GCS FT with storage namespace",
			storageNS:                     "test-storage-ns",
			redisPasswordEnv:              redisPassword,
			redisPasswordInRayStartParams: "$REDIS_PASSWORD",
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			test := WithParallel(t)
			g := test.Gomega()
			namespace := test.WithNamespace()

			redisPassword := ""
			require.False(t, tc.redisPasswordEnv != "" && tc.redisPasswordInRayStartParams != "" && tc.redisPasswordInRayStartParams != "$REDIS_PASSWORD", "redisPasswordEnv and redisPasswordInRayStartParams are both set")

			switch {
			case tc.redisPasswordEnv != "":
				redisPassword = tc.redisPasswordEnv
			case tc.redisPasswordInRayStartParams != "":
				redisPassword = tc.redisPasswordInRayStartParams
			}

			checkRedisDBSize := deployRedis(test, namespace.Name, redisPassword)
			defer g.Eventually(checkRedisDBSize, time.Second*30, time.Second).Should(BeEquivalentTo("0"))

			// Prepare RayCluster ApplyConfiguration
			podTemplateAC := headPodTemplateApplyConfiguration()
			podTemplateAC.Spec.Containers[utils.RayContainerIndex].WithEnv(
				corev1ac.EnvVar().WithName("RAY_REDIS_ADDRESS").WithValue(redisAddress),
			)
			if tc.redisPasswordEnv != "" {
				podTemplateAC.Spec.Containers[utils.RayContainerIndex].WithEnv(
					corev1ac.EnvVar().WithName("REDIS_PASSWORD").WithValue(tc.redisPasswordEnv),
				)
			}
			rayClusterAC := rayv1ac.RayCluster("raycluster-gcsft", namespace.Name).WithAnnotations(
				map[string]string{utils.RayFTEnabledAnnotationKey: "true"},
			).WithSpec(
				rayClusterSpecWith(
					rayv1ac.RayClusterSpec().
						WithRayVersion(GetRayVersion()).
						WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
							// RayStartParams are not allowed to be empty.
							WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
							WithTemplate(podTemplateAC)),
				),
			)
			if tc.storageNS != "" {
				rayClusterAC.WithAnnotations(map[string]string{utils.RayExternalStorageNSAnnotationKey: tc.storageNS})
			}
			if tc.redisPasswordInRayStartParams != "" {
				rayClusterAC.Spec.HeadGroupSpec.WithRayStartParams(map[string]string{"redis-password": tc.redisPasswordInRayStartParams})
			}

			// Apply RayCluster
			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(HaveOccurred())
			LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))

			LogWithTimestamp(t, "Verifying environment variables on Head Pod")
			rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			headPod, err := test.Client().Core().CoreV1().Pods(namespace.Name).Get(test.Ctx(), rayCluster.Status.Head.PodName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(utils.EnvVarExists(utils.RAY_REDIS_ADDRESS, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeTrue())
			g.Expect(utils.EnvVarExists(utils.RAY_EXTERNAL_STORAGE_NS, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeTrue())
			if redisPassword == "" {
				g.Expect(utils.EnvVarExists(utils.REDIS_PASSWORD, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeFalse())
			} else {
				g.Expect(utils.EnvVarExists(utils.REDIS_PASSWORD, headPod.Spec.Containers[utils.RayContainerIndex].Env)).Should(BeTrue())
			}

			err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		})
	}
}
