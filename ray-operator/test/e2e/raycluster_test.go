package e2e

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayClusterManagedBy(t *testing.T) {
	_ = WithParallel(t)

	t.Run("Successful creation of cluster, managed by Kuberay Operator", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace
		namespace := subtest.WithNamespace()

		rayClusterAC := rayv1ac.RayCluster("raycluster-ok", namespace.Name).
			WithSpec(newRayClusterSpec().
				WithManagedBy(utils.KubeRayController))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	})

	t.Run("Creation of cluster skipped, managed by Kueue", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace
		namespace := subtest.WithNamespace()

		rayClusterAC := rayv1ac.RayCluster("raycluster-skip", namespace.Name).
			WithSpec(newRayClusterSpec().
				WithManagedBy("kueue.x-k8s.io/multikueue"))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "RayCluster %s/%s will not become ready - not reconciled", rayCluster.Namespace, rayCluster.Name)
		g.Consistently(func(gg Gomega) {
			rc, err := RayCluster(subtest, rayCluster.Namespace, rayCluster.Name)()
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(rc.Status.Conditions).To(BeEmpty())
		}, time.Second*3, time.Millisecond*500).Should(Succeed())

		// Should not to be able to change managedBy field as it's immutable
		rayClusterAC.Spec.WithManagedBy(utils.KubeRayController)
		rayCluster, err = subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Eventually(RayCluster(subtest, *rayClusterAC.Namespace, *rayClusterAC.Name)).
			Should(WithTransform(RayClusterManagedBy, Equal(ptr.To("kueue.x-k8s.io/multikueue"))))
	})

	t.Run("Failed creation of cluster, managed by external non supported controller", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace
		namespace := subtest.WithNamespace()

		rayClusterAC := rayv1ac.RayCluster("raycluster-fail", namespace.Name).
			WithSpec(newRayClusterSpec().
				WithManagedBy("controller.com/not-supported"))

		_, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Expect(errors.IsInvalid(err)).To(BeTrue(), "error: %v", err)
	})
}

func TestRayClusterSuspend(t *testing.T) {
	t.Run("Suspend and resume cluster", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace
		namespace := subtest.WithNamespace()

		rayClusterAC := rayv1ac.RayCluster("raycluster-suspend", namespace.Name).WithSpec(newRayClusterSpec())

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionTrue, rayv1.HeadPodRunningAndReady)))
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))

		rayClusterAC = rayClusterAC.WithSpec(rayClusterAC.Spec.WithSuspend(true))
		rayCluster, err = subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Suspend RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to be suspended", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.RayClusterSuspended), MatchCondition(metav1.ConditionTrue, string(rayv1.RayClusterSuspended))))
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionFalse, rayv1.HeadPodNotFound)))
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionFalse, rayv1.RayClusterPodsProvisioning)))

		rayClusterAC = rayClusterAC.WithSpec(rayClusterAC.Spec.WithSuspend(false))
		rayCluster, err = subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Resume RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to be resumed", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.RayClusterSuspended), MatchCondition(metav1.ConditionFalse, string(rayv1.RayClusterSuspended))))
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionTrue, rayv1.HeadPodRunningAndReady)))
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))
	})
}

func TestRayClusterWithResourceQuota(t *testing.T) {
	t.Run("Cluster creation with resource quota", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t).
			WithResourceQuota("test-quota", "0.1", "0.1Gi")
		g := subtest.Gomega()

		// Create a namespace (resource quota will be created automatically)
		namespace := subtest.WithNamespace()

		rayClusterAC := rayv1ac.RayCluster("raycluster-resource-quota", namespace.Name).WithSpec(newRayClusterSpec())

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to have ReplicaFailure condition", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, namespace.Name, rayCluster.Name), TestTimeoutShort).
			Should(WithTransform(StatusCondition(rayv1.RayClusterReplicaFailure),
				MatchConditionContainsMessage(metav1.ConditionTrue, utils.ErrFailedCreateHeadPod.Error(), "forbidden: exceeded quota")))
	})
}

func TestRayClusterScalingDown(t *testing.T) {
	t.Run("Scale down cluster replicas", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		subtest.WithFinalizers("test.kuberay.io/finalizers")
		g := subtest.Gomega()

		// Create a namespace (finalizers will be cleaned up automatically)
		namespace := subtest.WithNamespace()

		rayClusterAC := rayv1ac.RayCluster("raycluster-scaling-down", namespace.Name).
			WithSpec(rayv1ac.RayClusterSpec().
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
					WithTemplate(headPodTemplateApplyConfiguration().WithFinalizers("test.kuberay.io/finalizers"))).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(2).
					WithMinReplicas(1).
					WithMaxReplicas(5).
					WithGroupName("small-group").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(workerPodTemplateApplyConfiguration().WithFinalizers("test.kuberay.io/finalizers"))))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", namespace.Name, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", namespace.Name, rayCluster.Name)
		g.Eventually(RayCluster(subtest, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		headPod, err := GetHeadPod(subtest, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		workerPods, err := GetWorkerPods(subtest, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(t, "Scaling down replicas of RayCluster %s/%s by 1", namespace.Name, rayCluster.Name)
		rayClusterAC.Spec.WorkerGroupSpecs[0].WithReplicas(1)
		rayCluster, err = subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to scale down RayCluster")

		time.Sleep(5 * time.Second)

		headPod, err = GetHeadPod(subtest, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod.DeletionTimestamp).To(BeNil(), "Head pod should not have deletionTimestamp")

		workerPods, err = GetWorkerPods(subtest, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		deletingCount := 0
		for _, pod := range workerPods {
			if pod.DeletionTimestamp != nil {
				deletingCount++
			}
		}
		g.Expect(deletingCount).To(Equal(1), "Should have only one worker pod having deletionTimestamp")

		// Finalizers will be cleaned up automatically by WithFinalizers
	})
}
