package e2e

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJobWithClusterSelector(t *testing.T) {
	_ = WithParallel(t).WithConfigMaps("counter.py", "fail.py")

	t.Run("Successful RayJob", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		jobs := subtest.WithConfigMaps("counter.py", "fail.py")
		g := subtest.Gomega()

		// Create a namespace (ConfigMap will be created automatically)
		namespace := subtest.WithNamespace()

		// RayCluster
		rayClusterAC := rayv1ac.RayCluster("raycluster", namespace.Name).
			WithSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// RayJob
		rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithClusterSelector(map[string]string{utils.RayClusterLabelKey: rayCluster.Name}).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		// Assert the Ray job has completed successfully
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))
	})

	t.Run("Failing RayJob", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		jobs := subtest.WithConfigMaps("counter.py", "fail.py")
		g := subtest.Gomega()

		// Create a namespace (ConfigMap will be created automatically)
		namespace := subtest.WithNamespace()

		// RayCluster
		rayClusterAC := rayv1ac.RayCluster("raycluster", namespace.Name).
			WithSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// RayJob
		rayJobAC := rayv1ac.RayJob("fail", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithClusterSelector(map[string]string{utils.RayClusterLabelKey: rayCluster.Name}).
				WithEntrypoint("python /home/ray/jobs/fail.py").
				WithShutdownAfterJobFinishes(false).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		// Assert the Ray job has failed
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))
	})

	t.Run("RayJob should be created but not to be updated when managed externally", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		jobs := subtest.WithConfigMaps("counter.py", "fail.py")
		g := subtest.Gomega()

		// Create a namespace (ConfigMap will be created automatically)
		namespace := subtest.WithNamespace()

		// RayCluster
		rayClusterAC := rayv1ac.RayCluster("raycluster", namespace.Name).
			WithSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// RayJob
		rayJobAC := rayv1ac.RayJob("managed-externally", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithClusterSelector(map[string]string{utils.RayClusterLabelKey: rayCluster.Name}).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()).
				WithManagedBy("kueue.x-k8s.io/multikueue"))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Assert the Ray job status has not been updated
		g.Consistently(func(gg Gomega) {
			var err2 error
			rayJob, err2 = GetRayJob(subtest, rayJob.Namespace, rayJob.Name)
			err = err2
			gg.Expect(err).ToNot(HaveOccurred())
			gg.Expect(rayJob.Status.JobDeploymentStatus).To(Equal(rayv1.JobDeploymentStatusNew))
		}, time.Second*3, time.Millisecond*500).Should(Succeed())
	})

	t.Run("RayJob should not be created due to managedBy invalid value", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		jobs := subtest.WithConfigMaps("counter.py", "fail.py")
		g := subtest.Gomega()

		// Create a namespace (ConfigMap will be created automatically)
		namespace := subtest.WithNamespace()

		// RayCluster
		rayClusterAC := rayv1ac.RayCluster("raycluster", namespace.Name).
			WithSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")))

		rayCluster, err := subtest.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(subtest.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(t, "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(RayCluster(subtest, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// RayJob
		rayJobAC := rayv1ac.RayJob("managed-externally-invalid", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithClusterSelector(map[string]string{utils.RayClusterLabelKey: rayCluster.Name}).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()).
				WithManagedBy("invalid.com/controller"))

		var err3 error
		_, err3 = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		err = err3
		g.Expect(errors.IsInvalid(err)).To(BeTrue(), "error: %v", err)
	})
}
