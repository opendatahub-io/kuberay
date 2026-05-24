package e2e

import (
	"testing"

	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJobSuspend(t *testing.T) {
	_ = WithParallel(t).WithConfigMaps("long_running.py", "counter.py")

	t.Run("Suspend the RayJob when its status is 'Running', and then resume it.", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		jobs := subtest.WithConfigMaps("long_running.py", "counter.py")
		g := subtest.Gomega()

		// Create a namespace (ConfigMap will be created automatically)
		namespace := subtest.WithNamespace()

		// RayJob
		rayJobAC := rayv1ac.RayJob("long-running", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running.py").
				WithShutdownAfterJobFinishes(true).
				WithTTLSecondsAfterFinished(600).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to be 'Running'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)))

		LogWithTimestamp(t, "Suspend the RayJob %s/%s", rayJob.Namespace, rayJob.Name)
		rayJobAC.Spec.WithSuspend(true)
		rayJob, err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to be 'Suspended'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusSuspended)))

		// Assert the RayCluster has been torn down
		g.Eventually(func() error {
			_, err := GetRayCluster(subtest, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutShort).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

		// Assert the submitter Job has been cascade deleted
		g.Eventually(Jobs(subtest, namespace.Name), TestTimeoutShort).Should(BeEmpty())

		LogWithTimestamp(t, "Resume the RayJob by updating `suspend` to false.")
		rayJobAC.Spec.WithSuspend(false)
		rayJob, err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)))

		// Delete the RayJob
		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	t.Run("Create a suspended RayJob, and then resume it.", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		jobs := subtest.WithConfigMaps("long_running.py", "counter.py")
		g := subtest.Gomega()

		// Create a namespace (ConfigMap will be created automatically)
		namespace := subtest.WithNamespace()

		// RayJob
		rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSuspend(true).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to be 'Suspended'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusSuspended)))

		LogWithTimestamp(t, "Resume the RayJob by updating `suspend` to false.")
		rayJobAC.Spec.WithSuspend(false)
		rayJob, err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		// Assert the RayJob has completed successfully
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

		// Refresh the RayJob status
		rayJob, err = GetRayJob(subtest, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		// Delete the RayJob
		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Assert the RayCluster has been cascade deleted
		g.Eventually(func() error {
			_, err := GetRayCluster(subtest, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutShort).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

		// Assert the Pods has been cascade deleted
		g.Eventually(Pods(subtest, namespace.Name,
			LabelSelector(utils.RayClusterLabelKey+"="+rayJob.Status.RayClusterName)), TestTimeoutShort).
			Should(BeEmpty())
	})
}
