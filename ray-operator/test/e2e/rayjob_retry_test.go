package e2e

import (
	"testing"

	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJobRetry(t *testing.T) {
	test := WithParallel(t)
	g := test.Gomega()

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Job scripts
	jobsAC := newConfigMap(namespace.Name, files(test, "fail.py"))
	jobs := test.NewConfigMap(namespace.Name, jobsAC.Name, jobsAC.Data)
	LogWithTimestamp(t, "Created ConfigMap %s/%s successfully", jobs.Namespace, jobs.Name)

	t.Run("Failing RayJob without cluster shutdown after finished", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// RayJob: Set RayJob.BackoffLimit to 2
		rayJobAC := rayv1ac.RayJob("fail", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithBackoffLimit(2).
				WithSubmitterConfig(rayv1ac.SubmitterConfig().
					WithBackoffLimit(1)).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/fail.py").
				WithShutdownAfterJobFinishes(false).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)

		// Assert that the RayJob deployment status and RayJob reason have been updated accordingly.
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutLong).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.AppFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))

		// Check whether the controller respects the backoffLimit.
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobFailed, Equal(int32(3))))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobSucceeded, Equal(int32(0))))

		job, err := subtest.Client().Core().BatchV1().Jobs(namespace.Name).Get(subtest.Ctx(), rayJob.Name, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(job.Spec.BackoffLimit).To(HaveValue(Equal(int32(1))))
		g.Expect(job.Status.Failed).To(Equal(int32(0))) // Ray job failures (at the application level) are not counted as K8s job submitter failures; K8s job submitter failures only count for issues like network errors.
		g.Expect(job.Status.Succeeded).To(Equal(int32(1)))

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

		// Assert the submitter Job has been cascade deleted
		g.Eventually(Jobs(subtest, namespace.Name), TestTimeoutShort).Should(BeEmpty())
	})

	t.Run("Failing submitter K8s Job", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// RayJob: Set RayJob.BackoffLimit to 2 & SubmitterConfig.BackoffLimit to 0 to test RayJob level backoffLimit
		rayJobAC := rayv1ac.RayJob("fail-submitter-k8s-job", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithBackoffLimit(2).
				WithSubmitterConfig(rayv1ac.SubmitterConfig().
					WithBackoffLimit(0)).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("The command will be overridden by the submitter Job").
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		// In this test, we try to simulate the case where the submitter Job can't connect to the RayCluster successfully.
		// Hence, KubeRay can't get the Ray job information from the RayCluster. When the RayJob reaches the backoff
		// limit, it will be marked as failed. Then, the RayJob should transition to `Failed`.
		rayJobAC.Spec.SubmitterPodTemplate.Spec.Containers[0].WithCommand("ray", "job", "submit", "--address", "http://do-not-exist:8265", "--", "echo 123")

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)

		// Ensure JobDeploymentStatus transit to Failed
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		// Ensure JobStatus is empty
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusNew)))
		// Ensure Reason is SubmissionFailed
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.SubmissionFailed)))

		// Check whether the controller respects the backoffLimit.
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobFailed, Equal(int32(3))))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobSucceeded, Equal(int32(0))))

		// Refresh the RayJob status
		rayJob, err = GetRayJob(subtest, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		// Assert the RayCluster has been deleted because ShutdownAfterJobFinishes is true.
		g.Eventually(func() error {
			_, err := GetRayCluster(subtest, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
		// Asset submitter Job is not deleted yet
		g.Eventually(Jobs(subtest, namespace.Name), TestTimeoutShort).ShouldNot(BeEmpty())

		// Delete the RayJob
		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	t.Run("RayJob has passed ActiveDeadlineSeconds", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// RayJob will transition to JobDeploymentStatusFailed
		// regardless of the value of backoffLimit.
		rayJobAC := rayv1ac.RayJob("long-running", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithBackoffLimit(2).
				WithSubmitterConfig(rayv1ac.SubmitterConfig().
					WithBackoffLimit(0)).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running.py").
				WithShutdownAfterJobFinishes(true).
				WithTTLSecondsAfterFinished(600).
				WithActiveDeadlineSeconds(5).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// The RayJob will transition to `Failed` because it has passed `ActiveDeadlineSeconds`.
		LogWithTimestamp(t, "Waiting for RayJob %s/%s to be 'Failed'", rayJob.Namespace, rayJob.Name)

		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.DeadlineExceeded)))

		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobFailed, Equal(int32(1))))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobSucceeded, Equal(int32(0))))
	})

	t.Run("Failing RayJob with HttpMode submission mode", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Set up the RayJob with HTTP mode and a BackoffLimit
		rayJobAC := rayv1ac.RayJob("failing-rayjob-in-httpmode", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.HTTPMode).
				WithBackoffLimit(2).
				WithEntrypoint("python /home/ray/jobs/fail.py").
				WithShutdownAfterJobFinishes(false).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)

		// Assert that the RayJob deployment status has been updated.
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))

		// Assert the Ray job has failed.
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))

		// Check the RayJob reason has been updated.
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.AppFailed)))

		// Check whether the controller respects the backoffLimit.
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobFailed, Equal(int32(3)))) // 2 retries + 1 initial attempt = 3 failures
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobSucceeded, Equal(int32(0))))

		// Clean up
		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})
}
