package e2e

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJob(t *testing.T) {
	_ = WithParallel(t)

	// Each test will create its own namespace and ConfigMap

	t.Run("Successful RayJob", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		rayJob, err = GetRayJob(subtest, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(t, "Checking that the RayJob status info has been set correctly.")
		g.Expect(rayJob.Status.RayJobStatusInfo.StartTime).NotTo(BeNil())
		g.Expect(rayJob.Status.RayJobStatusInfo.EndTime).NotTo(BeNil())

		g.Eventually(func() error {
			_, err = GetRayCluster(subtest, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutShort).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

		g.Eventually(Jobs(subtest, namespace.Name)).ShouldNot(BeEmpty())

		LogWithTimestamp(t, "Update `suspend` to true. However, since the RayJob is completed, the status should not be updated to `Suspended`.")
		rayJobAC.Spec.WithSuspend(true)
		rayJob, err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		g.Consistently(RayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	t.Run("Failing RayJob without cluster shutdown after finished", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("fail", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/fail.py").
				WithShutdownAfterJobFinishes(false).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))

		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.AppFailed)))

		rayJob, err = GetRayJob(subtest, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		g.Eventually(func() error {
			_, err := GetRayCluster(subtest, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutShort).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

		g.Eventually(Jobs(subtest, namespace.Name), TestTimeoutShort).Should(BeEmpty())
	})

	t.Run("Failing submitter K8s Job", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("fail-k8s-job", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("The command will be overridden by the submitter Job").
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJobAC.Spec.SubmitterPodTemplate.Spec.Containers[0].WithCommand("ray", "job", "submit", "--address", "http://do-not-exist:8265", "--", "echo 123")

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusNew)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.SubmissionFailed)))

		rayJob, err = GetRayJob(subtest, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(func() error {
			_, err := GetRayCluster(subtest, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
		g.Eventually(Jobs(subtest, namespace.Name), TestTimeoutShort).ShouldNot(BeEmpty())

		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	t.Run("Should transition to 'Complete' if the Ray job has stopped", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("stop", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithEntrypoint("python /home/ray/jobs/stop.py").
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to be 'Running'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)))

		LogWithTimestamp(t, "Waiting for RayJob %s/%s to be 'Complete'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusStopped)))

		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	t.Run("RuntimeEnvYAML is not a valid YAML string", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("invalid-yamlstr", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`invalid_yaml_string`).
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		g.Consistently(RayJob(subtest, rayJob.Namespace, rayJob.Name), 5*time.Second).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusNew)))
	})

	t.Run("RayJob has passed ActiveDeadlineSeconds", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("long-running", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running.py").
				WithShutdownAfterJobFinishes(true).
				WithTTLSecondsAfterFinished(600).
				WithActiveDeadlineSeconds(5).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(subtest, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.DeadlineExceeded)))
	})

	t.Run("RayJob should be created but not updated when managed externally", func(t *testing.T) {
		t.Parallel()
		subtest := WithParallel(t)
		g := subtest.Gomega()

		// Create a namespace and ConfigMap
		namespace := subtest.WithNamespace()
		_, jobs := subtest.WithConfigMaps("counter.py", "fail.py", "stop.py", "long_running.py")

		rayJobAC := rayv1ac.RayJob("managed-externally", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(newRayClusterSpec(mountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(jobSubmitterPodTemplateApplyConfiguration()).
				WithManagedBy("kueue.x-k8s.io/multikueue"))

		rayJob, err := subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		rayJobAC.Spec.WithManagedBy(utils.KubeRayController)
		_, err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(subtest.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Eventually(RayJob(subtest, *rayJobAC.Namespace, *rayJobAC.Name)).
			Should(WithTransform(RayJobManagedBy, Equal(ptr.To("kueue.x-k8s.io/multikueue"))))

		g.Eventually(RayJob(subtest, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusNew)))

		rcList, err := subtest.Client().Ray().RayV1().RayClusters(rayJob.Namespace).List(subtest.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		for _, rc := range rcList.Items {
			g.Expect(rc.Name).NotTo(HaveSuffix(*rayJobAC.Name))
		}

		g.Eventually(Jobs(subtest, namespace.Name)).Should(BeEmpty())

		err = subtest.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(subtest.Ctx(), *rayJobAC.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayJob %s/%s successfully", *rayJobAC.Namespace, *rayJobAC.Name)
	})
}
