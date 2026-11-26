/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ray

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestNetworkPolicyUWMUpgrade(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true,
	}
	config, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))
	c, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)

	cluster := rayClusterTemplateForNetworkPolicy("uwm-upgrade", "default")
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 9090}}
	legacyRule := networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{"openshift-user-workload-monitoring"},
		}}}}},
		Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(8080))}},
	}
	cluster.Spec.NetworkPolicy.Head.IngressRules = append(cluster.Spec.NetworkPolicy.Head.IngressRules, legacyRule)
	require.NoError(t, c.Create(t.Context(), cluster))
	before := cluster.DeepCopy()
	r := &NetworkPolicyController{Scheme: scheme}
	policies := []*networkingv1.NetworkPolicy{
		r.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAllIngress),
		r.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAllIngress, "small-group"),
		r.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAllIngress, "removed"),
	}
	for _, policy := range policies {
		require.NoError(t, controllerutil.SetControllerReference(cluster, policy, scheme))
		require.NoError(t, c.Create(t.Context(), policy))
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "existing-worker", Namespace: cluster.Namespace,
			Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode), utils.RayNodeGroupLabelKey: "small-group"},
		},
		Spec: *cluster.Spec.WorkerGroupSpecs[0].Template.Spec.DeepCopy(),
	}
	pod.Spec.Containers[0].Ports[0].ContainerPort = 8080
	require.NoError(t, controllerutil.SetControllerReference(cluster, pod, scheme))
	require.NoError(t, c.Create(t.Context(), pod))
	beforePod := pod.DeepCopy()
	retainedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "removed-group-worker", Namespace: cluster.Namespace, Labels: policies[2].Spec.PodSelector.MatchLabels, OwnerReferences: pod.OwnerReferences},
		Spec:       *pod.Spec.DeepCopy(),
	}
	retainedPod.Spec.Containers[0].Ports[0].ContainerPort = 9191
	require.NoError(t, c.Create(t.Context(), retainedPod))
	beforeRetainedPod := retainedPod.DeepCopy()
	headPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-head", Namespace: cluster.Namespace, Labels: policies[0].Spec.PodSelector.MatchLabels, OwnerReferences: pod.OwnerReferences},
		Spec:       *pod.Spec.DeepCopy(),
	}
	require.NoError(t, c.Create(t.Context(), headPod))

	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: controllerconfig.Controller{SkipNameValidation: new(true)},
	})
	require.NoError(t, err)
	r, err = NewNetworkPolicyController(mgr, true)
	require.NoError(t, err)
	require.NoError(t, r.SetupWithManager(mgr, 1))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("NetworkPolicy manager failed to stop")
		}
	})

	for _, target := range []*corev1.Pod{headPod, pod, retainedPod} {
		require.Eventually(t, func() bool {
			return slices.Equal(uwmMetricsPortsForPod(t, c, target), []int32{target.Spec.Containers[0].Ports[0].ContainerPort})
		}, 10*time.Second, 50*time.Millisecond, target.Name)
	}
	for _, old := range policies {
		got := &networkingv1.NetworkPolicy{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(old), got))
		require.NotContains(t, got.Spec.Ingress, legacyRule)
		require.Empty(t, uwmMetricsPorts(got), "metrics access must be scoped separately from the group isolation policy")
		require.Equal(t, old.UID, got.UID, "upgrade must update the existing policy")
		require.Equal(t, old.Spec.PodSelector, got.Spec.PodSelector)
	}
	got := &rayv1.RayCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), got))
	require.Equal(t, before.Spec, got.Spec)
	require.Equal(t, before.Generation, got.Generation)
	gotPod := &corev1.Pod{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), gotPod))
	require.Equal(t, beforePod.UID, gotPod.UID, "policy reconciliation must not replace Pods")
	require.Equal(t, beforePod.Spec, gotPod.Spec, "only a Pod metadata label may change")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(retainedPod), gotPod))
	require.Equal(t, beforeRetainedPod.UID, gotPod.UID)
	require.Equal(t, beforeRetainedPod.Spec, gotPod.Spec)

	checkWorkerPorts := func(want ...int32) {
		require.Eventually(t, func() bool {
			items := &networkingv1.NetworkPolicyList{}
			if err := c.List(t.Context(), items, client.InNamespace(cluster.Namespace), client.MatchingLabels{utils.RayNodeGroupLabelKey: "small-group"}); err != nil {
				return false
			}
			var ports []int32
			for _, item := range items.Items {
				ports = append(ports, uwmMetricsPorts(&item)...)
			}
			slices.Sort(ports)
			return slices.Equal(ports, want)
		}, 10*time.Second, 50*time.Millisecond)
	}
	checkWorkerPorts(8080)
	anotherPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "another-worker-version", Namespace: cluster.Namespace, Labels: pod.Labels, OwnerReferences: pod.OwnerReferences},
		Spec:       *pod.Spec.DeepCopy(),
	}
	anotherPod.Spec.Containers[0].Ports[0].ContainerPort = 9191
	require.NoError(t, c.Create(t.Context(), anotherPod))
	checkWorkerPorts(8080, 9191)
	require.Equal(t, []int32{8080}, uwmMetricsPortsForPod(t, c, pod))
	require.Equal(t, []int32{9191}, uwmMetricsPortsForPod(t, c, anotherPod))
	require.NoError(t, c.Delete(t.Context(), pod, client.GracePeriodSeconds(0)))
	checkWorkerPorts(9191)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(anotherPod), anotherPod))
	anotherPod.Status.Phase = corev1.PodFailed
	require.NoError(t, c.Status().Update(t.Context(), anotherPod))
	checkWorkerPorts()
	require.NoError(t, c.Delete(t.Context(), retainedPod, client.GracePeriodSeconds(0)))
	require.Eventually(t, func() bool {
		return errors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(policies[2]), &networkingv1.NetworkPolicy{}))
	}, 10*time.Second, 50*time.Millisecond)
}

func rayClusterTemplateForNetworkPolicy(name string, namespace string) *rayv1.RayCluster {
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				utils.EnableSecureTrustedNetworkAnnotationKey: "true",
			},
		},
		Spec: rayv1.RayClusterSpec{
			RayVersion: support.GetRayVersion(),
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: support.GetRayImage(),
							},
						},
					},
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					Replicas:    ptr.To[int32](1),
					MinReplicas: ptr.To[int32](0),
					MaxReplicas: ptr.To[int32](2),
					GroupName:   "small-group",
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "ray-worker",
									Image: support.GetRayImage(),
								},
							},
						},
					},
				},
			},
		},
	}
	utils.EnsureOpenShiftRayClusterSecurity(cluster)
	return cluster
}

var _ = Context("NetworkPolicy Controller Integration Tests", func() {
	Describe("Basic NetworkPolicy Creation", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-networkpolicy", namespace)

		It("Verify RayCluster spec", func() {
			Expect(rayCluster.Spec.WorkerGroupSpecs).To(HaveLen(1))
			Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(ptr.To[int32](1)))
		})

		It("Create a RayCluster custom resource", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayCluster: %v", rayCluster.Name)
		})

		It("Check Head NetworkPolicy is created", func() {
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedHeadName := rayCluster.Name + "-head"
			headNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedHeadName}

			Eventually(
				getResourceFunc(ctx, headNamespacedName, headNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Head NetworkPolicy should be created: %v", expectedHeadName)
		})

		It("Check Worker NetworkPolicy is created", func() {
			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedWorkerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedWorkerName}

			Eventually(
				getResourceFunc(ctx, workerNamespacedName, workerNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Worker NetworkPolicy should be created: %v", expectedWorkerName)
		})

		It("Verify Head NetworkPolicy has correct structure", func() {
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedHeadName := rayCluster.Name + "-head"
			headNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedHeadName}

			err := k8sClient.Get(ctx, headNamespacedName, headNetworkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Failed to get Head NetworkPolicy")

			// Verify basic properties
			Expect(headNetworkPolicy.Name).To(Equal(expectedHeadName))
			Expect(headNetworkPolicy.Namespace).To(Equal(namespace))

			// Verify labels
			expectedLabels := map[string]string{
				utils.RayClusterLabelKey:                rayCluster.Name,
				utils.RayNodeGroupLabelKey:              utils.RayNodeHeadGroupLabelValue,
				utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
				utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
			}
			Expect(headNetworkPolicy.Labels).To(Equal(expectedLabels))

			// Verify owner reference is set
			Expect(headNetworkPolicy.OwnerReferences).To(HaveLen(1))
			Expect(headNetworkPolicy.OwnerReferences[0].Name).To(Equal(rayCluster.Name))
			Expect(headNetworkPolicy.OwnerReferences[0].Kind).To(Equal("RayCluster"))

			// Verify policy type
			Expect(headNetworkPolicy.Spec.PolicyTypes).To(Equal([]networkingv1.PolicyType{networkingv1.PolicyTypeIngress}))

			// Verify pod selector targets head pods only
			expectedPodSelector := metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:  rayCluster.Name,
					utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
				},
			}
			Expect(headNetworkPolicy.Spec.PodSelector).To(Equal(expectedPodSelector))

			// Verify ingress rules: intra-cluster plus the OpenShift security rules.
			Expect(headNetworkPolicy.Spec.Ingress).To(HaveLen(6))

			// Verify Rule 1: Intra-cluster communication - NO PORTS (allows all)
			intraClusterRule := headNetworkPolicy.Spec.Ingress[0]
			Expect(intraClusterRule.From).To(HaveLen(1))
			Expect(intraClusterRule.Ports).To(BeEmpty(), "Intra-cluster rule should have NO ports (allows all)")

			expectedIntraClusterPeer := networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						utils.RayClusterLabelKey: rayCluster.Name,
					},
				},
			}
			Expect(intraClusterRule.From[0]).To(Equal(expectedIntraClusterPeer))

			// Verify Rule 2: External access from any pod in namespace
			externalRule := headNetworkPolicy.Spec.Ingress[1]
			Expect(externalRule.From).To(HaveLen(1))
			Expect(externalRule.Ports).To(HaveLen(2), "External rule should have 2 ports (10001, 8265)")

			// Verify empty pod selector (any pod in namespace)
			expectedAnyPodPeer := networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{},
			}
			Expect(externalRule.From[0]).To(Equal(expectedAnyPodPeer))

			// Verify RayJob submitter access on the dashboard port.
			rayJobRule := headNetworkPolicy.Spec.Ingress[2]
			expectedRayJobPeer := networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						utils.RayOriginatedFromCRDLabelKey: utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
					},
				},
			}
			Expect(rayJobRule.From).To(Equal([]networkingv1.NetworkPolicyPeer{expectedRayJobPeer}))
			Expect(rayJobRule.Ports).To(HaveLen(1))
			Expect(rayJobRule.Ports[0].Port.IntVal).To(Equal(int32(8265)))

			// Verify the secured port rule: NO FROM (allows all).
			securedRule := headNetworkPolicy.Spec.Ingress[3]
			Expect(securedRule.From).To(BeEmpty(), "Secured ports rule should have NO from (allows all)")
			Expect(securedRule.Ports).To(HaveLen(1), "Secured ports rule should have 1 port (8443 only)")

			// Check for mTLS port 8443 (port 10001 is NOT in this rule - it's restricted to namespace/cluster/operator)
			Expect(securedRule.Ports[0].Port.IntVal).To(Equal(int32(8443)), "Should only include mTLS port 8443")
		})

		It("Verify Worker NetworkPolicy has correct structure", func() {
			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedWorkerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedWorkerName}

			err := k8sClient.Get(ctx, workerNamespacedName, workerNetworkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Failed to get Worker NetworkPolicy")

			// Verify basic properties
			Expect(workerNetworkPolicy.Name).To(Equal(expectedWorkerName))
			Expect(workerNetworkPolicy.Namespace).To(Equal(namespace))

			// Verify pod selector targets worker pods only
			expectedPodSelector := metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:   rayCluster.Name,
					utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
					utils.RayNodeGroupLabelKey: rayCluster.Spec.WorkerGroupSpecs[0].GroupName,
				},
			}
			Expect(workerNetworkPolicy.Spec.PodSelector).To(Equal(expectedPodSelector))

			// Verify ingress rules - workers only allow intra-cluster communication
			Expect(workerNetworkPolicy.Spec.Ingress).To(HaveLen(1))
			Expect(workerNetworkPolicy.Spec.Ingress[0].From).To(HaveLen(1))

			// Verify intra-cluster peer
			intraClusterPeer := workerNetworkPolicy.Spec.Ingress[0].From[0]
			expectedIntraClusterPeer := networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						utils.RayClusterLabelKey: rayCluster.Name,
					},
				},
			}
			Expect(intraClusterPeer).To(Equal(expectedIntraClusterPeer))
		})

		It("Delete RayCluster should delete NetworkPolicies", func() {
			// Delete the RayCluster
			err := k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")

			// Note: envtest doesn't run garbage collection automatically like a real cluster
			// In a real cluster, the NetworkPolicies would be automatically deleted due to owner reference
			// For testing, we manually delete them to simulate garbage collection

			// Clean up head NetworkPolicy
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedHeadName := rayCluster.Name + "-head"
			headNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedHeadName}

			err = k8sClient.Get(ctx, headNamespacedName, headNetworkPolicy)
			if err == nil {
				err = k8sClient.Delete(ctx, headNetworkPolicy)
				Expect(err).NotTo(HaveOccurred(), "Failed to manually delete Head NetworkPolicy")
			}

			// Clean up worker NetworkPolicy
			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedWorkerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedWorkerName}

			err = k8sClient.Get(ctx, workerNamespacedName, workerNetworkPolicy)
			if err == nil {
				err = k8sClient.Delete(ctx, workerNetworkPolicy)
				Expect(err).NotTo(HaveOccurred(), "Failed to manually delete Worker NetworkPolicy")
			}

			// Verify both NetworkPolicies are deleted
			Eventually(
				func() bool {
					headErr := k8sClient.Get(ctx, headNamespacedName, headNetworkPolicy)
					workerErr := k8sClient.Get(ctx, workerNamespacedName, workerNetworkPolicy)
					return (headErr != nil && client.IgnoreNotFound(headErr) == nil) &&
						(workerErr != nil && client.IgnoreNotFound(workerErr) == nil)
				},
				time.Second*5, time.Millisecond*500).Should(BeTrue(), "Both NetworkPolicies should be deleted")
		})
	})

	Describe("RayCluster owned by RayJob", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-rayjob", namespace)

		// Add RayJob owner reference
		rayCluster.Labels = map[string]string{
			utils.RayJobSubmissionModeLabelKey: string(rayv1.K8sJobMode),
		}
		rayCluster.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "ray.io/v1",
				Kind:       "RayJob",
				Name:       "test-rayjob",
				UID:        "12345",
				Controller: new(true),
			},
		}

		It("Create a RayCluster with RayJob owner", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster with RayJob owner")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayCluster: %v", rayCluster.Name)
		})

		It("Check Head NetworkPolicy includes RayJob peer", func() {
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedHeadName := rayCluster.Name + "-head"
			headNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedHeadName}

			Eventually(
				getResourceFunc(ctx, headNamespacedName, headNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Head NetworkPolicy should be created")

			// Find the RayJob peer among the ingress rules.
			foundRayJobPeer := false
			for _, rule := range headNetworkPolicy.Spec.Ingress {
				if len(rule.From) != 1 || rule.From[0].PodSelector == nil {
					continue
				}
				labels := rule.From[0].PodSelector.MatchLabels
				if labels[utils.RayOriginatedFromCRNameLabelKey] == "test-rayjob" &&
					labels[utils.RayOriginatedFromCRDLabelKey] == utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD) {
					foundRayJobPeer = true
					break
				}
			}
			Expect(foundRayJobPeer).To(BeTrue(), "Should have an ingress rule for the RayJob submitter")
		})

		It("Clean up RayCluster with RayJob owner", func() {
			err := k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")
		})
	})

	Describe("NetworkPolicy Already Exists", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-existing-np", namespace)
		existingHeadNetworkPolicy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rayCluster.Name + "-head",
				Namespace: namespace,
				Labels: map[string]string{
					"test": "existing",
				},
			},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			},
		}

		It("Create Head NetworkPolicy before RayCluster", func() {
			err := k8sClient.Create(ctx, existingHeadNetworkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Failed to create existing Head NetworkPolicy")
		})

		It("Does not take ownership of an existing NetworkPolicy", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayCluster: %v", rayCluster.Name)

			// Head NetworkPolicy should be updated by the controller
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			headNamespacedName := types.NamespacedName{Namespace: namespace, Name: existingHeadNetworkPolicy.Name}
			err = k8sClient.Get(ctx, headNamespacedName, headNetworkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Head NetworkPolicy should exist")
			Expect(headNetworkPolicy.Labels).To(HaveKeyWithValue("test", "existing"))
			Expect(headNetworkPolicy.OwnerReferences).To(BeEmpty())
		})

		It("Clean up resources", func() {
			err := k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")

			err = k8sClient.Delete(ctx, existingHeadNetworkPolicy)
			// Policy might have been updated by controller, ignore delete errors
			_ = err

			// Clean up worker policy if it exists
			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			expectedWorkerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerNamespacedName := types.NamespacedName{Namespace: namespace, Name: expectedWorkerName}
			err = k8sClient.Get(ctx, workerNamespacedName, workerNetworkPolicy)
			if err == nil {
				err = k8sClient.Delete(ctx, workerNetworkPolicy)
				Expect(err).NotTo(HaveOccurred(), "Failed to delete worker NetworkPolicy")
			}
		})
	})

	Describe("RayCluster Deletion", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-deletion", namespace)

		It("Create and immediately delete RayCluster", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")

			// Add deletion timestamp by deleting
			err = k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")

			// Verify RayCluster is being deleted or deleted
			Eventually(
				func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster)
					return err != nil && client.IgnoreNotFound(err) == nil
				},
				time.Second*10, time.Millisecond*500).Should(BeTrue(), "RayCluster should be deleted")
		})
	})

	Describe("Spec-Based Activation", Ordered, func() {
		ctx := context.Background()
		namespace := "default"

		It("Should NOT create NetworkPolicies when spec is missing", func() {
			rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-no-network-policy", namespace)
			rayCluster.Spec.NetworkPolicy = nil
			rayCluster.Annotations = nil

			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")

			// Wait a bit to ensure controller has time to reconcile
			time.Sleep(time.Second * 3)

			// Verify NetworkPolicies are NOT created
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			headName := rayCluster.Name + "-head"
			headKey := client.ObjectKey{Namespace: namespace, Name: headName}

			err = k8sClient.Get(ctx, headKey, headNetworkPolicy)
			Expect(err).To(HaveOccurred(), "Head NetworkPolicy should NOT exist")
			Expect(client.IgnoreNotFound(err)).To(Succeed(), "Error should be NotFound")

			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			workerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerKey := client.ObjectKey{Namespace: namespace, Name: workerName}

			err = k8sClient.Get(ctx, workerKey, workerNetworkPolicy)
			Expect(err).To(HaveOccurred(), "Worker NetworkPolicy should NOT exist")
			Expect(client.IgnoreNotFound(err)).To(Succeed(), "Error should be NotFound")

			// Cleanup
			err = k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")
		})

		It("Should create NetworkPolicies when spec is added", func() {
			rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-spec-toggle", namespace)
			rayCluster.Spec.NetworkPolicy = nil
			rayCluster.Annotations = nil

			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")

			// Wait and verify no NetworkPolicies created
			time.Sleep(time.Second * 2)

			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			headName := rayCluster.Name + "-head"
			headKey := client.ObjectKey{Namespace: namespace, Name: headName}

			err = k8sClient.Get(ctx, headKey, headNetworkPolicy)
			Expect(err).To(HaveOccurred(), "Head NetworkPolicy should NOT exist initially")

			// Now add the native NetworkPolicy configuration.
			err = k8sClient.Get(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to get RayCluster")
			utils.EnsureOpenShiftRayClusterSecurity(rayCluster)
			err = k8sClient.Update(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to update RayCluster spec")

			// Now NetworkPolicies should be created
			Eventually(
				getResourceFunc(ctx, headKey, headNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Head NetworkPolicy should be created after spec update")

			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			workerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerKey := client.ObjectKey{Namespace: namespace, Name: workerName}

			Eventually(
				getResourceFunc(ctx, workerKey, workerNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Worker NetworkPolicy should be created after spec update")

			// Cleanup
			err = k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")
		})

		It("Should delete NetworkPolicies when spec is removed", func() {
			rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-spec-removal", namespace)

			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")

			// Wait for NetworkPolicies to be created
			headNetworkPolicy := &networkingv1.NetworkPolicy{}
			headName := rayCluster.Name + "-head"
			headKey := client.ObjectKey{Namespace: namespace, Name: headName}

			Eventually(
				getResourceFunc(ctx, headKey, headNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Head NetworkPolicy should be created")

			workerNetworkPolicy := &networkingv1.NetworkPolicy{}
			workerName := workerGroupNetworkPolicyName(rayCluster.Name, rayCluster.Spec.WorkerGroupSpecs[0].GroupName)
			workerKey := client.ObjectKey{Namespace: namespace, Name: workerName}

			Eventually(
				getResourceFunc(ctx, workerKey, workerNetworkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "Worker NetworkPolicy should be created")

			// Now remove the native NetworkPolicy configuration.
			err = k8sClient.Get(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to get RayCluster")
			rayCluster.Spec.NetworkPolicy = nil
			// Keep this test independent of the downstream annotation compatibility path.
			rayCluster.Annotations = nil
			err = k8sClient.Update(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to update RayCluster spec")

			// NetworkPolicies should be deleted
			Eventually(
				func() bool {
					err := k8sClient.Get(ctx, headKey, headNetworkPolicy)
					return err != nil && client.IgnoreNotFound(err) == nil
				},
				time.Second*10, time.Millisecond*500).Should(BeTrue(), "Head NetworkPolicy should be deleted after spec update")

			Eventually(
				func() bool {
					err := k8sClient.Get(ctx, workerKey, workerNetworkPolicy)
					return err != nil && client.IgnoreNotFound(err) == nil
				},
				time.Second*10, time.Millisecond*500).Should(BeTrue(), "Worker NetworkPolicy should be deleted after spec update")

			// Cleanup
			err = k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")
		})
	})
})
