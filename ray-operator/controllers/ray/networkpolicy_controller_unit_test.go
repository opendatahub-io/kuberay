package ray

import (
	"context"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

var (
	testScheme                   *runtime.Scheme
	testNetworkPolicyController  *NetworkPolicyController
	testRayClusterBasic          *rayv1.RayCluster
	testRayClusterDenyAllIngress *rayv1.RayCluster
	testRayClusterDenyAllEgress  *rayv1.RayCluster
	testRayClusterWithRayJob     *rayv1.RayCluster
)

func setupNetworkPolicyTest(t *testing.T) {
	t.Helper()
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	testScheme = runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(testScheme))
	require.NoError(t, corev1.AddToScheme(testScheme))
	require.NoError(t, networkingv1.AddToScheme(testScheme))
	testNetworkPolicyController = &NetworkPolicyController{
		Scheme: testScheme,
		Client: clientFake.NewClientBuilder().
			WithScheme(testScheme).
			Build(),
	}

	// Basic cluster — DenyAll mode (default).
	testRayClusterBasic = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head", Image: "rayproject/ray:latest"},
						},
					},
				},
			},
		},
	}

	// Cluster configured with DenyAllIngress mode.
	testRayClusterDenyAllIngress = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-ingress",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllIngress),
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head", Image: "rayproject/ray:latest"},
						},
					},
				},
			},
		},
	}

	// Cluster configured with DenyAllEgress mode.
	testRayClusterDenyAllEgress = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-egress",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllEgress),
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head", Image: "rayproject/ray:latest"},
						},
					},
				},
			},
		},
	}

	// Cluster owned by a RayJob (K8sJobMode — has a standalone submitter pod).
	testRayClusterWithRayJob = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-rayjob",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayJobSubmissionModeLabelKey: string(rayv1.K8sJobMode),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       "RayJob",
					Name:       "test-job",
					UID:        "12345",
					Controller: new(true),
				},
			},
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head", Image: "rayproject/ray:latest"},
						},
					},
				},
			},
		},
	}
}

// TestBuildHeadNetworkPolicy_DenyAll verifies the head NetworkPolicy in default DenyAll mode.
func TestBuildHeadNetworkPolicy_DenyAll(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterBasic, rayv1.NetworkPolicyDenyAll)

	assert.Equal(t, "test-cluster-head", policy.Name)
	assert.Equal(t, "default", policy.Namespace)

	// Labels must identify the cluster, the operator, and the head group.
	expectedLabels := map[string]string{
		utils.RayClusterLabelKey:                testRayClusterBasic.Name,
		utils.RayNodeGroupLabelKey:              utils.RayNodeHeadGroupLabelValue,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}
	assert.Equal(t, expectedLabels, policy.Labels)

	// PodSelector must target head pods of this cluster.
	assert.Equal(t, map[string]string{
		utils.RayClusterLabelKey:  "test-cluster",
		utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
	}, policy.Spec.PodSelector.MatchLabels)

	// DenyAll restricts both directions.
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)

	// 1 base ingress rule: intra-cluster only (operator access removed, injected via webhook).
	assert.Len(t, policy.Spec.Ingress, 1)

	// 1 base egress rule: intra-cluster only (DNS no longer baked in).
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildHeadNetworkPolicy_DenyAllIngress verifies that DenyAllIngress omits the Egress policy type.
func TestBuildHeadNetworkPolicy_DenyAllIngress(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterDenyAllIngress, rayv1.NetworkPolicyDenyAllIngress)

	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.NotContains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
	assert.Len(t, policy.Spec.Ingress, 1)
	assert.Empty(t, policy.Spec.Egress)
}

// TestBuildHeadNetworkPolicy_DenyAllEgress verifies that DenyAllEgress only restricts egress.
func TestBuildHeadNetworkPolicy_DenyAllEgress(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterDenyAllEgress, rayv1.NetworkPolicyDenyAllEgress)

	assert.NotContains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
	assert.Empty(t, policy.Spec.Ingress)
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildWorkerNetworkPolicy_DenyAll verifies the worker NetworkPolicy in DenyAll mode.
func TestBuildWorkerGroupNetworkPolicy_DenyAll(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(testRayClusterBasic, rayv1.NetworkPolicyDenyAll, "small-group")

	assert.Equal(t, "test-cluster-workers-small-group", policy.Name)
	assert.Equal(t, "default", policy.Namespace)

	// Labels must identify the cluster, the operator, and the worker group.
	expectedLabels := map[string]string{
		utils.RayClusterLabelKey:                testRayClusterBasic.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
		utils.RayNodeGroupLabelKey:              "small-group",
	}
	assert.Equal(t, expectedLabels, policy.Labels)

	// PodSelector must target this group's worker pods only.
	assert.Equal(t, map[string]string{
		utils.RayClusterLabelKey:   "test-cluster",
		utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
		utils.RayNodeGroupLabelKey: "small-group",
	}, policy.Spec.PodSelector.MatchLabels)

	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)

	// Workers only allow intra-cluster ingress (no Ports = all ports).
	require.Len(t, policy.Spec.Ingress, 1)
	require.Len(t, policy.Spec.Ingress[0].From, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		policy.Spec.Ingress[0].From[0].PodSelector.MatchLabels)
	assert.Empty(t, policy.Spec.Ingress[0].Ports)

	// 1 base egress rule: intra-cluster only (DNS no longer baked in).
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildWorkerGroupNetworkPolicy_DenyAllIngress verifies no egress is added for DenyAllIngress mode.
func TestBuildWorkerGroupNetworkPolicy_DenyAllIngress(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(testRayClusterDenyAllIngress, rayv1.NetworkPolicyDenyAllIngress, "small-group")

	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.NotContains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
	assert.Len(t, policy.Spec.Ingress, 1)
	assert.Empty(t, policy.Spec.Egress)
}

// TestBuildWorkerGroupNetworkPolicy_FallsBackToWorkerRules verifies that a group
// without a workerGroups entry uses the default Worker rules.
func TestBuildWorkerGroupNetworkPolicy_FallsBackToWorkerRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Worker = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcpProto, Port: &customPort},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "small-group")

	// 1 base intra-cluster + 1 custom = 2.
	require.Len(t, policy.Spec.Ingress, 2)
	require.Len(t, policy.Spec.Ingress[1].Ports, 1)
	assert.Equal(t, &customPort, policy.Spec.Ingress[1].Ports[0].Port)
}

// TestBuildWorkerGroupNetworkPolicy_ReplacesWorkerRules verifies that a workerGroups
// entry's rules replace the default Worker rules for that group.
func TestBuildWorkerGroupNetworkPolicy_ReplacesWorkerRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Worker = &rayv1.NetworkPolicyRules{
		EgressRules: []networkingv1.NetworkPolicyEgressRule{
			{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
		},
	}
	cluster.Spec.NetworkPolicy.WorkerGroups = []rayv1.WorkerGroupNetworkPolicyRules{
		{
			GroupName: "gpu-group",
			NetworkPolicyRules: rayv1.NetworkPolicyRules{
				IngressRules: []networkingv1.NetworkPolicyIngressRule{
					{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "gpu-group")

	assert.Equal(t, "test-cluster-workers-gpu-group", policy.Name)
	assert.Equal(t, map[string]string{
		utils.RayClusterLabelKey:   "test-cluster",
		utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
		utils.RayNodeGroupLabelKey: "gpu-group",
	}, policy.Spec.PodSelector.MatchLabels)

	// 1 base intra-cluster + 1 custom ingress from the group entry.
	require.Len(t, policy.Spec.Ingress, 2)
	assert.Equal(t, &customPort, policy.Spec.Ingress[1].Ports[0].Port)

	// base intra-cluster rule only.
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildBaseIngressRules verifies the shared intra-cluster ingress rule used by both head and workers.
func TestBuildBaseIngressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildBaseIngressRules(testRayClusterBasic)
	require.Len(t, rules, 1)

	intraClusterRule := rules[0]
	require.Len(t, intraClusterRule.From, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		intraClusterRule.From[0].PodSelector.MatchLabels)
	assert.Empty(t, intraClusterRule.Ports, "Intra-cluster rule must allow all ports (no Ports field)")
}

// TestBuildHeadIngressRules verifies the head base ingress rules: intra-cluster only.
// The operator access rule is intentionally absent — platforms inject it via
// spec.networkPolicy.head.ingressRules (e.g. a mutating webhook).
func TestBuildHeadIngressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildHeadIngressRules(testRayClusterBasic)
	require.Len(t, rules, 1)

	// Rule 0: intra-cluster — no ports, pod selector matching the cluster label.
	intraClusterRule := rules[0]
	require.Len(t, intraClusterRule.From, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		intraClusterRule.From[0].PodSelector.MatchLabels)
	assert.Empty(t, intraClusterRule.Ports, "Intra-cluster rule must allow all ports (no Ports field)")
}

// TestBuildRayJobPeer verifies that buildRayJobPeer returns a peer for RayJob-owned clusters
// and nil for standalone clusters.
func TestBuildRayJobPeer(t *testing.T) {
	setupNetworkPolicyTest(t)

	peer := testNetworkPolicyController.buildRayJobPeer(testRayClusterBasic)
	assert.Nil(t, peer, "Standalone cluster should not have a RayJob peer")

	peer = testNetworkPolicyController.buildRayJobPeer(testRayClusterWithRayJob)
	require.NotNil(t, peer, "K8sJobMode RayJob-owned cluster should have a RayJob peer")
	assert.Equal(t, map[string]string{
		utils.RayOriginatedFromCRNameLabelKey: "test-job",
		utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
	}, peer.PodSelector.MatchLabels)

	// Non-K8sJobMode RayJob-owned cluster: no standalone submitter pod, so no peer.
	sidecarCluster := testRayClusterWithRayJob.DeepCopy()
	sidecarCluster.Labels[utils.RayJobSubmissionModeLabelKey] = string(rayv1.SidecarMode)
	peer = testNetworkPolicyController.buildRayJobPeer(sidecarCluster)
	assert.Nil(t, peer, "Non-K8sJobMode RayJob-owned cluster should not have a RayJob peer")
}

// TestBuildHeadIngressRules_WithRayJobOwner verifies that a RayJob-owned cluster gets
// a per-job submitter ingress rule (2 rules total: intra-cluster + submitter).
func TestBuildHeadIngressRules_WithRayJobOwner(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildHeadIngressRules(testRayClusterWithRayJob)
	require.Len(t, rules, 2)

	submitterRule := rules[1]
	require.Len(t, submitterRule.From, 1)
	assert.Equal(t, map[string]string{
		utils.RayOriginatedFromCRNameLabelKey: "test-job",
		utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
	}, submitterRule.From[0].PodSelector.MatchLabels)
	require.Len(t, submitterRule.Ports, 1)
	dashboardPort := intstr.FromInt32(utils.DefaultDashboardPort)
	assert.Equal(t, &dashboardPort, submitterRule.Ports[0].Port)
}

// TestBuildBaseEgressRules verifies the single base egress rule (intra-cluster only).
func TestBuildBaseEgressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildBaseEgressRules(testRayClusterBasic)
	require.Len(t, rules, 1)

	// Rule 0: intra-cluster egress — no ports, pod selector matching the cluster label.
	intraClusterRule := rules[0]
	require.Len(t, intraClusterRule.To, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		intraClusterRule.To[0].PodSelector.MatchLabels)
	assert.Empty(t, intraClusterRule.Ports, "Intra-cluster egress must allow all ports (no Ports field)")
}

// TestBuildHeadNetworkPolicy_WithRayJob verifies that a RayJob-owned cluster gets
// 2 ingress rules: intra-cluster + per-job submitter.
func TestBuildHeadNetworkPolicy_WithRayJob(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterWithRayJob, rayv1.NetworkPolicyDenyAll)

	require.Len(t, policy.Spec.Ingress, 2)
}

// TestBuildHeadNetworkPolicy_CustomIngressRules verifies that custom Head.IngressRules are appended after base rules.
func TestBuildHeadNetworkPolicy_CustomIngressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcpProto, Port: &customPort},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)

	// 1 base intra-cluster + 1 custom = 2.
	require.Len(t, policy.Spec.Ingress, 2)
	require.Len(t, policy.Spec.Ingress[1].Ports, 1)
	assert.Equal(t, &customPort, policy.Spec.Ingress[1].Ports[0].Port)
}

func TestBuildHeadNetworkPolicyManagedOpenShiftReplacesStalePlatformPortRules(t *testing.T) {
	setupNetworkPolicyTest(t)
	testNetworkPolicyController.isOpenShift = true

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Annotations = map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"}
	cluster.Spec.NetworkPolicy = nil
	cluster.Spec.HeadGroupSpec.RayStartParams = map[string]string{"dashboard-port": "9365"}
	utils.EnsureOpenShiftRayClusterNetworkPolicy(cluster)
	oldRules := cluster.Spec.NetworkPolicy.Head.IngressRules

	cluster.Spec.HeadGroupSpec.RayStartParams["dashboard-port"] = "9465"
	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAllIngress)

	assert.NotEqual(t, oldRules, policy.Spec.Ingress, "the generated policy should not copy platform rules from the CR")
	assert.True(t, policyContainsPort(policy.Spec.Ingress, 9465))
	assert.False(t, policyContainsPort(policy.Spec.Ingress, 9365))
}

func policyContainsPort(rules []networkingv1.NetworkPolicyIngressRule, want int32) bool {
	for _, rule := range rules {
		for _, port := range rule.Ports {
			if port.Port != nil && port.Port.Type == intstr.Int && port.Port.IntVal == want {
				return true
			}
		}
	}
	return false
}

func TestNetworkPolicyUWMMetricsIngress(t *testing.T) {
	for _, tc := range []struct {
		name      string
		openShift bool
		enabled   bool
		mode      rayv1.NetworkPolicyMode
		port      int32
		wantRule  bool
	}{
		{name: "default ports", openShift: true, enabled: true, mode: rayv1.NetworkPolicyDenyAllIngress, port: 8080, wantRule: true},
		{name: "custom ports", openShift: true, enabled: true, mode: rayv1.NetworkPolicyDenyAllIngress, port: 9090, wantRule: true},
		{name: "deny all", openShift: true, enabled: true, mode: rayv1.NetworkPolicyDenyAll, port: 8080, wantRule: true},
		{name: "gate disabled", openShift: true, mode: rayv1.NetworkPolicyDenyAllIngress, port: 8080},
		{name: "not OpenShift", enabled: true, mode: rayv1.NetworkPolicyDenyAllIngress, port: 8080},
		{name: "egress only", openShift: true, enabled: true, mode: rayv1.NetworkPolicyDenyAllEgress, port: 8080},
		{name: "invalid port", openShift: true, enabled: true, mode: rayv1.NetworkPolicyDenyAllIngress, port: 70000},
		{name: "missing metrics port", openShift: true, enabled: true, mode: rayv1.NetworkPolicyDenyAllIngress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupNetworkPolicyTest(t)
			features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, tc.enabled)
			cluster := testRayClusterBasic.DeepCopy()
			cluster.UID = "metrics-ingress"
			cluster.Spec.NetworkPolicy.Mode = &tc.mode
			for _, name := range []string{"small", "large"} {
				cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{GroupName: name, Template: *cluster.Spec.HeadGroupSpec.Template.DeepCopy()})
			}
			objects := []client.Object{cluster}
			pods := []*corev1.Pod{}
			for i, group := range []string{utils.RayNodeHeadGroupLabelValue, "small", "large"} {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: group, Namespace: cluster.Namespace, Labels: map[string]string{
						utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode), utils.RayNodeGroupLabelKey: group,
					}},
					Spec: *cluster.Spec.HeadGroupSpec.Template.Spec.DeepCopy(),
				}
				if i == 0 {
					pod.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.HeadNode)
				}
				pod.Spec.Containers[0].Ports = nil
				if tc.port != 0 {
					pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: tc.port + int32(i)}}
				}
				require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
				pods = append(pods, pod)
				objects = append(objects, pod)
			}
			r := &NetworkPolicyController{
				Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: tc.openShift,
				Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(objects...).Build(),
			}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.NoError(t, err)
			all := &networkingv1.NetworkPolicyList{}
			require.NoError(t, r.List(t.Context(), all))
			wantPolicies := 3
			if tc.wantRule {
				wantPolicies += 3
			}
			require.Len(t, all.Items, wantPolicies)
			for i, pod := range pods {
				if !tc.wantRule {
					assert.Empty(t, uwmMetricsPortsForPod(t, r.Client, pod))
					continue
				}
				wantPort := tc.port + int32(i)
				assert.Equal(t, []int32{wantPort}, uwmMetricsPortsForPod(t, r.Client, pod))
				baseName := workerGroupNetworkPolicyName(cluster.Name, pod.Labels[utils.RayNodeGroupLabelKey])
				if i == 0 {
					baseName = headNetworkPolicyName(cluster.Name)
				}
				base := &networkingv1.NetworkPolicy{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: baseName}, base))
				assert.Empty(t, uwmMetricsPorts(base), "base isolation must not grant group-wide metrics access")
				assert.Equal(t, r.buildBaseIngressRules(cluster), base.Spec.Ingress, "base policies must not include namespace-only, multi-peer, or all-port monitoring grants")
				policy := &networkingv1.NetworkPolicy{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: uwmMetricsPolicyName(cluster, base, fmt.Sprint(wantPort))}, policy))
				require.Len(t, policy.Spec.Ingress, 1)
				assert.Equal(t, networkingv1.NetworkPolicyIngressRule{
					From: []networkingv1.NetworkPolicyPeer{{
						NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "openshift-user-workload-monitoring"}},
						PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "prometheus", "app.kubernetes.io/instance": "user-workload"}},
					}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(wantPort))}},
				}, policy.Spec.Ingress[0])
			}
		})
	}
}

func TestNetworkPolicyUWMLivePodPorts(t *testing.T) {
	setupNetworkPolicyTest(t)
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	cluster := testRayClusterBasic.DeepCopy()
	cluster.UID = "cluster-uid"
	cluster.Spec.UpgradeStrategy = &rayv1.RayClusterUpgradeStrategy{Type: new(rayv1.RayClusterUpgradeNone)}
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 9090}}
	cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{GroupName: "small", Template: *cluster.Spec.HeadGroupSpec.Template.DeepCopy()}}
	pods := []*corev1.Pod{}
	for _, role := range []rayv1.RayNodeType{rayv1.HeadNode, rayv1.WorkerNode} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: string(role), Namespace: cluster.Namespace, Labels: map[string]string{
				utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(role), utils.RayNodeGroupLabelKey: "small",
			}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 8080}}}}},
		}
		require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
		pods = append(pods, pod)
	}
	foreign := pods[0].DeepCopy()
	foreign.Name, foreign.OwnerReferences = "foreign", nil
	foreign.Spec.Containers[0].Ports[0].ContainerPort = 9999
	finished := pods[0].DeepCopy()
	finished.Name, finished.Status.Phase = "finished", corev1.PodFailed
	finished.Spec.Containers[0].Ports[0].ContainerPort = 9998
	r := &NetworkPolicyController{
		Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true,
		Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, pods[0], pods[1], foreign, finished).Build(),
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	for _, pod := range pods {
		assert.Equal(t, []int32{8080}, uwmMetricsPortsForPod(t, r.Client, pod))
		require.NoError(t, r.Delete(t.Context(), pod))
	}
	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	policies := &networkingv1.NetworkPolicyList{}
	require.NoError(t, r.List(t.Context(), policies))
	require.Len(t, policies.Items, 2, "only the head and worker isolation policies remain")
	for _, policy := range policies.Items {
		assert.Empty(t, uwmMetricsPorts(&policy))
	}
}

func TestNetworkPolicyUWMRetainedWorkerGroup(t *testing.T) {
	setupNetworkPolicyTest(t)
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	cluster := testRayClusterBasic.DeepCopy()
	cluster.UID = "retained-worker-group"
	cluster.Spec.UpgradeStrategy = &rayv1.RayClusterUpgradeStrategy{Type: new(rayv1.RayClusterUpgradeNone)}
	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "removed")
	policy.Annotations = map[string]string{"user-note": "keep"}
	customRule := networkingv1.NetworkPolicyIngressRule{Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(7777))}}}
	policy.Spec.Ingress = append(policy.Spec.Ingress, customRule)
	require.NoError(t, controllerutil.SetControllerReference(cluster, policy, testScheme))
	original := policy.DeepCopy()
	foreign := policy.DeepCopy()
	foreign.Name, foreign.OwnerReferences = "foreign-policy", nil
	foreign.Labels[uwmMetricsPortLabel] = "9999"
	pods := []*corev1.Pod{}
	for i, port := range []int32{8080, 9090} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("retained-%d", i), Namespace: cluster.Namespace, Labels: policy.Spec.PodSelector.MatchLabels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: port}}}}},
		}
		require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
		pods = append(pods, pod)
	}
	r := &NetworkPolicyController{
		Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true,
		Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, policy, foreign, pods[0], pods[1]).Build(),
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	checkPorts := func(want ...int32) *networkingv1.NetworkPolicy {
		result, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		assert.Equal(t, legacyNetworkPolicyMigrationRetryInterval, result.RequeueAfter)
		got := &networkingv1.NetworkPolicy{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(policy), got))
		policies := &networkingv1.NetworkPolicyList{}
		require.NoError(t, r.List(t.Context(), policies, client.MatchingLabels{utils.RayNodeGroupLabelKey: "removed"}))
		var ports []int32
		for _, item := range policies.Items {
			ports = append(ports, uwmMetricsPorts(&item)...)
		}
		assert.ElementsMatch(t, want, ports)
		assert.Equal(t, original.Annotations, got.Annotations)
		assert.Equal(t, original.OwnerReferences, got.OwnerReferences)
		assert.Equal(t, original.Spec, got.Spec, "retained isolation rules must not change")
		return got
	}
	first := checkPorts(8080, 9090)
	assert.Equal(t, first.ResourceVersion, checkPorts(8080, 9090).ResourceVersion, "unchanged retained policies must not be rewritten")
	require.NoError(t, r.Delete(t.Context(), pods[0]))
	checkPorts(9090)
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, false)
	checkPorts()
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	checkPorts(9090)
	require.NoError(t, r.Delete(t.Context(), pods[1]))
	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.True(t, errors.IsNotFound(r.Get(t.Context(), client.ObjectKeyFromObject(policy), &networkingv1.NetworkPolicy{})))
	gotForeign := &networkingv1.NetworkPolicy{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(foreign), gotForeign))
	assert.Equal(t, foreign.Spec, gotForeign.Spec)
}

func TestNetworkPolicyUWMPortsAreScopedToPods(t *testing.T) {
	setupNetworkPolicyTest(t)
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	cluster := testRayClusterBasic.DeepCopy()
	cluster.UID = "port-transition"
	cluster.Spec.UpgradeStrategy = &rayv1.RayClusterUpgradeStrategy{Type: new(rayv1.RayClusterUpgradeNone)}
	cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{GroupName: "small", Template: *cluster.Spec.HeadGroupSpec.Template.DeepCopy()}}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 9090}}
	// A group name must not collide with another group's metrics policy.
	cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{GroupName: "small-metrics-8080", Template: *cluster.Spec.HeadGroupSpec.Template.DeepCopy()})
	pods := []*corev1.Pod{}
	for i, port := range []int32{8080, 9090} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("worker-%d", i), Namespace: cluster.Namespace, Labels: map[string]string{
				utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode), utils.RayNodeGroupLabelKey: "small",
			}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "ray-worker", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: port}}},
				{Name: "application", Ports: []corev1.ContainerPort{{Name: "app", ContainerPort: 17170 - port}}},
			}},
		}
		require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
		pods = append(pods, pod)
	}
	r := &NetworkPolicyController{
		Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true,
		Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, pods[0], pods[1]).Build(),
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	for _, removed := range []bool{false, true} {
		if removed {
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
			cluster.Spec.WorkerGroupSpecs = nil
			require.NoError(t, r.Update(t.Context(), cluster))
		}
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		for _, pod := range pods {
			assert.Equal(t, []int32{pod.Spec.Containers[0].Ports[0].ContainerPort}, uwmMetricsPortsForPod(t, r.Client, pod), "the other Pod's metrics port is an application listener here")
		}
		before := &networkingv1.NetworkPolicyList{}
		require.NoError(t, r.List(t.Context(), before))
		_, err = r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		for _, policy := range before.Items {
			got := &networkingv1.NetworkPolicy{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(&policy), got))
			assert.Equal(t, policy.ResourceVersion, got.ResourceVersion, "unchanged policies must not be rewritten")
		}
	}
}

func TestNetworkPolicyUWMMetricsFailurePreservesIsolation(t *testing.T) {
	for _, failure := range []string{"patch", "create"} {
		t.Run(failure, func(t *testing.T) {
			setupNetworkPolicyTest(t)
			features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
			cluster := testRayClusterBasic.DeepCopy()
			cluster.UID = "metrics-failure"
			for _, group := range []string{"small", "large"} {
				cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{GroupName: group})
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "head", Namespace: cluster.Namespace, Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.HeadNode)}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 8080}}}}},
			}
			require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
			unvisited := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "unvisited-metrics", Namespace: cluster.Namespace, Labels: networkPolicyLabels(cluster, "old")}}
			unvisited.Labels[uwmMetricsPortLabel] = "8080"
			require.NoError(t, controllerutil.SetControllerReference(cluster, unvisited, testScheme))
			c := clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, pod, unvisited).WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if failure == "patch" {
						return fmt.Errorf("metrics denied")
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if failure == "create" && obj.GetLabels()[uwmMetricsPortLabel] != "" {
						return fmt.Errorf("metrics denied")
					}
					return c.Create(ctx, obj, opts...)
				},
			}).Build()
			r := &NetworkPolicyController{Client: c, Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.ErrorContains(t, err, "metrics denied")
			for _, name := range []string{headNetworkPolicyName(cluster.Name), workerGroupNetworkPolicyName(cluster.Name, "small"), workerGroupNetworkPolicyName(cluster.Name, "large"), unvisited.Name} {
				require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: name}, &networkingv1.NetworkPolicy{}), "metrics failures must not remove or prevent base isolation")
			}
		})
	}
}

func TestNetworkPolicyUWMCrossClusterNames(t *testing.T) {
	for _, order := range [][]string{{"a", "a-metrics"}, {"a-metrics", "a"}} {
		t.Run(strings.Join(order, "/"), func(t *testing.T) {
			setupNetworkPolicyTest(t)
			features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
			clusters := map[string]*rayv1.RayCluster{}
			pods := map[string]*corev1.Pod{}
			objects := []client.Object{}
			for _, name := range []string{"a", "a-metrics"} {
				cluster := testRayClusterBasic.DeepCopy()
				cluster.Name, cluster.UID = name, types.UID(name+"-uid")
				group := "small"
				if name == "a-metrics" {
					group = "small-8080"
				}
				cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{GroupName: group}}
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: map[string]string{utils.RayClusterLabelKey: name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode), utils.RayNodeGroupLabelKey: group}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 8080}}}}},
				}
				require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
				clusters[name], pods[name] = cluster, pod
				objects = append(objects, cluster, pod)
			}
			r := &NetworkPolicyController{Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true, Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(objects...).Build()}
			for _, name := range order {
				_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(clusters[name])})
				require.NoError(t, err)
			}
			for name, pod := range pods {
				assert.Equal(t, []int32{8080}, uwmMetricsPortsForPod(t, r.Client, pod))
				base := &networkingv1.NetworkPolicy{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: pod.Namespace, Name: workerGroupNetworkPolicyName(name, pod.Labels[utils.RayNodeGroupLabelKey])}, base))
				assert.True(t, metav1.IsControlledBy(base, clusters[name]))
				assert.Empty(t, uwmMetricsPorts(base))
			}
		})
	}
}

func TestNetworkPolicyUWMRemovesIneligiblePodLabel(t *testing.T) {
	setupNetworkPolicyTest(t)
	features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, true)
	cluster := testRayClusterBasic.DeepCopy()
	cluster.UID = "owner-change"
	cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{GroupName: "small"}}
	objects := []client.Object{cluster}
	pods := []*corev1.Pod{}
	for _, name := range []string{"worker-a", "worker-b"} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode), utils.RayNodeGroupLabelKey: "small"}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 8080}}}}},
		}
		require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
		objects = append(objects, pod)
		pods = append(pods, pod)
	}
	r := &NetworkPolicyController{Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true, Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(objects...).Build()}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	legacy := &networkingv1.NetworkPolicyList{}
	require.NoError(t, r.List(t.Context(), legacy))
	for _, policy := range legacy.Items {
		if policy.Labels[uwmMetricsPortLabel] == "" {
			continue
		}
		policy.Name, policy.ResourceVersion, policy.UID = cluster.Name+"-metrics-workers-small-8080", "", ""
		require.NoError(t, r.Create(t.Context(), &policy))
	}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pods[0]), pods[0]))
	pods[0].OwnerReferences = nil
	require.NoError(t, r.Update(t.Context(), pods[0]))
	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pods[0]), pods[0]))
	assert.NotContains(t, pods[0].Labels, uwmMetricsPortLabel)
	assert.Empty(t, uwmMetricsPortsForPod(t, r.Client, pods[0]))
	assert.Equal(t, []int32{8080}, uwmMetricsPortsForPod(t, r.Client, pods[1]))
	assert.True(t, errors.IsNotFound(r.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-metrics-workers-small-8080"}, &networkingv1.NetworkPolicy{})))
}

func TestNetworkPolicyUWMLiveHeadDashboardPorts(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			setupNetworkPolicyTest(t)
			features.SetFeatureGateDuringTest(t, features.RayClusterPodMonitor, enabled)
			cluster := testRayClusterBasic.DeepCopy()
			cluster.UID = "head-transition"
			cluster.Annotations = map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"}
			cluster.Spec.UpgradeStrategy = &rayv1.RayClusterUpgradeStrategy{Type: new(rayv1.RayClusterUpgradeNone)}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "head-a", Namespace: cluster.Namespace, Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.HeadNode)}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Ports: []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 8080}, {Name: utils.DashboardPortName, ContainerPort: 8265}}}}},
			}
			require.NoError(t, controllerutil.SetControllerReference(cluster, pod, testScheme))
			r := &NetworkPolicyController{Scheme: testScheme, Recorder: &events.FakeRecorder{}, isOpenShift: true, Client: clientFake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, pod).Build()}
			for _, ports := range [][2]int32{{9090, 8080}, {9191, 8080}, {9292, 9191}} {
				require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
				cluster.Spec.HeadGroupSpec.RayStartParams = map[string]string{"metrics-export-port": fmt.Sprint(ports[0]), "dashboard-port": fmt.Sprint(ports[1])}
				cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: ports[0]}, {Name: utils.DashboardPortName, ContainerPort: ports[1]}}
				cluster.Spec.NetworkPolicy = nil
				utils.EnsureOpenShiftRayClusterNetworkPolicy(cluster)
				require.NoError(t, r.Update(t.Context(), cluster))
				before := cluster.Spec.DeepCopy()
				_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
				require.NoError(t, err)
				policy := &networkingv1.NetworkPolicy{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: headNetworkPolicyName(cluster.Name)}, policy))
				assert.True(t, policyContainsPort(policy.Spec.Ingress, 8265))
				assert.False(t, policyContainsPort(policy.Spec.Ingress, 8080), "desired dashboard must not expose the actual old metrics listener")
				require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
				assert.Equal(t, before, &cluster.Spec)
			}
			other := pod.DeepCopy()
			other.Name = "head-b"
			other.ResourceVersion, other.UID = "", ""
			other.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: utils.MetricsPortName, ContainerPort: 8265}, {Name: utils.DashboardPortName, ContainerPort: 8080}}
			require.NoError(t, r.Create(t.Context(), other))
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.NoError(t, err)
			policy := &networkingv1.NetworkPolicy{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: headNetworkPolicyName(cluster.Name)}, policy))
			assert.False(t, policyContainsPort(policy.Spec.Ingress, 8080))
			assert.False(t, policyContainsPort(policy.Spec.Ingress, 8265))
			assert.True(t, policyContainsPort(policy.Spec.Ingress, 10001))
		})
	}
}

func uwmMetricsPortsForPod(t *testing.T, c client.Client, pod *corev1.Pod) []int32 {
	t.Helper()
	ports, err := readUWMMetricsPortsForPod(t.Context(), c, pod)
	require.NoError(t, err)
	return ports
}

func readUWMMetricsPortsForPod(ctx context.Context, c client.Client, pod *corev1.Pod) ([]int32, error) {
	current := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
		return nil, err
	}
	policies := &networkingv1.NetworkPolicyList{}
	if err := c.List(ctx, policies, client.InNamespace(pod.Namespace)); err != nil {
		return nil, err
	}
	var ports []int32
	for _, policy := range policies.Items {
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			return nil, err
		}
		if selector.Matches(labels.Set(current.Labels)) {
			ports = append(ports, uwmMetricsPorts(&policy)...)
		}
	}
	return ports, nil
}

func uwmMetricsPorts(policy *networkingv1.NetworkPolicy) []int32 {
	var ports []int32
	for _, rule := range policy.Spec.Ingress {
		if len(rule.From) == 1 && rule.From[0].PodSelector != nil && rule.From[0].PodSelector.MatchLabels["app.kubernetes.io/instance"] == "user-workload" {
			for _, port := range rule.Ports {
				ports = append(ports, port.Port.IntVal)
			}
		}
	}
	return ports
}

// TestBuildHeadNetworkPolicy_CustomEgressRules verifies that custom Head.EgressRules are appended after base egress.
func TestBuildHeadNetworkPolicy_CustomEgressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(8080)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{
		EgressRules: []networkingv1.NetworkPolicyEgressRule{
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcpProto, Port: &customPort},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)

	// 1 base egress + 1 custom = 2.
	require.Len(t, policy.Spec.Egress, 2)
	require.Len(t, policy.Spec.Egress[1].Ports, 1)
	assert.Equal(t, &customPort, policy.Spec.Egress[1].Ports[0].Port)
}

// TestGetHeadPort_DefaultFallback verifies the default port is returned when rayStartParams has no override.
func TestGetHeadPort_DefaultFallback(t *testing.T) {
	setupNetworkPolicyTest(t)

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.HeadGroupSpec.RayStartParams = map[string]string{}

	port := testNetworkPolicyController.getHeadPort(cluster, "dashboard-port", utils.DefaultDashboardPort)
	assert.Equal(t, int32(utils.DefaultDashboardPort), port)
}

// TestGetHeadPort_CustomPort verifies that a port set via rayStartParams is returned.
func TestGetHeadPort_CustomPort(t *testing.T) {
	setupNetworkPolicyTest(t)

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.HeadGroupSpec.RayStartParams = map[string]string{
		"dashboard-port": "9265",
	}

	port := testNetworkPolicyController.getHeadPort(cluster, "dashboard-port", utils.DefaultDashboardPort)
	assert.Equal(t, int32(9265), port)
}

// TestBuildNetworkPolicy_LongClusterName verifies NetworkPolicy names are constructed correctly for long names.
func TestBuildNetworkPolicy_LongClusterName(t *testing.T) {
	setupNetworkPolicyTest(t)

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Name = strings.Repeat("a", utils.MaxRayClusterNameLength)

	headPolicy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)
	workerPolicy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "small-group")

	assert.Equal(t, cluster.Name+"-head", headPolicy.Name)
	assert.Equal(t, cluster.Name+"-workers-small-group", workerPolicy.Name)
}

// TestBuildHeadNetworkPolicy_WorkerRulesNotLeaked verifies that worker-specific
// IngressRules are not applied to the head NetworkPolicy.
func TestBuildHeadNetworkPolicy_WorkerRulesNotLeaked(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Worker = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
		},
	}

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)

	// Head NP must only have the intra-cluster base rule — no worker rules.
	require.Len(t, policy.Spec.Ingress, 1)
}

// TestBuildWorkerGroupNetworkPolicy_HeadRulesNotLeaked verifies that head-specific
// IngressRules are not applied to the worker NetworkPolicy.
func TestBuildWorkerGroupNetworkPolicy_HeadRulesNotLeaked(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
		},
	}

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "small-group")

	// Worker NP must only have the intra-cluster base rule — no head rules.
	require.Len(t, policy.Spec.Ingress, 1)
}

// TestDeleteStaleNetworkPolicies verifies that only NetworkPolicies that are both
// stale (not in desiredNames) and owned by the RayCluster are deleted. This is what
// removes a worker group's NetworkPolicy after the group is dropped from the spec.
func TestDeleteStaleNetworkPolicies(t *testing.T) {
	setupNetworkPolicyTest(t)

	instance := testRayClusterBasic.DeepCopy()
	instance.UID = "test-uid"

	newNetworkPolicy := func(name string, owned bool) *networkingv1.NetworkPolicy {
		networkPolicy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: instance.Namespace,
				Labels:    networkPolicyLabels(instance, "some-group"),
			},
		}
		if owned {
			require.NoError(t, controllerutil.SetControllerReference(instance, networkPolicy, testScheme))
		}
		return networkPolicy
	}

	desired := newNetworkPolicy("test-cluster-workers-small-group", true)
	stale := newNetworkPolicy("test-cluster-workers-removed-group", true)
	foreign := newNetworkPolicy("test-cluster-workers-foreign-group", false)

	controller := &NetworkPolicyController{
		Scheme:   testScheme,
		Recorder: &events.FakeRecorder{},
		Client: clientFake.NewClientBuilder().
			WithScheme(testScheme).
			WithObjects(desired, stale, foreign).
			Build(),
	}

	_, err := controller.deleteStaleNetworkPolicies(t.Context(), instance, map[string]bool{desired.Name: true}, false)
	require.NoError(t, err)

	remaining := &networkingv1.NetworkPolicyList{}
	require.NoError(t, controller.List(t.Context(), remaining, client.InNamespace(instance.Namespace)))

	names := make([]string, 0, len(remaining.Items))
	for _, item := range remaining.Items {
		names = append(names, item.Name)
	}
	// The stale owned policy is gone; the desired one and the unowned one survive.
	assert.ElementsMatch(t, []string{desired.Name, foreign.Name}, names)
}

// TestNetworkPolicyControllerPreservesLegacyPoliciesAfterMigrationWindow verifies that
// a delayed legacy-security migration never removes the policies that are still
// providing isolation for an upgraded OpenShift RayCluster.
func TestNetworkPolicyControllerPreservesLegacyPoliciesAfterMigrationWindow(t *testing.T) {
	setupNetworkPolicyTest(t)

	instance := testRayClusterBasic.DeepCopy()
	instance.Name = "legacy-cluster"
	instance.UID = "legacy-cluster-uid"
	instance.Spec.NetworkPolicy = nil
	instance.Annotations = map[string]string{
		utils.EnableSecureTrustedNetworkAnnotationKey: "true",
	}

	legacyPolicy := func(name, group string) *networkingv1.NetworkPolicy {
		policy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: instance.Namespace,
				Labels:    networkPolicyLabels(instance, group),
			},
		}
		require.NoError(t, controllerutil.SetControllerReference(instance, policy, testScheme))
		return policy
	}

	headPolicy := legacyPolicy(headNetworkPolicyName(instance.Name), utils.RayNodeHeadGroupLabelValue)
	workerPolicy := legacyPolicy(instance.Name+"-workers", "")
	controller := &NetworkPolicyController{
		Client: clientFake.NewClientBuilder().
			WithScheme(testScheme).
			WithObjects(instance, headPolicy, workerPolicy).
			Build(),
		Scheme:      testScheme,
		Recorder:    &events.FakeRecorder{},
		isOpenShift: true,
	}

	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(instance)}
	for attempt := 0; attempt <= legacyNetworkPolicyMigrationMaxRetries; attempt++ {
		result, err := controller.Reconcile(t.Context(), request)
		require.NoError(t, err)
		if attempt < legacyNetworkPolicyMigrationMaxRetries {
			assert.Equal(t, legacyNetworkPolicyMigrationRequeueInterval, result.RequeueAfter)
		} else {
			assert.Equal(t, legacyNetworkPolicyMigrationRetryInterval, result.RequeueAfter)
		}
	}

	for _, expected := range []*networkingv1.NetworkPolicy{headPolicy, workerPolicy} {
		got := &networkingv1.NetworkPolicy{}
		require.NoError(t, controller.Get(t.Context(), client.ObjectKeyFromObject(expected), got))
		assert.Equal(t, expected.Name, got.Name)
	}
}
