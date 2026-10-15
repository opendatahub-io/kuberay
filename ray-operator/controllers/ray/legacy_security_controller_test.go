package ray

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func legacySecurityScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(rayv1.GroupVersion, &rayv1.RayCluster{}, &rayv1.RayClusterList{})
	metav1.AddToGroupVersion(scheme, rayv1.GroupVersion)
	return scheme
}

func TestLegacySecurityControllerAnnotationValues(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	for _, value := range []string{"true", "True", "TRUE", "1", "yes", "Yes", "YES", "on", "On", "ON"} {
		t.Run(value, func(t *testing.T) {
			cluster := legacySecurityCluster("legacy", map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: value})
			c := fake.NewClientBuilder().WithScheme(legacySecurityScheme(t)).WithObjects(cluster).Build()
			r := &LegacySecurityController{Client: c, isOpenShift: true}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.NoError(t, err)
			got := &rayv1.RayCluster{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), got))
			require.NotNil(t, got.Spec.NetworkPolicy, "all legacy enabled values must migrate")
			require.NotNil(t, got.Spec.TLSOptions)
			require.True(t, *got.Spec.TLSOptions.Enabled)
		})
	}
}

func legacySecurityCluster(name string, annotations map[string]string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{Template: corev1.PodTemplateSpec{}},
		},
	}
}

func TestLegacySecurityControllerMigratesLegacyAnnotation(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	t.Setenv("MONITORING_NAMESPACE", "openshift-user-workload-monitoring")
	cluster := legacySecurityCluster("legacy", map[string]string{
		utils.EnableSecureTrustedNetworkAnnotationKey: "true",
	})
	c := fake.NewClientBuilder().WithScheme(legacySecurityScheme(t)).WithObjects(cluster).Build()
	r := &LegacySecurityController{Client: c, isOpenShift: true}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	if err != nil {
		t.Fatal(err)
	}

	got := &rayv1.RayCluster{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cluster), got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.TLSOptions == nil || got.Spec.TLSOptions.Enabled == nil || !*got.Spec.TLSOptions.Enabled {
		t.Fatalf("legacy annotation did not enable native TLSOptions: %#v", got.Spec.TLSOptions)
	}
	if got.Spec.NetworkPolicy == nil || got.Spec.NetworkPolicy.Mode == nil || *got.Spec.NetworkPolicy.Mode != rayv1.NetworkPolicyDenyAllIngress {
		t.Fatalf("legacy annotation did not create native NetworkPolicy: %#v", got.Spec.NetworkPolicy)
	}
}

func TestLegacySecurityControllerPreservesExplicitNativeFields(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	mode := rayv1.NetworkPolicyDenyAllEgress
	explicit := legacySecurityCluster("explicit", map[string]string{
		utils.EnableSecureTrustedNetworkAnnotationKey: "true",
	})
	explicit.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(false)}
	explicit.Spec.NetworkPolicy = &rayv1.NetworkPolicyConfig{Mode: &mode}
	explicit.Spec.HeadGroupSpec.EnableIngress = new(true)
	want := explicit.Spec.DeepCopy()

	c := fake.NewClientBuilder().WithScheme(legacySecurityScheme(t)).WithObjects(explicit).Build()
	r := &LegacySecurityController{Client: c, isOpenShift: true}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(explicit)})
	if err != nil {
		t.Fatal(err)
	}

	got := &rayv1.RayCluster{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(explicit), got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, &got.Spec) {
		t.Fatalf("explicit native fields changed: want=%#v got=%#v", want, got.Spec)
	}
}

func TestLegacySecurityControllerIgnoresClustersWithoutLegacyAnnotation(t *testing.T) {
	cluster := legacySecurityCluster("unannotated", nil)
	c := fake.NewClientBuilder().WithScheme(legacySecurityScheme(t)).WithObjects(cluster).Build()
	r := &LegacySecurityController{Client: c}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	if err != nil {
		t.Fatal(err)
	}

	got := &rayv1.RayCluster{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cluster), got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.TLSOptions != nil || got.Spec.NetworkPolicy != nil {
		t.Fatalf("unannotated cluster was changed: %#v", got.Spec)
	}
}

func TestLegacySecurityControllerSkipsIneligibleClusters(t *testing.T) {
	for _, scenario := range []string{"mTLS disabled", "NetworkPolicy disabled", "annotation disabled", "deleting", "externally managed"} {
		t.Run(scenario, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, scenario != "mTLS disabled")
			features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, scenario != "NetworkPolicy disabled")
			cluster := legacySecurityCluster("legacy", map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"})
			switch scenario {
			case "annotation disabled":
				cluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey] = "false"
			case "deleting":
				now := metav1.NewTime(time.Now())
				cluster.DeletionTimestamp = &now
				cluster.Finalizers = []string{"test.example.com/hold"}
			case "externally managed":
				cluster.Spec.ManagedBy = new("kueue.x-k8s.io/multikueue")
			}
			want := cluster.DeepCopy()
			c := fake.NewClientBuilder().WithScheme(legacySecurityScheme(t)).WithObjects(cluster).Build()
			r := &LegacySecurityController{Client: c, isOpenShift: true}
			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.NoError(t, err)
			assert.Zero(t, result.RequeueAfter)
			got := &rayv1.RayCluster{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), got))
			assert.Equal(t, want.Spec, got.Spec)
			assert.Equal(t, want.Annotations, got.Annotations)
		})
	}
}
