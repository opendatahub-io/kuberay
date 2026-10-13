package ray

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestLegacySecurityMigrationWaitsForCRDUpgrade(t *testing.T) {
	scheme, cluster, policies := legacyNetworkPolicyFixture(t)
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "ray.io_rayclusters.yaml"))
	require.NoError(t, err)
	currentCRD := &apiextensionsv1.CustomResourceDefinition{}
	require.NoError(t, yaml.Unmarshal(data, currentCRD))
	legacyCRD := currentCRD.DeepCopy()
	// Model the relevant 1.6.2 schema behavior: neither native security field
	// is known, so the API server prunes the migration patch without an error.
	for i := range legacyCRD.Spec.Versions {
		spec := legacyCRD.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties["spec"]
		delete(spec.Properties, "networkPolicy")
		delete(spec.Properties, "tlsOptions")
	}
	environment := &envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{legacyCRD}}
	config, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = environment.Stop() })
	c, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	cluster.UID = ""
	require.NoError(t, c.Create(t.Context(), cluster))
	clusterUID := cluster.UID
	for _, policy := range policies {
		policy.UID = ""
		policy.OwnerReferences = nil
		require.NoError(t, controllerutil.SetControllerReference(cluster, policy, scheme))
		require.NoError(t, c.Create(t.Context(), policy))
	}
	migration := &LegacySecurityController{Client: c, isOpenShift: true}
	network := &NetworkPolicyController{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(100), isOpenShift: true}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	for attempt := 0; attempt <= legacyNetworkPolicyMigrationMaxRetries; attempt++ {
		result, err := migration.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.Positive(t, result.RequeueAfter, "a successful but pruned patch must remain retryable")
		result, err = network.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.Positive(t, result.RequeueAfter)
		got := &rayv1.RayCluster{}
		require.NoError(t, c.Get(t.Context(), request.NamespacedName, got))
		assert.Nil(t, got.Spec.TLSOptions)
		assert.Nil(t, got.Spec.NetworkPolicy)
		for _, policy := range policies {
			stored := &networkingv1.NetworkPolicy{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policy), stored))
			assert.Equal(t, policy.UID, stored.UID)
			assert.Equal(t, policy.Spec, stored.Spec)
		}
	}
	installedCRD := &apiextensionsv1.CustomResourceDefinition{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: currentCRD.Name}, installedCRD))
	installedCRD.Spec = currentCRD.Spec
	require.NoError(t, c.Update(t.Context(), installedCRD))
	// Retry the same objects; no deletion/recreation or operator restart.
	ctx := t.Context()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, err := migration.Reconcile(ctx, request)
		assert.NoError(collect, err)
		got := &rayv1.RayCluster{}
		if assert.NoError(collect, c.Get(ctx, request.NamespacedName, got)) {
			assert.NotNil(collect, got.Spec.NetworkPolicy)
			assert.NotNil(collect, got.Spec.TLSOptions)
		}
	}, 10*time.Second, 100*time.Millisecond)
	result, err := migration.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "migration stops retrying once the fields persist")
	_, err = network.Reconcile(t.Context(), request)
	require.NoError(t, err)
	stored := &rayv1.RayCluster{}
	require.NoError(t, c.Get(t.Context(), request.NamespacedName, stored))
	assert.Equal(t, clusterUID, stored.UID)
	head := &networkingv1.NetworkPolicy{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(policies[0]), head))
	assert.Equal(t, policies[0].UID, head.UID)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-workers-small-group"}, &networkingv1.NetworkPolicy{}))
	assert.True(t, apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(policies[1]), &networkingv1.NetworkPolicy{})))
}
