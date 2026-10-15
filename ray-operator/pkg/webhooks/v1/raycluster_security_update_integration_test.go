package v1

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// A RayService reconstructs its child spec on in-place updates. Exercise the
// actual CREATE/UPDATE admission contract, including a failing webhook, rather
// than relying on a fake client that bypasses admission.
func TestOpenShiftRayClusterSecurityAdmission(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	t.Setenv("MONITORING_NAMESPACE", "openshift-user-workload-monitoring")
	environment := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "..", "config", "webhook")},
		},
	}
	config, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = environment.Stop() })
	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	options := environment.WebhookInstallOptions
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host: options.LocalServingHost, Port: options.LocalServingPort, CertDir: options.LocalServingCertDir,
		}),
	})
	require.NoError(t, err)
	require.NoError(t, ctrl.NewWebhookManagedBy(manager, &rayv1.RayCluster{}).
		WithDefaulter(&RayClusterDefaulter{RESTMapper: &mockOpenShiftRESTMapperUnit{}}).Complete())
	require.NoError(t, SetupRayClusterWebhookWithManager(manager))
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := manager.Start(ctx); err != nil {
			t.Errorf("webhook manager: %v", err)
		}
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("webhook manager did not stop")
		}
	})
	address := fmt.Sprintf("%s:%d", options.LocalServingHost, options.LocalServingPort)
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: time.Second},
		Config:    &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // Local envtest serving certificate.
	}
	require.Eventually(t, func() bool {
		connection, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return false
		}
		_ = connection.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond)
	c, err := client.New(config, client.Options{Scheme: scheme})
	require.NoError(t, err)
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-service-child", Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: rayv1.GroupVersion.String(), Kind: "RayService", Name: "legacy-service", UID: "legacy-service-uid", Controller: new(true),
			}},
		},
		Spec: rayv1.RayClusterSpec{HeadGroupSpec: rayv1.HeadGroupSpec{RayStartParams: map[string]string{}, Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-head", Image: "rayproject/ray:latest"}}},
		}}},
	}
	require.NoError(t, c.Create(t.Context(), cluster))
	wantTLS := cluster.Spec.TLSOptions.DeepCopy()
	wantPolicy := cluster.Spec.NetworkPolicy.DeepCopy()
	require.NotNil(t, wantTLS)
	require.NotNil(t, wantPolicy)
	// Model the RayService's reconstructed child UPDATE from a parent whose
	// template does not contain the platform's defaulted fields.
	cluster.Spec.TLSOptions = nil
	cluster.Spec.NetworkPolicy = nil
	cluster.Annotations = nil
	cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{GroupName: "added-group", RayStartParams: map[string]string{}, Template: cluster.Spec.HeadGroupSpec.Template}}
	require.NoError(t, c.Update(t.Context(), cluster))
	assert.Equal(t, wantTLS, cluster.Spec.TLSOptions)
	assert.Equal(t, wantPolicy, cluster.Spec.NetworkPolicy)
	assert.Equal(t, "true", cluster.Annotations[utils.EnableSecureTrustedNetworkAnnotationKey])
	// Explicit parent settings survive admission rather than being merged with defaults.
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(false)}
	mode := rayv1.NetworkPolicyDenyAllEgress
	cluster.Spec.NetworkPolicy = &rayv1.NetworkPolicyConfig{Mode: &mode}
	require.NoError(t, c.Update(t.Context(), cluster))
	assert.False(t, *cluster.Spec.TLSOptions.Enabled)
	assert.Equal(t, &rayv1.NetworkPolicyConfig{Mode: &mode}, cluster.Spec.NetworkPolicy)
	before := cluster.DeepCopy()
	// An unavailable Fail webhook rejects the update and leaves stored security unchanged.
	stop()
	<-done
	cluster.Spec.TLSOptions = nil
	cluster.Spec.NetworkPolicy = nil
	require.ErrorContains(t, c.Update(t.Context(), cluster), "failed calling webhook")
	stored := &rayv1.RayCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(before), stored))
	assert.Equal(t, before.Spec.TLSOptions, stored.Spec.TLSOptions)
	assert.Equal(t, before.Spec.NetworkPolicy, stored.Spec.NetworkPolicy)
}
