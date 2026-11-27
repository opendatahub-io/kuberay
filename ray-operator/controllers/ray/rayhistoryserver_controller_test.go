package ray

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

func historyServerTestCR() *rayv1alpha1.RayHistoryServer {
	return &rayv1alpha1.RayHistoryServer{
		ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "history-server-uid", Generation: 3},
		Spec: rayv1alpha1.RayHistoryServerSpec{Storage: &rayv1alpha1.HistoryServerStorage{
			Type: rayv1alpha1.HistoryServerStorageS3, S3: &rayv1alpha1.HistoryServerS3Storage{Bucket: "history", Region: "eu-west-1"},
		}},
	}
}

func historyServerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, rayv1alpha1.AddToScheme(s))
	require.NoError(t, certmanagerv1.AddToScheme(s))
	return s
}

func historyServerTestClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(historyServerTestScheme(t)).WithObjects(objects...).WithStatusSubresource(&rayv1alpha1.RayHistoryServer{}, &appsv1.Deployment{}).Build()
}

func historyServerTestController(c client.Client) *RayHistoryServerController {
	return &RayHistoryServerController{Client: c, APIReader: c, Scheme: c.Scheme(), Namespace: "operator", Image: "history:test", ProxyImage: "proxy:test"}
}

func historyServerTestCondition(t *testing.T, hs *rayv1alpha1.RayHistoryServer, kind rayv1alpha1.RayHistoryServerConditionType, status metav1.ConditionStatus) metav1.Condition {
	t.Helper()
	condition := meta.FindStatusCondition(hs.Status.Conditions, string(kind))
	require.NotNil(t, condition, "missing %s condition", kind)
	require.Equal(t, status, condition.Status, "%s: %s", kind, condition.Message)
	require.Equal(t, hs.Generation, condition.ObservedGeneration)
	return *condition
}

func TestHistoryServerAbsentDoesNoWork(t *testing.T) {
	ctx := context.Background()
	c := historyServerTestClient(t)
	gets := 0
	r := historyServerTestController(interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			gets++
			require.IsType(t, &rayv1alpha1.RayHistoryServer{}, obj)
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			t.Fatal("unexpected Create")
			return nil
		},
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			t.Fatal("unexpected Patch")
			return nil
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			t.Fatal("unexpected List")
			return nil
		},
	}))
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "default"}})
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	require.Equal(t, 1, gets)
}

func TestHistoryServerInvalidConfigurationPersistsFinalizerAndStatus(t *testing.T) {
	ctx := context.Background()
	hs := historyServerTestCR()
	hs.Spec.Storage = nil
	c := historyServerTestClient(t, hs)
	r := historyServerTestController(c)
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hs)})
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(hs), hs))
	require.Contains(t, hs.Finalizers, historyServerFinalizer)
	historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerConfigurationValid, metav1.ConditionFalse)
	historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerReady, metav1.ConditionFalse)
	historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerDegraded, metav1.ConditionTrue)
	require.Equal(t, hs.Generation, hs.Status.ObservedGeneration)
	deployments := &appsv1.DeploymentList{}
	require.NoError(t, c.List(ctx, deployments))
	require.Empty(t, deployments.Items)
}

func TestHistoryServerOwnershipConflictDoesNotOverwrite(t *testing.T) {
	for _, owner := range []string{"", "previous-history-server"} {
		t.Run("owner="+owner, func(t *testing.T) {
			ctx := context.Background()
			hs := historyServerTestCR()
			existing := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ray-history-server-config", Namespace: "operator", Labels: map[string]string{historyServerOwnerLabel: string(hs.UID)}}, Data: map[string]string{"admin": "preserve"}}
			if owner != "" {
				existing.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ray.io/v1alpha1", Kind: "RayHistoryServer", Name: "default", UID: types.UID(owner), Controller: new(true)}}
			}
			c := historyServerTestClient(t, hs, existing)
			before := &corev1.ConfigMap{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(existing), before))
			desired := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: existing.Name, Namespace: existing.Namespace}, Data: map[string]string{"managed": "replacement"}}
			err := historyServerTestController(c).applyOwned(ctx, hs, desired)
			require.ErrorContains(t, err, "ownership conflict")
			after := &corev1.ConfigMap{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(existing), after))
			require.Equal(t, before, after)
		})
	}
}

func TestHistoryServerStatusStableAcrossRolloutAndRepeatedFinish(t *testing.T) {
	ctx := context.Background()
	hs := historyServerTestCR()
	transition := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	hs.Status.Conditions = []metav1.Condition{{Type: string(rayv1alpha1.RayHistoryServerConfigurationValid), Status: metav1.ConditionTrue, Reason: "ConfigurationValid", Message: "Storage and security configuration is valid", ObservedGeneration: hs.Generation, LastTransitionTime: transition}}
	patches := 0
	c := fake.NewClientBuilder().WithScheme(historyServerTestScheme(t)).WithObjects(hs).WithStatusSubresource(hs).WithInterceptorFuncs(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subresource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			return c.SubResource(subresource).Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	r := historyServerTestController(c)
	for _, step := range []struct {
		available, ready bool
		reason           string
	}{{true, true, "Ready"}, {false, false, "DeploymentPending"}, {false, false, "DeploymentFailed"}, {true, true, "Ready"}} {
		_, err := r.finish(ctx, hs, true, step.available, step.ready, step.reason, "rollout observation")
		require.NoError(t, err)
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(hs), hs))
		condition := historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerConfigurationValid, metav1.ConditionTrue)
		require.Equal(t, transition, condition.LastTransitionTime)
		require.Equal(t, "ConfigurationValid", condition.Reason)
		if step.reason == "DeploymentFailed" {
			historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerDegraded, metav1.ConditionTrue)
		}
		before, count := hs.DeepCopy(), patches
		_, err = r.finish(ctx, hs, true, step.available, step.ready, step.reason, "rollout observation")
		require.NoError(t, err)
		require.Equal(t, count, patches, "unchanged status must not issue a patch")
		require.Equal(t, before.Status, hs.Status, "condition transition times must be stable")
	}
}

func TestHistoryServerDeletionOnlyRemovesManagedResources(t *testing.T) {
	ctx := context.Background()
	hs := historyServerTestCR()
	hs.Finalizers = []string{historyServerFinalizer}
	now := metav1.Now()
	hs.DeletionTimestamp = &now
	s := historyServerTestScheme(t)
	managed := []client.Object{&appsv1.Deployment{}, &corev1.Service{}, &corev1.ConfigMap{}, &corev1.ServiceAccount{}, &networkingv1.NetworkPolicy{}, &rbacv1.ClusterRoleBinding{}, &certmanagerv1.Certificate{}, &certmanagerv1.Issuer{}, &corev1.Secret{}}
	objects := []client.Object{hs}
	for _, obj := range managed {
		obj.SetName("managed")
		if _, clusterScoped := obj.(*rbacv1.ClusterRoleBinding); !clusterScoped {
			obj.SetNamespace("operator")
		}
		obj.SetLabels(map[string]string{historyServerOwnerLabel: string(hs.UID)})
		require.NoError(t, controllerutil.SetOwnerReference(hs, obj, s))
		objects = append(objects, obj)
	}
	source, static := historyServerTestSource(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "static-tls", Namespace: "operator"}, Data: map[string][]byte{"admin": []byte("preserve")}}
	labelOnly := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "label-only", Namespace: "operator", Labels: map[string]string{historyServerOwnerLabel: string(hs.UID)}}}
	ownerOnly := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "owner-only", Namespace: "operator"}}
	require.NoError(t, controllerutil.SetOwnerReference(hs, ownerOnly, s))
	otherNamespace := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other-namespace", Namespace: "other", Labels: map[string]string{historyServerOwnerLabel: string(hs.UID)}}}
	require.NoError(t, controllerutil.SetOwnerReference(hs, otherNamespace, s))
	preserved := []client.Object{source, static, labelOnly, ownerOnly, otherNamespace}
	objects = append(objects, preserved...)
	c := historyServerTestClient(t, objects...)
	before := make([]client.Object, len(preserved))
	for i, obj := range preserved {
		before[i] = obj.DeepCopyObject().(client.Object)
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), before[i]))
	}
	r := historyServerTestController(c)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hs)}
	result, err := r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	require.NoError(t, c.Get(ctx, request.NamespacedName, hs))
	require.Contains(t, hs.Finalizers, historyServerFinalizer, "retain finalizer until managed resources disappear")
	for _, obj := range managed {
		require.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)), "%T must be deleted", obj)
	}
	_, err = r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, request.NamespacedName, hs)))
	for i, obj := range preserved {
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), obj))
		require.Equal(t, before[i], obj)
	}
}

func historyServerTestTLSSecret(t *testing.T) *corev1.Secret {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "history-server-test"}, DNSNames: []string{"ray-history-server.operator.svc"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	privateKey, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "static-tls", Namespace: "operator"}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateKey})}}
}

// This test deliberately uses its own API server and only the RHS CRD. It can
// run with -run '^TestHistoryServer' without starting the existing Ginkgo suite.
func TestHistoryServerEnvtestLifecycle(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run real server-side apply lifecycle coverage")
	}
	crdBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "ray.io_rayhistoryservers.yaml"))
	require.NoError(t, err)
	crd := &apiextensionsv1.CustomResourceDefinition{}
	require.NoError(t, yaml.Unmarshal(crdBytes, crd))
	environment := &envtest.Environment{BinaryAssetsDirectory: assets, CRDs: []*apiextensionsv1.CustomResourceDefinition{crd}}
	config, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	c, err := client.New(config, client.Options{Scheme: historyServerTestScheme(t)})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "operator"}}))
	source, static := historyServerTestSource(), historyServerTestTLSSecret(t)
	source.UID = ""
	require.NoError(t, c.Create(ctx, source))
	require.NoError(t, c.Create(ctx, static))
	staticBefore := static.DeepCopy()
	hs := historyServerTestCR()
	hs.UID, hs.Generation = "", 0
	hs.Spec.Storage.CredentialsSecret = &corev1.SecretReference{Name: source.Name}
	hs.Spec.Security = &rayv1alpha1.HistoryServerSecurity{TLS: &rayv1alpha1.HistoryServerTLS{SecretName: static.Name}}
	require.NoError(t, c.Create(ctx, hs))
	r := historyServerTestController(c)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(hs)}
	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, request)
		require.NoError(t, err)
		require.NoError(t, c.Get(ctx, request.NamespacedName, hs))
	}
	reconcile()
	require.Contains(t, hs.Finalizers, historyServerFinalizer)
	configuration := historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerConfigurationValid, metav1.ConditionTrue)
	historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerReady, metav1.ConditionFalse)
	deployment := &appsv1.Deployment{}
	key := client.ObjectKey{Namespace: "operator", Name: "ray-history-server"}
	require.NoError(t, c.Get(ctx, key, deployment))
	desiredImage := deployment.Spec.Template.Spec.Containers[0].Image
	// A normal Update steals field ownership; reconciliation must force SSA
	// only after checking this exact CR's ownership and restore the operand.
	deployment.Spec.Template.Spec.Containers[0].Image = "drift:latest"
	require.NoError(t, c.Update(ctx, deployment, client.FieldOwner("administrator")))
	reconcile()
	require.NoError(t, c.Get(ctx, key, deployment))
	require.Equal(t, desiredImage, deployment.Spec.Template.Spec.Containers[0].Image)
	generation := deployment.Generation
	reconcile()
	require.NoError(t, c.Get(ctx, key, deployment))
	require.Equal(t, generation, deployment.Generation, "identical SSA must not trigger another rollout")
	replicas := *deployment.Spec.Replicas
	deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: replicas, UpdatedReplicas: replicas, AvailableReplicas: replicas, ReadyReplicas: replicas}
	require.NoError(t, c.Status().Update(ctx, deployment))
	reconcile()
	historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerReady, metav1.ConditionTrue)
	require.Equal(t, configuration.LastTransitionTime, historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerConfigurationValid, metav1.ConditionTrue).LastTransitionTime)
	status, resourceVersion := hs.Status, hs.ResourceVersion
	reconcile()
	require.Equal(t, status, hs.Status)
	require.Equal(t, resourceVersion, hs.ResourceVersion, "no redundant CR status patch")
	// Secret rotation must change the pod template without adopting the source.
	templateBefore := deployment.Spec.Template.DeepCopy()
	source.Data["AWS_SECRET_ACCESS_KEY"] = []byte("rotated-key")
	require.NoError(t, c.Update(ctx, source))
	sourceBefore := source.DeepCopy()
	reconcile()
	require.NoError(t, c.Get(ctx, key, deployment))
	require.NotEqual(t, templateBefore.Annotations, deployment.Spec.Template.Annotations)
	historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerReady, metav1.ConditionFalse)
	require.Equal(t, configuration.LastTransitionTime, historyServerTestCondition(t, hs, rayv1alpha1.RayHistoryServerConfigurationValid, metav1.ConditionTrue).LastTransitionTime)
	for _, pair := range [][2]*corev1.Secret{{sourceBefore, source}, {staticBefore, static}} {
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pair[1]), pair[1]))
		require.Equal(t, pair[0], pair[1])
	}
	// envtest has no garbage collector. Keep a real custom finalizer to prove
	// the CR waits; then emulate GC removing foregroundDeletion on dependents.
	deployment.Finalizers = append(deployment.Finalizers, "test.ray.io/hold")
	require.NoError(t, c.Update(ctx, deployment))
	require.NoError(t, c.Delete(ctx, hs))
	reconcile()
	require.Contains(t, hs.Finalizers, historyServerFinalizer)
	require.NoError(t, c.Get(ctx, key, deployment))
	require.False(t, deployment.DeletionTimestamp.IsZero())
	require.Contains(t, deployment.Finalizers, "test.ray.io/hold")
	for _, list := range []client.ObjectList{&appsv1.DeploymentList{}, &corev1.ServiceList{}, &corev1.ConfigMapList{}, &corev1.ServiceAccountList{}, &networkingv1.NetworkPolicyList{}, &rbacv1.ClusterRoleBindingList{}} {
		require.NoError(t, c.List(ctx, list, client.MatchingLabels{historyServerOwnerLabel: string(hs.UID)}))
		require.NoError(t, meta.EachListItem(list, func(item runtime.Object) error {
			obj := item.(client.Object)
			require.False(t, obj.GetDeletionTimestamp().IsZero())
			obj.SetFinalizers(nil)
			return c.Update(ctx, obj)
		}))
	}
	_, err = r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, request.NamespacedName, hs)))
	for _, pair := range [][2]*corev1.Secret{{sourceBefore, source}, {staticBefore, static}} {
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pair[1]), pair[1]))
		require.Equal(t, pair[0], pair[1])
	}
}
