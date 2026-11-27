package ray

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

func historyServingSecret(t *testing.T, dns string, expiry time.Time) *corev1.Secret {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{dns}, NotBefore: time.Now().Add(-time.Hour), NotAfter: expiry, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provided-tls", Namespace: "operator"}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})}}
}

func TestHistoryServerServingCertificate(t *testing.T) {
	const dns = "ray-history-server.operator.svc"
	valid := historyServingSecret(t, dns, time.Now().Add(time.Hour))
	require.NoError(t, validHistoryServerCertificate(valid, dns))
	require.ErrorContains(t, validHistoryServerCertificate(valid, "different.svc"), "must cover")
	expired := historyServingSecret(t, dns, time.Now().Add(-time.Minute))
	require.ErrorContains(t, validHistoryServerCertificate(expired, dns), "not currently valid")
	valid.Data[corev1.TLSPrivateKeyKey] = []byte("not a key")
	require.ErrorContains(t, validHistoryServerCertificate(valid, dns), "matching")
}

func TestHistoryServerStaticTLSDoesNotRequireCertManager(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1alpha1.AddToScheme(scheme))
	secret := historyServingSecret(t, "ray-history-server.operator.svc", time.Now().Add(time.Hour))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	r := &RayHistoryServerController{Client: c, APIReader: c, Scheme: scheme, Namespace: "operator"}
	hs := &rayv1alpha1.RayHistoryServer{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "hs"}, Spec: rayv1alpha1.RayHistoryServerSpec{Storage: &rayv1alpha1.HistoryServerStorage{}, Security: &rayv1alpha1.HistoryServerSecurity{TLS: &rayv1alpha1.HistoryServerTLS{SecretName: secret.Name}}}}
	in := historyServerInputs{}
	ready, revision, err := r.reconcileHistoryServerTLS(ctx, hs, &in)
	require.NoError(t, err)
	require.True(t, ready)
	require.NotEmpty(t, revision)
	require.Equal(t, secret.Name, in.TLSSecretName)
	got := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secret), got))
	require.Empty(t, got.OwnerReferences)
	require.Empty(t, got.Labels)
}

func TestHistoryServerRequestsCertificateBeforeWaiting(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1alpha1.AddToScheme(scheme))
	require.NoError(t, certmanagerv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &RayHistoryServerController{Client: c, APIReader: c, Scheme: scheme, Namespace: "operator"}
	hs := &rayv1alpha1.RayHistoryServer{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "hs"}, Spec: rayv1alpha1.RayHistoryServerSpec{Storage: &rayv1alpha1.HistoryServerStorage{}}}
	in := historyServerInputs{}
	ready, _, err := r.reconcileHistoryServerTLS(ctx, hs, &in)
	require.NoError(t, err)
	require.False(t, ready)
	issuer := &certmanagerv1.Issuer{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "operator", Name: "ray-history-server-issuer"}, issuer))
	require.NotNil(t, issuer.Spec.SelfSigned)
	certificate := &certmanagerv1.Certificate{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "operator", Name: "ray-history-server-cert"}, certificate))
	require.Contains(t, certificate.Spec.DNSNames, "ray-history-server.operator.svc")
	require.Equal(t, "ray-history-server-tls", certificate.Spec.SecretName)
	require.True(t, metav1.IsControlledBy(certificate, hs))
	require.NotContains(t, certificate.Spec.Usages, certmanagerv1.UsageClientAuth)
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "operator", Name: certificate.Spec.SecretName}, secret))
	require.True(t, historyServerOwnsSecret(secret, hs), "reserve ownership before issuance so deletion cannot leak the Secret")
}

func TestHistoryServerTLSRejectsUnownedSecret(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1alpha1.AddToScheme(scheme))
	require.NoError(t, certmanagerv1.AddToScheme(scheme))
	hs := &rayv1alpha1.RayHistoryServer{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "hs"}, Spec: rayv1alpha1.RayHistoryServerSpec{Storage: &rayv1alpha1.HistoryServerStorage{}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ray-history-server-tls", Namespace: "operator", Labels: map[string]string{historyServerOwnerLabel: "hs"}}, Data: map[string][]byte{"admin": []byte("untouched")}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	r := &RayHistoryServerController{Client: c, APIReader: c, Scheme: scheme, Namespace: "operator"}
	_, _, err := r.reconcileHistoryServerTLS(ctx, hs, &historyServerInputs{})
	require.ErrorContains(t, err, "not managed")
	got := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secret), got))
	require.Empty(t, got.OwnerReferences)
	require.Equal(t, secret.Data, got.Data)
}
