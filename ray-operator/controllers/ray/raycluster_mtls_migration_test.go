package ray

import (
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestMTLSMigrationReusesLegacyResources(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	cluster := newMTLSTestCluster("legacy-cluster")
	cluster.Annotations = map[string]string{utils.EnableSecureTrustedNetworkAnnotationKey: "true"}
	scheme := newMTLSTestScheme()
	// Names and specs follow the ODH 1.6.2 resource contract, including its
	// UID-suffixed CA Secret and broader leaf SANs.
	caSecretName := "legacy-cluster-ca-secret-test-uid"
	ca := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: "ray-ca-certificate-legacy-cluster", Namespace: cluster.Namespace, UID: "ca-cert-uid"},
		Spec: certmanagerv1.CertificateSpec{
			SecretName: caSecretName, IsCA: true, CommonName: "ray-root-ca-legacy-cluster",
			PrivateKey: &certmanagerv1.CertificatePrivateKey{Algorithm: certmanagerv1.RSAKeyAlgorithm, Size: 4096},
			Duration:   &metav1.Duration{Duration: 3650 * 24 * time.Hour}, RenewBefore: &metav1.Duration{Duration: 30 * 24 * time.Hour},
			Usages:         []certmanagerv1.KeyUsage{certmanagerv1.UsageCertSign, certmanagerv1.UsageCRLSign},
			IssuerRef:      cmmeta.IssuerReference{Name: "ray-selfsigned-issuer-legacy-cluster", Kind: "Issuer", Group: "cert-manager.io"},
			SecretTemplate: &certmanagerv1.CertificateSecretTemplate{Labels: map[string]string{"ray.openshift.ai/cluster-name": cluster.Name}},
		},
	}
	objects := []client.Object{cluster, ca}
	certificates := []*certmanagerv1.Certificate{ca}
	for _, node := range []string{"head", "worker"} {
		cert := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{Name: "ray-" + node + "-cert-legacy-cluster", Namespace: cluster.Namespace, UID: types.UID(node + "-cert-uid")},
			Spec: certmanagerv1.CertificateSpec{
				SecretName:  "ray-" + node + "-secret-legacy-cluster",
				DNSNames:    []string{"localhost", "legacy-cluster-" + node + "-svc", "legacy-cluster-" + node + "-svc.default.svc", "legacy-cluster-" + node + "-svc.default.svc.cluster.local"},
				IPAddresses: []string{"127.0.0.1", "10.0.0.1", "10.0.0.2"},
				Duration:    &metav1.Duration{Duration: 2160 * time.Hour}, RenewBefore: &metav1.Duration{Duration: 360 * time.Hour},
				Usages: leafCertKeyUsages(), IssuerRef: cmmeta.IssuerReference{Name: "ray-ca-issuer-legacy-cluster", Kind: "Issuer", Group: "cert-manager.io"},
			},
		}
		certificates = append(certificates, cert)
		objects = append(objects, cert)
	}
	var secrets []*corev1.Secret
	for _, cert := range certificates {
		require.NoError(t, controllerutil.SetControllerReference(cluster, cert, scheme))
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: cert.Spec.SecretName, Namespace: cluster.Namespace, UID: types.UID(cert.Name + "-secret-uid")},
			Data:       map[string][]byte{"tls.crt": []byte("existing-cert"), "tls.key": []byte("existing-key"), "ca.crt": []byte("existing-ca")},
		}
		secrets = append(secrets, secret)
		objects = append(objects, secret)
	}
	issuers := []*certmanagerv1.Issuer{
		{ObjectMeta: metav1.ObjectMeta{Name: "ray-selfsigned-issuer-legacy-cluster", Namespace: cluster.Namespace, UID: "selfsigned-uid"}, Spec: certmanagerv1.IssuerSpec{IssuerConfig: certmanagerv1.IssuerConfig{SelfSigned: &certmanagerv1.SelfSignedIssuer{}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "ray-ca-issuer-legacy-cluster", Namespace: cluster.Namespace, UID: "ca-issuer-uid"}, Spec: certmanagerv1.IssuerSpec{IssuerConfig: certmanagerv1.IssuerConfig{CA: &certmanagerv1.CAIssuer{SecretName: caSecretName}}}},
	}
	for _, issuer := range issuers {
		require.NoError(t, controllerutil.SetControllerReference(cluster, issuer, scheme))
		objects = append(objects, issuer)
	}
	var pods []*corev1.Pod
	for i, node := range []rayv1.RayNodeType{rayv1.HeadNode, rayv1.WorkerNode} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: string(node) + "-existing", Namespace: cluster.Namespace, UID: types.UID(string(node) + "-pod-uid"), Labels: map[string]string{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(node), utils.RayNodeGroupLabelKey: "small-group"}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray", Env: []corev1.EnvVar{{Name: "RAY_USE_TLS", Value: "1"}}}}, Volumes: []corev1.Volume{{Name: "ray-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: certificates[i+1].Spec.SecretName}}}}},
			Status:     corev1.PodStatus{PodIP: []string{"10.0.0.1", "10.0.0.2"}[i], Phase: corev1.PodRunning},
		}
		pods = append(pods, pod)
		objects = append(objects, pod)
	}
	r := newMTLSController(t, objects...)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	migration := &LegacySecurityController{Client: r.Client, isOpenShift: true}
	_, err := migration.Reconcile(t.Context(), request)
	require.NoError(t, err)
	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	for _, cert := range certificates {
		got := &certmanagerv1.Certificate{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cert), got))
		assert.Equal(t, cert.UID, got.UID)
		assert.Equal(t, cert.Spec.SecretName, got.Spec.SecretName)
		assert.Equal(t, cert.Spec.IssuerRef, got.Spec.IssuerRef)
		if cert == ca {
			assert.Equal(t, ca.Spec, got.Spec, "the upgrade must not rotate the existing root CA")
		} else {
			assert.NotEqual(t, cert.Spec.IPAddresses, got.Spec.IPAddresses, "leaf SANs are reconciled by node type")
		}
	}
	for _, secret := range secrets {
		got := &corev1.Secret{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(secret), got))
		assert.Equal(t, secret.UID, got.UID)
		assert.Equal(t, secret.Data, got.Data, "cert-manager owns issuance; migration must not overwrite key material")
		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, "Certificate", got.OwnerReferences[0].Kind)
	}
	for _, issuer := range issuers {
		got := &certmanagerv1.Issuer{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(issuer), got))
		assert.Equal(t, issuer.UID, got.UID)
		assert.Equal(t, issuer.Spec, got.Spec)
	}
	for _, pod := range pods {
		got := &corev1.Pod{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pod), got))
		assert.Equal(t, pod.UID, got.UID)
		assert.Equal(t, pod.Spec, got.Spec, "existing pods must not roll during migration")
	}
}

func TestMTLSRenewalWaitsForCurrentGeneration(t *testing.T) {
	cluster := newMTLSTestCluster("renewal-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
	head := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.HeadNode))
	worker := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode))
	head.Generation++ // Ready=True still describes the previously issued SANs.
	objects := []client.Object{cluster, head, worker}
	for _, node := range []rayv1.RayNodeType{rayv1.HeadNode, rayv1.WorkerNode} {
		objects = append(objects, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: utils.GetTLSSecretName(cluster.Name, node), Namespace: cluster.Namespace},
			Data:       map[string][]byte{"tls.crt": []byte("old-cert"), "tls.key": []byte("old-key"), "ca.crt": []byte("ca")},
		})
	}
	r := newMTLSController(t, objects...)
	main := &RayClusterReconciler{Client: r.Client, Scheme: r.Scheme}
	require.ErrorContains(t, main.checkMTLSSecretsReady(t.Context(), cluster), "not ready at current generation 2")
	markMTLSCertificatesReady(t.Context(), t, r, cluster)
	require.NoError(t, main.checkMTLSSecretsReady(t.Context(), cluster))
}
