package ray

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

// Serving TLS is unrelated to RayCluster mTLS: no collector-to-server link or
// client certificates are required. A static Secret bypasses cert-manager.
func (r *RayHistoryServerController) reconcileHistoryServerTLS(ctx context.Context, hs *rayv1alpha1.RayHistoryServer, in *historyServerInputs) (bool, string, error) {
	var options *rayv1alpha1.HistoryServerTLS
	if hs.Spec.Security != nil {
		options = hs.Spec.Security.TLS
	}
	static := options != nil && options.SecretName != "" && options.IssuerRef == nil
	in.TLSSecretName = "ray-history-server-tls"
	if options != nil && options.SecretName != "" {
		in.TLSSecretName = options.SecretName
	}
	if ref := hs.Spec.Storage.CredentialsSecret; ref != nil && ref.Name == in.TLSSecretName {
		return false, "", fmt.Errorf("serving TLS Secret must be different from the storage credentials Secret")
	}
	var certificate *certmanagerv1.Certificate
	if !static {
		issuerRef := cmmeta.ObjectReference{Name: "ray-history-server-issuer", Kind: "Issuer", Group: "cert-manager.io"}
		if options != nil && options.IssuerRef != nil {
			ref := options.IssuerRef
			issuerRef.Name, issuerRef.Kind = ref.Name, ref.Kind
			if issuerRef.Kind == "" {
				issuerRef.Kind = "Issuer"
			}
			if ref.APIGroup != nil && *ref.APIGroup != "cert-manager.io" {
				return false, "", fmt.Errorf("security.tls.issuerRef.apiGroup must be cert-manager.io")
			}
			if issuerRef.Name == "" {
				return false, "", fmt.Errorf("security.tls.issuerRef.name is required")
			}
			var issuer client.Object
			key := client.ObjectKey{Name: issuerRef.Name, Namespace: r.Namespace}
			switch issuerRef.Kind {
			case "Issuer":
				issuer = &certmanagerv1.Issuer{}
			case "ClusterIssuer":
				issuer = &certmanagerv1.ClusterIssuer{}
				key.Namespace = ""
			default:
				return false, "", fmt.Errorf("security.tls.issuerRef.kind must be Issuer or ClusterIssuer")
			}
			if err := r.APIReader.Get(ctx, key, issuer); err != nil {
				return false, "", fmt.Errorf("read serving certificate issuer: %w", err)
			}
		} else {
			issuer := &certmanagerv1.Issuer{ObjectMeta: metav1.ObjectMeta{Name: issuerRef.Name, Namespace: r.Namespace}, Spec: certmanagerv1.IssuerSpec{IssuerConfig: certmanagerv1.IssuerConfig{SelfSigned: &certmanagerv1.SelfSignedIssuer{}}}}
			if err := r.applyOwned(ctx, hs, issuer); err != nil {
				return false, "", fmt.Errorf("reconcile serving issuer (cert-manager is required): %w", err)
			}
		}
		// Do not ask cert-manager to overwrite an existing administrator Secret.
		existingSecret := &corev1.Secret{}
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: in.TLSSecretName}, existingSecret)
		if err != nil && !errors.IsNotFound(err) {
			return false, "", err
		}
		if err == nil {
			managed := historyServerOwnsSecret(existingSecret, hs)
			if !managed {
				return false, "", fmt.Errorf("serving TLS Secret %q already exists and is not managed by this RayHistoryServer; use tls.secretName without issuerRef for a static Secret", in.TLSSecretName)
			}
		} else {
			// Reserve the Secret with ownership before requesting issuance. This
			// also makes deletion safe if the CR is removed before cert-manager
			// issues it, regardless of cert-manager's owner-reference flags.
			placeholder := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: in.TLSSecretName, Namespace: r.Namespace, Labels: map[string]string{historyServerOwnerLabel: string(hs.UID)}}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: {}, corev1.TLSPrivateKeyKey: {}}}
			if err := controllerutil.SetOwnerReference(hs, placeholder, r.Scheme); err != nil {
				return false, "", err
			}
			if err := r.Create(ctx, placeholder); err != nil {
				return false, "", err
			}
		}
		certificate = &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{Name: "ray-history-server-cert", Namespace: r.Namespace},
			Spec: certmanagerv1.CertificateSpec{
				SecretName: in.TLSSecretName, IssuerRef: issuerRef,
				DNSNames:       []string{"ray-history-server", "ray-history-server." + r.Namespace, "ray-history-server." + r.Namespace + ".svc", "ray-history-server." + r.Namespace + ".svc.cluster.local"},
				Usages:         []certmanagerv1.KeyUsage{certmanagerv1.UsageDigitalSignature, certmanagerv1.UsageKeyEncipherment, certmanagerv1.UsageServerAuth},
				SecretTemplate: &certmanagerv1.CertificateSecretTemplate{Labels: map[string]string{historyServerOwnerLabel: string(hs.UID)}},
			},
		}
		if err := r.applyOwned(ctx, hs, certificate); err != nil {
			return false, "", fmt.Errorf("reconcile serving certificate: %w", err)
		}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(certificate), certificate); err != nil {
			return false, "", err
		}
	}
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: in.TLSSecretName}, secret); err != nil {
		if errors.IsNotFound(err) && !static {
			return false, "", nil
		}
		return false, "", fmt.Errorf("read serving TLS Secret: %w", err)
	}
	if !static {
		if !historyServerOwnsSecret(secret, hs) {
			return false, "", fmt.Errorf("serving TLS Secret ownership changed")
		}
	}
	revision := string(secret.UID) + ":" + secret.ResourceVersion
	if err := validHistoryServerCertificate(secret, "ray-history-server."+r.Namespace+".svc"); err != nil {
		if static {
			return false, revision, err
		}
		return false, revision, nil // cert-manager can repair/renew it.
	}
	if certificate != nil {
		for _, condition := range certificate.Status.Conditions {
			if condition.Type == certmanagerv1.CertificateConditionReady && condition.Status == cmmeta.ConditionTrue && condition.ObservedGeneration >= certificate.Generation {
				return true, revision, nil
			}
		}
		return false, revision, nil
	}
	return true, revision, nil
}

func historyServerOwnsSecret(secret *corev1.Secret, hs *rayv1alpha1.RayHistoryServer) bool {
	if secret.Labels[historyServerOwnerLabel] != string(hs.UID) {
		return false
	}
	for _, owner := range secret.OwnerReferences {
		if owner.UID == hs.UID {
			return true
		}
	}
	return false
}

func validHistoryServerCertificate(secret *corev1.Secret, dnsName string) error {
	pair, err := tls.X509KeyPair(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return fmt.Errorf("serving TLS Secret must contain a matching tls.crt/tls.key pair")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("serving TLS Secret contains an invalid certificate")
	}
	if time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return fmt.Errorf("serving TLS certificate is not currently valid")
	}
	if err := leaf.VerifyHostname(dnsName); err != nil {
		return fmt.Errorf("serving TLS certificate must cover %s", dnsName)
	}
	return nil
}
