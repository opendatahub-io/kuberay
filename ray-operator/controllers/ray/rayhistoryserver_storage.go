package ray

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

// Credentials are selected explicitly: arbitrary Secret keys must not override
// the backend, disable TLS, or inject process configuration into the workload.
func (r *RayHistoryServerController) storageInputs(ctx context.Context, hs *rayv1alpha1.RayHistoryServer) (historyServerInputs, error) {
	in := historyServerInputs{Namespace: r.Namespace, Image: r.Image, ProxyImage: r.ProxyImage, ProxyTLSArgs: r.ProxyTLSArgs}
	if r.Namespace == "" {
		return in, fmt.Errorf("POD_NAMESPACE must identify the KubeRay operator namespace")
	}
	if r.Image == "" {
		return in, fmt.Errorf("RELATED_IMAGE_RAY_HISTORY_SERVER must specify the supported History Server image")
	}
	storage := hs.Spec.Storage
	if storage == nil {
		return in, fmt.Errorf("spec.storage is required")
	}
	if security := hs.Spec.Security; security != nil {
		if security.LiveClusterProxying != nil && *security.LiveClusterProxying {
			return in, fmt.Errorf("live cluster proxying is not supported by the managed History Server")
		}
		if len(security.CABundle) > 0 {
			if !x509.NewCertPool().AppendCertsFromPEM(security.CABundle) {
				return in, fmt.Errorf("security.caBundle must contain PEM certificates")
			}
			in.Volumes = append(in.Volumes, corev1.Volume{Name: "storage-ca", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "ray-history-server-config"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}}})
			in.VolumeMounts = append(in.VolumeMounts, corev1.VolumeMount{Name: "storage-ca", MountPath: "/etc/history-server/ca", ReadOnly: true})
			in.Env = append(in.Env, corev1.EnvVar{Name: "SSL_CERT_FILE", Value: "/etc/history-server/ca/ca.crt"}, corev1.EnvVar{Name: "AWS_CA_BUNDLE", Value: "/etc/history-server/ca/ca.crt"})
		}
	}
	var secret *corev1.Secret
	if ref := storage.CredentialsSecret; ref != nil {
		if ref.Name == "" || (ref.Namespace != "" && ref.Namespace != r.Namespace) {
			return in, fmt.Errorf("storage.credentialsSecret must name a Secret in the KubeRay operator namespace %q", r.Namespace)
		}
		secret = &corev1.Secret{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: ref.Name}, secret); err != nil {
			return in, fmt.Errorf("read storage.credentialsSecret: %w", err)
		}
	}
	add := func(name, value string) { in.Env = append(in.Env, corev1.EnvVar{Name: name, Value: value}) }
	credential := func(name string, required bool) error {
		if secret == nil || len(secret.Data[name]) == 0 {
			if required {
				return fmt.Errorf("storage.credentialsSecret must contain nonempty key %s", name)
			}
			return nil
		}
		in.Env = append(in.Env, corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: name}}})
		return nil
	}
	add("STORAGE_BACKEND", strings.ToLower(string(storage.Type)))
	add("STORAGE_ROOT_DIR", storage.RootDir)
	switch storage.Type {
	case rayv1alpha1.HistoryServerStorageS3:
		if storage.S3 == nil || storage.S3.Bucket == "" || storage.S3.Region == "" {
			return in, fmt.Errorf("storage.s3.bucket and storage.s3.region are required")
		}
		if err := historyServerHTTPSURL(storage.S3.Endpoint); err != nil {
			return in, fmt.Errorf("storage.s3.endpoint: %w", err)
		}
		add("S3_BUCKET", storage.S3.Bucket)
		add("S3_REGION", storage.S3.Region)
		add("S3_ENDPOINT", storage.S3.Endpoint)
		add("S3DISABLE_SSL", "false")
		// Private S3-compatible Services normally do not provide wildcard
		// bucket DNS names; address buckets beneath the configured endpoint.
		if storage.S3.Endpoint != "" {
			add("S3FORCE_PATH_STYLE", "true")
		}
		for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
			if err := credential(key, secret != nil && key != "AWS_SESSION_TOKEN"); err != nil {
				return in, err
			}
		}
	case rayv1alpha1.HistoryServerStorageGCS:
		if storage.GCS == nil || storage.GCS.Bucket == "" {
			return in, fmt.Errorf("storage.gcs.bucket is required")
		}
		add("GCS_BUCKET", storage.GCS.Bucket)
		if secret != nil {
			if !json.Valid(secret.Data["credentials.json"]) {
				return in, fmt.Errorf("storage.credentialsSecret must contain valid JSON in credentials.json")
			}
			add("GOOGLE_APPLICATION_CREDENTIALS", "/etc/history-server/gcs/credentials.json")
			in.Volumes = append(in.Volumes, corev1.Volume{Name: "gcs-credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secret.Name, Items: []corev1.KeyToPath{{Key: "credentials.json", Path: "credentials.json"}}}}})
			in.VolumeMounts = append(in.VolumeMounts, corev1.VolumeMount{Name: "gcs-credentials", MountPath: "/etc/history-server/gcs", ReadOnly: true})
		}
	case rayv1alpha1.HistoryServerStorageAzureBlob:
		if storage.AzureBlob == nil || storage.AzureBlob.Container == "" {
			return in, fmt.Errorf("storage.azureBlob.container is required")
		}
		azure := storage.AzureBlob
		mode := strings.ToLower(strings.TrimSpace(azure.AuthMode))
		if mode == "" {
			if secret != nil && len(secret.Data["AZURE_STORAGE_CONNECTION_STRING"]) > 0 {
				mode = "connection_string"
			} else {
				mode = "default"
			}
		}
		add("AZURE_STORAGE_CONTAINER", azure.Container)
		add("AZURE_STORAGE_AUTH_MODE", mode)
		switch mode {
		case "connection_string":
			if err := credential("AZURE_STORAGE_CONNECTION_STRING", true); err != nil {
				return in, err
			}
			// Never include connection-string contents in validation errors/status.
			connection := strings.ToLower(string(secret.Data["AZURE_STORAGE_CONNECTION_STRING"]))
			if strings.Contains(connection, "http://") || strings.Contains(connection, "defaultendpointsprotocol=http;") || strings.HasSuffix(connection, "defaultendpointsprotocol=http") {
				return in, fmt.Errorf("Azure storage credentials must use HTTPS endpoints")
			}
		case "default":
			if azure.AccountURL == "" {
				return in, fmt.Errorf("storage.azureBlob.accountUrl is required for default authentication")
			}
			if err := historyServerHTTPSURL(azure.AccountURL); err != nil {
				return in, fmt.Errorf("storage.azureBlob.accountUrl: %w", err)
			}
			add("AZURE_STORAGE_ACCOUNT_URL", azure.AccountURL)
			for _, key := range []string{"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"} {
				if err := credential(key, secret != nil); err != nil {
					return in, err
				}
			}
		default:
			return in, fmt.Errorf("managed Azure authentication currently supports connection_string or default; workload_identity requires additional service-account/token configuration")
		}
	default:
		return in, fmt.Errorf("unsupported storage backend %q", storage.Type)
	}
	// Hash public spec plus source UID/resourceVersion, never credential contents.
	// This restarts env-based consumers on rotation without exposing key material.
	revision, err := json.Marshal(hs.Spec)
	if err != nil {
		return in, err
	}
	if secret != nil {
		revision = append(revision, []byte(string(secret.UID)+":"+secret.ResourceVersion)...)
	}
	in.Revision = fmt.Sprintf("%x", sha256.Sum256(revision))
	return in, nil
}

func historyServerHTTPSURL(value string) error {
	if value == "" {
		return nil
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must be an HTTPS URL without embedded credentials, query or fragment")
	}
	return nil
}
