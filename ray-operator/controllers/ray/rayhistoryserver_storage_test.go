package ray

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // RayHistoryServer is intentionally introduced in the v1alpha1 API.
)

func historyServerTestSource() *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "storage-source", Namespace: "operator", UID: "source-uid"}, Data: map[string][]byte{
		"AWS_ACCESS_KEY_ID": []byte("private-access-id"), "AWS_SECRET_ACCESS_KEY": []byte("private-access-key"), "AWS_SESSION_TOKEN": []byte("private-session-token"),
	}}
}

func historyServerTestEnv(t *testing.T, in historyServerInputs) map[string]corev1.EnvVar {
	t.Helper()
	env := make(map[string]corev1.EnvVar)
	for _, variable := range in.Env {
		_, duplicate := env[variable.Name]
		require.False(t, duplicate, "duplicate environment variable %s", variable.Name)
		env[variable.Name] = variable
	}
	return env
}

func TestHistoryServerStorageRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*RayHistoryServerController, *rayv1alpha1.RayHistoryServer, *corev1.Secret)
		want   string
	}{
		{"namespace", func(r *RayHistoryServerController, _ *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			r.Namespace = ""
		}, "POD_NAMESPACE"},
		{"image", func(r *RayHistoryServerController, _ *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) { r.Image = "" }, "RELATED_IMAGE_RAY_HISTORY_SERVER"},
		{"storage", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage = nil
		}, "spec.storage is required"},
		{"cross namespace", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage.CredentialsSecret.Namespace = "other"
		}, "operator namespace"},
		{"empty secret name", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage.CredentialsSecret.Name = ""
		}, "operator namespace"},
		{"missing secret", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage.CredentialsSecret.Name = "missing"
		}, "read storage.credentialsSecret"},
		{"missing access key", func(_ *RayHistoryServerController, _ *rayv1alpha1.RayHistoryServer, secret *corev1.Secret) {
			delete(secret.Data, "AWS_ACCESS_KEY_ID")
		}, "nonempty key AWS_ACCESS_KEY_ID"},
		{"empty secret key", func(_ *RayHistoryServerController, _ *rayv1alpha1.RayHistoryServer, secret *corev1.Secret) {
			secret.Data["AWS_SECRET_ACCESS_KEY"] = nil
		}, "nonempty key AWS_SECRET_ACCESS_KEY"},
		{"missing s3", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage.S3 = nil
		}, "bucket and storage.s3.region"},
		{"missing bucket", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage.S3.Bucket = ""
		}, "bucket and storage.s3.region"},
		{"missing region", func(_ *RayHistoryServerController, hs *rayv1alpha1.RayHistoryServer, _ *corev1.Secret) {
			hs.Spec.Storage.S3.Region = ""
		}, "bucket and storage.s3.region"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs, source := historyServerTestCR(), historyServerTestSource()
			hs.Spec.Storage.CredentialsSecret = &corev1.SecretReference{Name: source.Name}
			r := &RayHistoryServerController{Namespace: "operator", Image: "history:test"}
			tc.change(r, hs, source)
			c := historyServerTestClient(t, source)
			r.APIReader = c
			_, err := r.storageInputs(context.Background(), hs)
			require.ErrorContains(t, err, tc.want)
			for _, credential := range source.Data {
				if len(credential) > 0 {
					require.NotContains(t, err.Error(), string(credential), "validation must not expose credentials")
				}
			}
		})
	}
}

func TestHistoryServerStorageS3CredentialsAndRotation(t *testing.T) {
	ctx := context.Background()
	hs, source := historyServerTestCR(), historyServerTestSource()
	hs.Spec.Storage.RootDir = "sessions/archive"
	hs.Spec.Storage.S3.Endpoint = "https://objects.example.test:9443"
	hs.Spec.Storage.CredentialsSecret = &corev1.SecretReference{Name: source.Name, Namespace: source.Namespace}
	// These keys must never override literal settings or become process configuration.
	source.Data["STORAGE_BACKEND"] = []byte("azureblob")
	source.Data["S3_BUCKET"] = []byte("attacker-bucket")
	source.Data["S3DISABLE_SSL"] = []byte("true")
	source.Data["LD_PRELOAD"] = []byte("/tmp/inject.so")
	c := historyServerTestClient(t, source)
	r := historyServerTestController(c)
	before := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), before))
	in, err := r.storageInputs(ctx, hs)
	require.NoError(t, err)
	env := historyServerTestEnv(t, in)
	for name, value := range map[string]string{"STORAGE_BACKEND": "s3", "STORAGE_ROOT_DIR": "sessions/archive", "S3_BUCKET": "history", "S3_REGION": "eu-west-1", "S3_ENDPOINT": "https://objects.example.test:9443", "S3DISABLE_SSL": "false"} {
		require.Equal(t, corev1.EnvVar{Name: name, Value: value}, env[name])
	}
	require.NotContains(t, env, "LD_PRELOAD")
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		require.Empty(t, env[name].Value)
		require.NotNil(t, env[name].ValueFrom)
		require.Equal(t, &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: source.Name}, Key: name}, env[name].ValueFrom.SecretKeyRef)
	}
	serialized, err := json.Marshal(in)
	require.NoError(t, err)
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		require.NotContains(t, string(serialized), string(source.Data[name]))
	}
	again, err := r.storageInputs(ctx, hs)
	require.NoError(t, err)
	require.Equal(t, in.Revision, again.Revision)
	after := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(source), after))
	require.Equal(t, before, after, "source Secret must remain untouched")
	after.Data["AWS_SECRET_ACCESS_KEY"] = []byte("rotated-private-key")
	require.NoError(t, c.Update(ctx, after))
	rotated, err := r.storageInputs(ctx, hs)
	require.NoError(t, err)
	require.NotEqual(t, in.Revision, rotated.Revision)
	require.Equal(t, in.Env, rotated.Env, "rotation changes the rollout revision, not credential references")
	// UID must distinguish a deleted and recreated source even if the version is reused.
	replacement := after.DeepCopy()
	replacement.UID = "replacement-source-uid"
	r.APIReader = historyServerTestClient(t, replacement)
	recreated, err := r.storageInputs(ctx, hs)
	require.NoError(t, err)
	require.NotEqual(t, rotated.Revision, recreated.Revision)
	// Without an explicit Secret, SDK ambient credentials remain available.
	hs.Spec.Storage.CredentialsSecret = nil
	ambient, err := r.storageInputs(ctx, hs)
	require.NoError(t, err)
	require.NotContains(t, historyServerTestEnv(t, ambient), "AWS_ACCESS_KEY_ID")
}

func TestHistoryServerStorageHTTPS(t *testing.T) {
	for _, endpoint := range []string{"http://objects.example.test", "//objects.example.test", "https://", "https://user:password@objects.example.test", "https://objects.example.test?token=secret", "https://objects.example.test#fragment", "://bad"} {
		t.Run(endpoint, func(t *testing.T) {
			hs := historyServerTestCR()
			hs.Spec.Storage.S3.Endpoint = endpoint
			_, err := historyServerTestController(historyServerTestClient(t)).storageInputs(context.Background(), hs)
			require.ErrorContains(t, err, "HTTPS URL")
			require.NotContains(t, err.Error(), endpoint)
		})
	}
}

func TestHistoryServerStorageGCS(t *testing.T) {
	for _, credentials := range []string{"", "invalid-json", `{"type":"service_account","private_key":"private-gcs-key"}`} {
		t.Run(credentials, func(t *testing.T) {
			hs, source := historyServerTestCR(), historyServerTestSource()
			hs.Spec.Storage = &rayv1alpha1.HistoryServerStorage{Type: rayv1alpha1.HistoryServerStorageGCS, GCS: &rayv1alpha1.HistoryServerGCSStorage{Bucket: "gcs-history"}, CredentialsSecret: &corev1.SecretReference{Name: source.Name}}
			source.Data = map[string][]byte{"credentials.json": []byte(credentials)}
			in, err := historyServerTestController(historyServerTestClient(t, source)).storageInputs(context.Background(), hs)
			if !json.Valid([]byte(credentials)) {
				require.ErrorContains(t, err, "valid JSON in credentials.json")
				return
			}
			require.NoError(t, err)
			env := historyServerTestEnv(t, in)
			require.Equal(t, "gcs", env["STORAGE_BACKEND"].Value)
			require.Equal(t, "gcs-history", env["GCS_BUCKET"].Value)
			require.Equal(t, "/etc/history-server/gcs/credentials.json", env["GOOGLE_APPLICATION_CREDENTIALS"].Value)
			require.Equal(t, []corev1.Volume{{Name: "gcs-credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: source.Name, Items: []corev1.KeyToPath{{Key: "credentials.json", Path: "credentials.json"}}}}}}, in.Volumes)
			require.Equal(t, []corev1.VolumeMount{{Name: "gcs-credentials", MountPath: "/etc/history-server/gcs", ReadOnly: true}}, in.VolumeMounts)
			encoded, err := json.Marshal(in)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "private-gcs-key")
		})
	}
}

func TestHistoryServerStorageAzure(t *testing.T) {
	for _, tc := range []struct {
		name, mode, account, connection, wantMode, wantError string
		servicePrincipal                                     bool
	}{
		{name: "infer connection string", connection: "DefaultEndpointsProtocol=https;AccountName=history;AccountKey=private-azure-key", wantMode: "connection_string"},
		{name: "explicit connection string", mode: " CONNECTION_STRING ", connection: "DefaultEndpointsProtocol=https;AccountName=history;AccountKey=private-azure-key", wantMode: "connection_string"},
		{name: "connection string missing", mode: "connection_string", wantError: "nonempty key AZURE_STORAGE_CONNECTION_STRING"},
		{name: "insecure connection string", connection: "DefaultEndpointsProtocol=http;AccountKey=private-azure-key", wantError: "HTTPS endpoints"},
		{name: "insecure blob endpoint", connection: "BlobEndpoint=http://storage.test;AccountKey=private-azure-key", wantError: "HTTPS endpoints"},
		{name: "ambient default", account: "https://history.blob.core.windows.net", wantMode: "default"},
		{name: "service principal", mode: "default", account: "https://history.blob.core.windows.net", servicePrincipal: true, wantMode: "default"},
		{name: "default account missing", mode: "default", wantError: "accountUrl is required"},
		{name: "insecure default", account: "http://history.blob.core.windows.net", wantError: "HTTPS URL"},
		{name: "unsupported mode", mode: "workload_identity", wantError: "currently supports"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs, source := historyServerTestCR(), historyServerTestSource()
			hs.Spec.Storage = &rayv1alpha1.HistoryServerStorage{Type: rayv1alpha1.HistoryServerStorageAzureBlob, AzureBlob: &rayv1alpha1.HistoryServerAzureBlobStorage{Container: "sessions", AccountURL: tc.account, AuthMode: tc.mode}}
			source.Data = map[string][]byte{}
			if tc.connection != "" {
				source.Data["AZURE_STORAGE_CONNECTION_STRING"] = []byte(tc.connection)
			}
			if tc.servicePrincipal {
				for _, name := range []string{"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"} {
					source.Data[name] = []byte("private-" + name)
				}
			}
			if len(source.Data) > 0 {
				hs.Spec.Storage.CredentialsSecret = &corev1.SecretReference{Name: source.Name}
			}
			in, err := historyServerTestController(historyServerTestClient(t, source)).storageInputs(context.Background(), hs)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.NotContains(t, err.Error(), "private-azure-key")
				return
			}
			require.NoError(t, err)
			env := historyServerTestEnv(t, in)
			require.Equal(t, "azureblob", env["STORAGE_BACKEND"].Value)
			require.Equal(t, tc.wantMode, env["AZURE_STORAGE_AUTH_MODE"].Value)
			require.Equal(t, "sessions", env["AZURE_STORAGE_CONTAINER"].Value)
			if tc.wantMode == "default" {
				require.Equal(t, tc.account, env["AZURE_STORAGE_ACCOUNT_URL"].Value)
			}
			for name := range source.Data {
				require.Empty(t, env[name].Value)
				require.NotNil(t, env[name].ValueFrom)
				require.Equal(t, name, env[name].ValueFrom.SecretKeyRef.Key)
				require.Equal(t, source.Name, env[name].ValueFrom.SecretKeyRef.Name)
			}
		})
	}
}
