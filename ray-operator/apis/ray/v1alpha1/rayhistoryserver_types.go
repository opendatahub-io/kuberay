package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HistoryServerStorageType is the object storage backend for History Server and collectors.
// +kubebuilder:validation:Enum=s3;gcs;azureBlob
type HistoryServerStorageType string

const (
	HistoryServerStorageS3        HistoryServerStorageType = "s3"
	HistoryServerStorageGCS       HistoryServerStorageType = "gcs"
	HistoryServerStorageAzureBlob HistoryServerStorageType = "azureBlob"
)

// RayHistoryServerConditionType is the type of a RayHistoryServer condition.
type RayHistoryServerConditionType string

const (
	// RayHistoryServerConfigurationValid indicates the spec is structurally valid and references resolve.
	RayHistoryServerConfigurationValid RayHistoryServerConditionType = "ConfigurationValid"
	// RayHistoryServerDeploymentAvailable indicates the History Server workload is available.
	RayHistoryServerDeploymentAvailable RayHistoryServerConditionType = "DeploymentAvailable"
	// RayHistoryServerReady indicates the History Server is ready to serve traffic.
	RayHistoryServerReady RayHistoryServerConditionType = "Ready"
	// RayHistoryServerDegraded indicates partial failure or reduced capability.
	RayHistoryServerDegraded RayHistoryServerConditionType = "Degraded"
)

// RayHistoryServerSpec defines the desired state of RayHistoryServer.
//
// +kubebuilder:validation:XValidation:rule="has(self.storage)",message="storage is required"
// +kubebuilder:validation:XValidation:rule="!has(self.storage) || self.storage.type != 's3' || has(self.storage.s3)",message="storage.s3 is required when storage.type is s3"
// +kubebuilder:validation:XValidation:rule="!has(self.storage) || self.storage.type != 'gcs' || has(self.storage.gcs)",message="storage.gcs is required when storage.type is gcs"
// +kubebuilder:validation:XValidation:rule="!has(self.storage) || self.storage.type != 'azureBlob' || has(self.storage.azureBlob)",message="storage.azureBlob is required when storage.type is azureBlob"
// +kubebuilder:validation:XValidation:rule="!has(self.storage) || self.storage.type != 's3' || !has(self.storage.s3) || size(self.storage.s3.region) > 0",message="storage.s3.region is required when storage.type is s3"
type RayHistoryServerSpec struct {
	// Storage configures object storage shared by the History Server and RayCluster collectors.
	Storage *HistoryServerStorage `json:"storage"`

	// Security configures TLS, proxy, and isolation for the History Server API.
	// +optional
	Security *HistoryServerSecurity `json:"security,omitempty"`

	// Deployment configures the History Server Kubernetes workload.
	// +optional
	Deployment *HistoryServerDeployment `json:"deployment,omitempty"`
}

// HistoryServerStorage defines object storage settings for History Server data.
type HistoryServerStorage struct {
	// Type selects the storage backend implementation.
	Type HistoryServerStorageType `json:"type"`

	// RootDir is the logical prefix inside the backend where session data is stored.
	// +optional
	RootDir string `json:"rootDir,omitempty"`

	// CredentialsSecret references a Secret in the same namespace as the History Server
	// operands that holds backend credentials (for example keys consumed via collector env).
	// +optional
	CredentialsSecret *corev1.SecretReference `json:"credentialsSecret,omitempty"`

	// S3 holds Amazon S3–compatible configuration when type is s3.
	// +optional
	S3 *HistoryServerS3Storage `json:"s3,omitempty"`

	// GCS holds Google Cloud Storage configuration when type is gcs.
	// +optional
	GCS *HistoryServerGCSStorage `json:"gcs,omitempty"`

	// AzureBlob holds Azure Blob Storage configuration when type is azureBlob.
	// +optional
	AzureBlob *HistoryServerAzureBlobStorage `json:"azureBlob,omitempty"`
}

// HistoryServerS3Storage configures an S3-compatible backend.
type HistoryServerS3Storage struct {
	// Bucket is the target bucket name.
	Bucket string `json:"bucket"`
	// Endpoint is the optional custom S3 endpoint URL.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// Region is the AWS region (required for AWS and many S3-compatible stores).
	Region string `json:"region"`
}

// HistoryServerGCSStorage configures Google Cloud Storage.
type HistoryServerGCSStorage struct {
	// Bucket is the GCS bucket name.
	Bucket string `json:"bucket"`
}

// HistoryServerAzureBlobStorage configures Azure Blob Storage.
type HistoryServerAzureBlobStorage struct {
	// Container is the blob container name.
	Container string `json:"container"`
	// AccountURL is the storage account URL (workload identity / default credential flows).
	// +optional
	AccountURL string `json:"accountUrl,omitempty"`
	// AuthMode selects how credentials are obtained (for example connection_string or workload_identity).
	// +optional
	AuthMode string `json:"authMode,omitempty"`
}

// HistoryServerSecurity defines security-related History Server settings.
type HistoryServerSecurity struct {
	// TLS configures serving certificates for the History Server Service.
	// +optional
	TLS *HistoryServerTLS `json:"tls,omitempty"`

	// KubeRBACProxy enables the kube-rbac-proxy sidecar in front of the History Server API.
	// +optional
	KubeRBACProxy *HistoryServerKubeRBACProxy `json:"kubeRBACProxy,omitempty"`

	// CABundle is an optional PEM bundle used to verify clients or upstream TLS when configuring proxies.
	// +optional
	CABundle []byte `json:"caBundle,omitempty"`

	// LiveClusterProxying enables proxying of live RayCluster dashboard traffic through the History Server.
	// +kubebuilder:default=false
	// +optional
	LiveClusterProxying *bool `json:"liveClusterProxying,omitempty"`
}

// HistoryServerTLS configures cert-manager or static TLS material for the History Server.
type HistoryServerTLS struct {
	// IssuerRef references a cert-manager Issuer or ClusterIssuer.
	// +optional
	IssuerRef *corev1.TypedLocalObjectReference `json:"issuerRef,omitempty"`

	// SecretName is the TLS certificate Secret mounted by the History Server pods.
	// +optional
	SecretName string `json:"secretName,omitempty"`
}

// HistoryServerKubeRBACProxy toggles the optional kube-rbac-proxy sidecar.
type HistoryServerKubeRBACProxy struct {
	// Enabled controls whether kube-rbac-proxy is deployed with the History Server.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// HistoryServerDeployment configures the History Server Deployment.
type HistoryServerDeployment struct {
	// Replicas is the desired number of History Server pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources sets compute resources for History Server containers.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// RayHistoryServerStatus defines the observed state of RayHistoryServer.
type RayHistoryServerStatus struct {
	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the History Server state.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:storageversion
//+kubebuilder:resource:scope=Cluster,categories=all,shortName=rhs
//+kubebuilder:validation:XValidation:rule="self.metadata.name == 'default'",message="RayHistoryServer is a singleton; metadata.name must be 'default'"
//+kubebuilder:printcolumn:name="Storage",type=string,JSONPath=`.spec.storage.type`,priority=0
//+kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`,priority=0
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`,priority=0
// +genclient
// +genclient:nonNamespaced

// RayHistoryServer is the Schema for the cluster-scoped History Server singleton.
type RayHistoryServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RayHistoryServerSpec   `json:"spec,omitempty"`
	Status RayHistoryServerStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RayHistoryServerList contains a list of RayHistoryServer.
type RayHistoryServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RayHistoryServer `json:"items"`
}
