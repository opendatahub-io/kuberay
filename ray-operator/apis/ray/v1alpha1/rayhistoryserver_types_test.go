package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestRayHistoryServerSpec_StorageRequired(t *testing.T) {
	spec := RayHistoryServerSpec{}
	errs := validateRayHistoryServerSpec(&spec, field.NewPath("spec"))
	require.NotEmpty(t, errs)
}

func TestRayHistoryServerSpec_BackendConfigRequired(t *testing.T) {
	spec := RayHistoryServerSpec{
		Storage: &HistoryServerStorage{
			Type: HistoryServerStorageS3,
		},
	}
	errs := validateRayHistoryServerSpec(&spec, field.NewPath("spec"))
	require.NotEmpty(t, errs)
}

func TestRayHistoryServerSpec_S3RegionRequired(t *testing.T) {
	spec := RayHistoryServerSpec{
		Storage: &HistoryServerStorage{
			Type: HistoryServerStorageS3,
			S3: &HistoryServerS3Storage{
				Bucket: "my-bucket",
			},
		},
	}
	errs := validateRayHistoryServerSpec(&spec, field.NewPath("spec"))
	require.NotEmpty(t, errs)
}

// validateRayHistoryServerSpec mirrors CRD CEL rules for unit testing until envtest CRD admission tests land.
func validateRayHistoryServerSpec(spec *RayHistoryServerSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Storage == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("storage"), "storage is required"))
		return allErrs
	}
	switch spec.Storage.Type {
	case HistoryServerStorageS3:
		if spec.Storage.S3 == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("storage").Child("s3"), "s3 configuration is required when storage.type is s3"))
			return allErrs
		}
		if spec.Storage.S3.Region == "" {
			allErrs = append(allErrs, field.Required(fldPath.Child("storage").Child("s3").Child("region"), "region is required when storage.type is s3"))
		}
	case HistoryServerStorageGCS:
		if spec.Storage.GCS == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("storage").Child("gcs"), "gcs configuration is required when storage.type is gcs"))
		}
	case HistoryServerStorageAzureBlob:
		if spec.Storage.AzureBlob == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("storage").Child("azureBlob"), "azureBlob configuration is required when storage.type is azureBlob"))
		}
	}
	return allErrs
}
