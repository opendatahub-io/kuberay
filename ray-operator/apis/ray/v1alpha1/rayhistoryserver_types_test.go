package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestRayHistoryServerSpec_StorageRequiredWhenEnabled(t *testing.T) {
	enabled := true
	spec := RayHistoryServerSpec{Enabled: &enabled}
	errs := validateRayHistoryServerSpec(&spec, field.NewPath("spec"))
	require.NotEmpty(t, errs)
}

func TestRayHistoryServerSpec_BackendConfigRequired(t *testing.T) {
	enabled := true
	spec := RayHistoryServerSpec{
		Enabled: &enabled,
		Storage: &HistoryServerStorage{
			Type: HistoryServerStorageS3,
		},
	}
	errs := validateRayHistoryServerSpec(&spec, field.NewPath("spec"))
	require.NotEmpty(t, errs)
}

// validateRayHistoryServerSpec mirrors CRD CEL rules for unit testing until envtest CRD admission tests land.
func validateRayHistoryServerSpec(spec *RayHistoryServerSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Enabled != nil && *spec.Enabled {
		if spec.Storage == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("storage"), "storage is required when enabled is true"))
			return allErrs
		}
		switch spec.Storage.Type {
		case HistoryServerStorageS3:
			if spec.Storage.S3 == nil {
				allErrs = append(allErrs, field.Required(fldPath.Child("storage").Child("s3"), "s3 configuration is required when storage.type is s3"))
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
	}
	return allErrs
}
