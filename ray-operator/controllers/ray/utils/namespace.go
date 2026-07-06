package utils

import (
	"os"

	routev1 "github.com/openshift/api/route/v1"
	"k8s.io/apimachinery/pkg/api/meta"
)

// GetPlatformNamespace returns the platform namespace where shared resources should be created.
// This is used for placing HTTPRoutes and other resources that need to be in a trusted namespace.
//
// Returns: (namespace string, isOpenShift bool)
//
// Detection logic:
// - OpenShift: APPLICATION_NAMESPACE (from ODH operator) → POD_NAMESPACE → ODH/RHODS fallback
// - Non-OpenShift: POD_NAMESPACE → ray-system fallback
func GetPlatformNamespace(restMapper meta.RESTMapper) (string, bool) {
	isOpenShift := isOpenShiftCluster(restMapper)

	// On OpenShift, use stricter namespace detection
	if isOpenShift {
		// 1. Check APPLICATION_NAMESPACE env var (set by ODH operator via params.env)
		if appNs := os.Getenv("APPLICATION_NAMESPACE"); appNs != "" {
			return appNs, true
		}

		// 2. Fallback to POD_NAMESPACE
		if podNs := os.Getenv("POD_NAMESPACE"); podNs != "" {
			return podNs, true
		}

		// 3. Final fallback for OpenShift (prefer redhat-ods-applications as primary)
		return "redhat-ods-applications", true
	}

	// Non-OpenShift: simpler fallback chain
	if podNs := os.Getenv("POD_NAMESPACE"); podNs != "" {
		return podNs, false
	}

	return "ray-system", false
}

// isOpenShiftCluster checks if the cluster is running on OpenShift
// by checking for the existence of the Route API
func isOpenShiftCluster(restMapper meta.RESTMapper) bool {
	if restMapper == nil {
		return false
	}
	gvk := routev1.GroupVersion.WithKind("Route")
	_, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// GetGatewayNamespace returns the namespace where the Gateway resource is located.
// In OpenShift AI/ODH environments, this is typically "openshift-ingress".
func GetGatewayNamespace() string {
	if gatewayNs := os.Getenv("GATEWAY_NAMESPACE"); gatewayNs != "" {
		return gatewayNs
	}
	return "openshift-ingress"
}

// GetGatewayName returns the name of the Gateway resource to use.
// In OpenShift AI/ODH environments, this is typically "data-science-gateway".
func GetGatewayName() string {
	if gatewayName := os.Getenv("GATEWAY_NAME"); gatewayName != "" {
		return gatewayName
	}
	return "data-science-gateway"
}
