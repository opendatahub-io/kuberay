package utils

import (
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// AuthenticationMode represents the type of authentication configured in the cluster
type AuthenticationMode string

const (
	// ModeIntegratedOAuth represents OpenShift's integrated OAuth authentication
	ModeIntegratedOAuth AuthenticationMode = "IntegratedOAuth"
	// ModeOIDC represents OIDC-based authentication
	ModeOIDC AuthenticationMode = "OIDC"
)

// DetectAuthenticationMode determines whether the cluster is using OAuth or OIDC
// Returns IntegratedOAuth by default when no specific authentication is configured
func DetectAuthenticationMode(isOpenShift bool) AuthenticationMode {
	// First, check if we're on OpenShift
	if !isOpenShift {
		// Default to IntegratedOAuth for non-OpenShift clusters
		return ModeIntegratedOAuth
	}

	// Default to IntegratedOAuth when no specific authentication is configured
	return ModeIntegratedOAuth
}

// ShouldEnableOAuth determines if OAuth should be enabled based on the cluster annotation and authentication mode
// Returns true only if the cluster has the enable-authentication annotation set to "true" AND the authentication mode is IntegratedOAuth
func ShouldEnableOAuth(cluster *rayv1.RayCluster, authMode AuthenticationMode) bool {
	if cluster == nil || cluster.Annotations == nil {
		return false
	}
	enableAuth := cluster.Annotations[EnableSecureTrustedNetworkAnnotationKey]
	return enableAuth == "true" && authMode == ModeIntegratedOAuth
}

// ShouldEnableOIDC determines if OIDC should be enabled based on the cluster annotation and authentication mode
// Returns true only if the cluster has the enable-authentication annotation set to "true" AND the authentication mode is OIDC
func ShouldEnableOIDC(cluster *rayv1.RayCluster, authMode AuthenticationMode) bool {
	if cluster == nil || cluster.Annotations == nil {
		return false
	}
	enableAuth := cluster.Annotations[EnableSecureTrustedNetworkAnnotationKey]
	return enableAuth == "true" && authMode == ModeOIDC
}
