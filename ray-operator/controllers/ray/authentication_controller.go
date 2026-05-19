package ray

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/go-logr/logr"
	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// Backoff and retry constants for OIDC rollout coordination
const (
	// MediumRequeueDelay for ongoing rollouts
	MediumRequeueDelay = 10 * time.Second

	// SafetyRequeueDelay provides periodic re-reconciliation to detect drift in
	// cross-namespace resources (HTTPRoute, ReferenceGrant) that cannot use owner
	// references and therefore don't trigger watch-based reconciliation.
	SafetyRequeueDelay = 5 * time.Minute

	// ReconcileTimeout prevents a single reconcile from blocking the worker indefinitely
	ReconcileTimeout = 60 * time.Second

	// Finalizer for ensuring cleanup of cross-namespace resources (HTTPRoutes)
	authenticationFinalizer = "ray.io/authentication-resources"

	// OAuth proxy constants
	oauthProxyContainerName = "oauth-proxy"
	oauthProxyVolumeName    = "proxy-tls-secret"
	authProxyPort           = 8443
	oauthProxyPortName      = "oauth-proxy"

	oauthConfigVolumeName = "oauth-config"

	oidcProxyContainerName  = "kube-rbac-proxy"
	oidcProxyPortName       = "https"
	oauthProxyImage         = "registry.redhat.io/openshift4/ose-oauth-proxy:latest"
	oidcProxyContainerImage = "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9@sha256:11828cdb31cd9c1e15bc9e31c7e4669daf71c84c028cad2df5dbab68150da273"
)

// AuthenticationController is a completely independent controller that watches authentication-related
// resources (ConfigMaps, ServiceAccounts, OpenShift Routes) and manages authentication configurations for Ray clusters on Openshift.
type AuthenticationController struct {
	client.Client
	Recorder   record.EventRecorder
	Scheme     *runtime.Scheme
	RESTMapper meta.RESTMapper
	options    RayClusterReconcilerOptions
}

// NewAuthenticationController creates a new authentication controller
func NewAuthenticationController(mgr manager.Manager, options RayClusterReconcilerOptions) *AuthenticationController {
	return &AuthenticationController{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		RESTMapper: mgr.GetRESTMapper(),
		Recorder:   mgr.GetEventRecorderFor("authentication-controller"),
		options:    options,
	}
}

// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=operator.openshift.io,resources=kubeapiservers,verbs=get;list;watch
// +kubebuilder:rbac:groups=operator.openshift.io,resources=kubeapiservers/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.openshift.io,resources=authentications,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.openshift.io,resources=authentications/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.openshift.io,resources=oauths,verbs=get;list;watch
// +kubebuilder:rbac:groups=config.openshift.io,resources=oauths/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=referencegrants,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles authentication-related resources and manages OAuth sidecar injection
func (r *AuthenticationController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, ReconcileTimeout)
	defer cancel()

	logger := ctrl.LoggerFrom(ctx).WithName("authentication-controller")
	logger.Info("Reconciling Authentication", "namespacedName", req.NamespacedName)

	// Get the RayCluster - this should always be a RayCluster now due to proper mapping
	rayCluster := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, rayCluster); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("RayCluster not found, likely deleted - checking for orphaned resources")
			// Clean up any orphaned ReferenceGrants in this namespace
			if err := r.cleanupOrphanedReferenceGrant(ctx, req.Namespace, logger); err != nil {
				logger.Info("Failed to cleanup orphaned ReferenceGrant (non-fatal)", "error", err)
			}
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get RayCluster")
		return ctrl.Result{RequeueAfter: MediumRequeueDelay}, err
	}

	// Handle deletion with finalizer
	if !rayCluster.DeletionTimestamp.IsZero() {
		logger.Info("RayCluster is being deleted, running finalizer", "cluster", rayCluster.Name)
		return r.handleDeletion(ctx, rayCluster, logger)
	}

	// Skip if managed by external controller
	if manager := utils.ManagedByExternalController(rayCluster.Spec.ManagedBy); manager != nil {
		logger.Info("Skipping RayCluster managed by external controller", "managed-by", manager)
		return ctrl.Result{}, nil
	}

	// Detect the authentication mode configured in the cluster
	authMode := utils.DetectAuthenticationMode(r.options.IsOpenShift)
	logger.Info("Detected authentication mode", "mode", authMode, "cluster", rayCluster.Name)

	// Handle authentication based on detected mode
	// Both OAuth and OIDC modes use the same OIDC configuration (kube-rbac-proxy + HTTPRoute)
	var err error
	switch authMode {
	case utils.ModeIntegratedOAuth:
		logger.Info("Handling Integrated OAuth with OIDC configuration", "cluster", rayCluster.Name)
		err = r.handleOIDCConfiguration(ctx, req, authMode, logger)

	case utils.ModeOIDC:
		logger.Info("Handling OIDC", "cluster", rayCluster.Name)
		err = r.handleOIDCConfiguration(ctx, req, authMode, logger)
	}

	if err != nil {
		logger.Error(err, "Failed to handle authentication configuration")
		return ctrl.Result{RequeueAfter: MediumRequeueDelay}, err
	}

	logger.Info("Successfully reconciled authentication", "cluster", rayCluster.Name)
	// Periodic re-reconcile to detect drift in cross-namespace resources (HTTPRoute,
	// ReferenceGrant) that lack owner references and won't trigger watch events.
	return ctrl.Result{RequeueAfter: SafetyRequeueDelay}, nil
}

// handleDeletion handles cleanup when a RayCluster is being deleted (finalizer pattern)
func (r *AuthenticationController) handleDeletion(ctx context.Context, cluster *rayv1.RayCluster, logger logr.Logger) (ctrl.Result, error) {
	// Check if our finalizer is present
	if !controllerutil.ContainsFinalizer(cluster, authenticationFinalizer) {
		logger.Info("Finalizer not present, skipping authentication cleanup")
		return ctrl.Result{}, nil
	}

	logger.Info("Running authentication resource cleanup (finalizer)", "cluster", cluster.Name)

	// Detect auth mode and run cleanup
	authMode := utils.DetectAuthenticationMode(r.options.IsOpenShift)
	if err := r.cleanupOIDCResources(ctx, cluster, authMode, logger); err != nil {
		logger.Error(err, "Failed to cleanup authentication resources")
		return ctrl.Result{RequeueAfter: MediumRequeueDelay}, err
	}

	// Remove the finalizer
	controllerutil.RemoveFinalizer(cluster, authenticationFinalizer)
	if err := r.Update(ctx, cluster); err != nil {
		logger.Error(err, "Failed to remove finalizer")
		return ctrl.Result{RequeueAfter: MediumRequeueDelay}, err
	}

	logger.Info("Authentication cleanup complete, finalizer removed", "cluster", cluster.Name)
	return ctrl.Result{}, nil
}

// handleOIDCConfiguration configures OIDC for RayClusters (supports both OIDC and OAuth modes)
func (r *AuthenticationController) handleOIDCConfiguration(ctx context.Context, req ctrl.Request, authMode utils.AuthenticationMode, logger logr.Logger) error {
	// Try to get RayCluster
	rayCluster := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, rayCluster); err != nil {
		// Not a RayCluster or doesn't exist, skip
		return client.IgnoreNotFound(err)
	}

	// Check if authentication should be enabled for this cluster (supports both OAuth and OIDC modes)
	shouldEnable := utils.ShouldEnableOIDC(rayCluster, authMode) || utils.ShouldEnableOAuth(rayCluster, authMode)
	if !shouldEnable {
		logger.Info("Authentication not requested for this cluster", "cluster", rayCluster.Name, "mode", authMode)

		// Set AuthenticationReady to True since auth is disabled (no resources needed)
		meta.SetStatusCondition(&rayCluster.Status.Conditions, metav1.Condition{
			Type:               string(rayv1.AuthenticationReady),
			Status:             metav1.ConditionTrue,
			Reason:             utils.AuthenticationDisabled,
			Message:            "Authentication not enabled for this cluster",
			ObservedGeneration: rayCluster.Generation,
		})
		if err := r.Status().Update(ctx, rayCluster); err != nil {
			return fmt.Errorf("failed to update AuthenticationReady condition when auth disabled: %w", err)
		}

		// Remove finalizer if present (auth is disabled)
		if controllerutil.ContainsFinalizer(rayCluster, authenticationFinalizer) {
			logger.Info("Removing authentication finalizer (auth disabled)", "cluster", rayCluster.Name)
			if err := r.cleanupOIDCResources(ctx, rayCluster, authMode, logger); err != nil {
				return err
			}
			controllerutil.RemoveFinalizer(rayCluster, authenticationFinalizer)
			if err := r.Update(ctx, rayCluster); err != nil {
				return fmt.Errorf("failed to remove finalizer: %w", err)
			}
		}
		return nil
	}

	// On OpenShift, enforce enableIngress: false for Gateway-only access
	// This handles both new clusters (webhook sets it) and existing clusters (upgrade scenario)
	// HTTPRoute creation is independent of enableIngress
	// enableIngress controls basic Route/Ingress creation (which may be disabled by webhooks in ODH)
	if r.options.IsOpenShift {
		if err := r.enforceEnableIngressFalseOnOpenShift(ctx, rayCluster, logger); err != nil {
			return err
		}
		// Re-fetch after spec update to get the latest resourceVersion
		if err := r.Get(ctx, req.NamespacedName, rayCluster); err != nil {
			return fmt.Errorf("failed to re-fetch after enableIngress update: %w", err)
		}
	}

	// Add finalizer to ensure cleanup happens before cluster deletion
	if !controllerutil.ContainsFinalizer(rayCluster, authenticationFinalizer) {
		logger.Info("Adding authentication finalizer to RayCluster", "cluster", rayCluster.Name)
		controllerutil.AddFinalizer(rayCluster, authenticationFinalizer)
		if err := r.Update(ctx, rayCluster); err != nil {
			return fmt.Errorf("failed to add finalizer: %w", err)
		}
		// Re-fetch the cluster after the update to get the latest resourceVersion
		// This is necessary because ensureOIDCResources and the status update need the current version
		if err := r.Get(ctx, req.NamespacedName, rayCluster); err != nil {
			return fmt.Errorf("failed to re-fetch cluster after adding finalizer: %w", err)
		}
		// Continue to create resources immediately - don't return early
		// This avoids a race where RayClusterController tries to create pods before ServiceAccount exists
	}

	// Ensure OIDC resources exist (only after finalizer is present)
	if err := r.ensureOIDCResources(ctx, rayCluster, authMode, logger); err != nil {
		// Set condition to False on error
		meta.SetStatusCondition(&rayCluster.Status.Conditions, metav1.Condition{
			Type:               string(rayv1.AuthenticationReady),
			Status:             metav1.ConditionFalse,
			Reason:             utils.AuthenticationFailed,
			Message:            fmt.Sprintf("Failed to create authentication resources: %v", err),
			ObservedGeneration: rayCluster.Generation,
		})
		if updateErr := r.Status().Update(ctx, rayCluster); updateErr != nil {
			logger.Error(updateErr, "Failed to update AuthenticationReady condition")
		}
		return fmt.Errorf("failed to ensure OIDC resources: %w", err)
	}

	// Set condition to True after successful creation
	meta.SetStatusCondition(&rayCluster.Status.Conditions, metav1.Condition{
		Type:               string(rayv1.AuthenticationReady),
		Status:             metav1.ConditionTrue,
		Reason:             utils.AuthenticationResourcesCreated,
		Message:            fmt.Sprintf("Authentication resources created successfully (mode: %s)", authMode),
		ObservedGeneration: rayCluster.Generation,
	})

	if err := r.Status().Update(ctx, rayCluster); err != nil {
		return fmt.Errorf("authentication resources created but failed to update AuthenticationReady condition: %w", err)
	}

	eventMsg := fmt.Sprintf("Authentication configured for RayCluster (mode: %s)", authMode)
	r.Recorder.Event(rayCluster, "Normal", "AuthenticationConfigured", eventMsg)
	return nil
}

func (r *AuthenticationController) ensureOIDCResources(ctx context.Context, cluster *rayv1.RayCluster, authMode utils.AuthenticationMode, logger logr.Logger) error {
	if err := r.ensureServiceAccount(ctx, cluster, authMode, logger); err != nil {
		return fmt.Errorf("failed to ensure service account: %w", err)
	}

	// Create ReferenceGrant BEFORE HTTPRoute to enable cross-namespace service references
	if err := r.ensureReferenceGrant(ctx, cluster, logger); err != nil {
		return fmt.Errorf("failed to ensure ReferenceGrant: %w", err)
	}

	// Create HttpRoute (in platform namespace)
	if err := r.ensureHttpRoute(ctx, cluster, logger); err != nil {
		return fmt.Errorf("failed to ensure HttpRoute: %w", err)
	}

	// MIGRATION: Clean up old HTTPRoute from cluster namespace if it exists
	// This handles migration from the old implementation where HTTPRoutes were created in the cluster namespace
	// We do this AFTER creating the new HTTPRoute to ensure no downtime
	if err := r.cleanupOldHTTPRouteFromClusterNamespace(ctx, cluster, logger); err != nil {
		logger.Info("Failed to cleanup old HTTPRoute (non-fatal)", "error", err)
		// Don't fail reconciliation if old cleanup fails - it's a migration step
	}

	// Create ConfigMap
	if err := r.ensureOIDCConfigMap(ctx, cluster, logger); err != nil {
		return fmt.Errorf("failed to ensure ConfigMap: %w", err)
	}

	return nil
}

func (r *AuthenticationController) cleanupOIDCResources(ctx context.Context, cluster *rayv1.RayCluster, authMode utils.AuthenticationMode, logger logr.Logger) error {
	namer := utils.NewResourceNamer(cluster)
	platformNamespace, _ := utils.GetPlatformNamespace(r.RESTMapper)

	// STEP 1: Remove cross-namespace access FIRST (security-first approach)
	// Delete ReferenceGrant if this is the last cluster with auth in the namespace
	// This revokes cross-namespace trust before cleaning up other resources
	if err := r.cleanupReferenceGrant(ctx, cluster, authMode, logger); err != nil {
		logger.Info("Failed to cleanup ReferenceGrant", "error", err)
	}

	// STEP 2: Remove HTTPRoutes (both new and old locations)
	// Remove HTTPRoute from platform namespace (using label selector)
	httpRouteList := &gatewayv1.HTTPRouteList{}
	listOpts := []client.ListOption{
		client.InNamespace(platformNamespace),
		client.MatchingLabels{
			"ray.io/cluster-namespace": cluster.Namespace,
			"ray.io/cluster-name":      cluster.Name,
		},
	}

	if err := r.List(ctx, httpRouteList, listOpts...); err == nil {
		for i := range httpRouteList.Items {
			httpRoute := &httpRouteList.Items[i]
			if err := r.Delete(ctx, httpRoute); err != nil {
				logger.Info("Failed to delete HTTPRoute", "error", err, "name", httpRoute.Name)
			} else {
				logger.Info("Deleted HTTPRoute", "name", httpRoute.Name, "namespace", platformNamespace)
			}
		}
	} else {
		logger.Info("Failed to list HTTPRoutes for cleanup", "error", err)
	}

	// MIGRATION: Also remove old HTTPRoute from cluster namespace (if it exists from previous implementation)
	// Old implementation created HTTPRoute with name matching cluster name in the cluster's own namespace
	oldHttpRoute := &gatewayv1.HTTPRoute{}
	oldHttpRouteName := cluster.Name // Old naming convention
	if err := r.Get(ctx, client.ObjectKey{Name: oldHttpRouteName, Namespace: cluster.Namespace}, oldHttpRoute); err == nil {
		if err := r.Delete(ctx, oldHttpRoute); err != nil {
			logger.Info("Failed to delete old HTTPRoute from cluster namespace", "error", err, "name", oldHttpRouteName)
		} else {
			logger.Info("Deleted old HTTPRoute from cluster namespace (migration)", "name", oldHttpRouteName, "namespace", cluster.Namespace)
		}
	}

	// STEP 3: Remove namespace-scoped resources
	// Remove ConfigMap
	configMap := &corev1.ConfigMap{}
	configMapName := namer.ConfigMapName()
	if err := r.Get(ctx, client.ObjectKey{Name: configMapName, Namespace: cluster.Namespace}, configMap); err == nil {
		if err := r.Delete(ctx, configMap); err != nil {
			logger.Info("Failed to delete ConfigMap", "error", err)
		} else {
			logger.Info("Deleted ConfigMap", "configMap", configMapName)
		}
	}

	// Remove service account
	sa := &corev1.ServiceAccount{}
	saName := namer.ServiceAccountName(authMode)
	if err := r.Get(ctx, client.ObjectKey{Name: saName, Namespace: cluster.Namespace}, sa); err == nil {
		if err := r.Delete(ctx, sa); err != nil {
			logger.Info("Failed to delete service account", "error", err)
		} else {
			logger.Info("Deleted service account", "serviceAccount", saName, "mode", authMode)
		}
	}

	return nil
}

func (r *AuthenticationController) ensureHttpRoute(ctx context.Context, cluster *rayv1.RayCluster, logger logr.Logger) error {
	serviceName, err := utils.GenerateHeadServiceName("RayCluster", cluster.Spec, cluster.Name)
	if err != nil {
		return err
	}

	// Get platform namespace where HTTPRoute should be created
	platformNamespace, _ := utils.GetPlatformNamespace(r.RESTMapper)
	gatewayNamespace := utils.GetGatewayNamespace()
	gatewayName := utils.GetGatewayName()

	// HTTPRoute name includes namespace to avoid conflicts in platform namespace
	// Must be a valid DNS label (max 63 characters) - use utility function for truncation
	baseRouteName := fmt.Sprintf("%s-%s", cluster.Namespace, cluster.Name)
	httpRouteName := utils.GenerateDNS1123Name(baseRouteName)

	httpRoute := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      httpRouteName,
			Namespace: platformNamespace, // Create in platform namespace, not cluster namespace
			Labels: map[string]string{
				"ray.io/cluster-namespace": cluster.Namespace,
				"ray.io/cluster-name":      cluster.Name,
				utils.RayClusterLabelKey:   cluster.Name,
			},
		},
	}

	opResult, err := controllerutil.CreateOrUpdate(ctx, r.Client, httpRoute, func() error {
		// NOTE: Cannot use SetControllerReference for cross-namespace ownership
		// Instead, we use labels and finalizers for cleanup
		// Add owner references in the same namespace if possible
		if platformNamespace == cluster.Namespace {
			if err := controllerutil.SetControllerReference(cluster, httpRoute, r.Scheme); err != nil {
				return err
			}
		}

		// Helper variables for pointer fields
		group := gatewayv1.Group("gateway.networking.k8s.io")
		kind := gatewayv1.Kind("Gateway")
		gatewayNameObj := gatewayv1.ObjectName(gatewayName)
		gatewayNamespaceObj := gatewayv1.Namespace(gatewayNamespace)
		serviceGroup := gatewayv1.Group("")
		serviceKind := gatewayv1.Kind("Service")
		serviceNamespace := gatewayv1.Namespace(cluster.Namespace) // Cross-namespace reference
		weight := int32(1)
		pathExact := gatewayv1.PathMatchExact
		pathPrefix := gatewayv1.PathMatchPathPrefix
		port := gatewayv1.PortNumber(8265)
		pathValue := "/"
		prefixValue := fmt.Sprintf("/ray/%s/%s", cluster.Namespace, cluster.Name)

		// Update the HTTPRoute spec
		httpRoute.Spec = gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{
					{
						Group:     &group,
						Kind:      &kind,
						Name:      gatewayNameObj,
						Namespace: &gatewayNamespaceObj,
					},
				},
			},
			Rules: []gatewayv1.HTTPRouteRule{
				// Rule 1: Exact match for root path - redirect to #/
				// This handles the case when users access /ray/{namespace}/{cluster} without the trailing hash
				{
					Matches: []gatewayv1.HTTPRouteMatch{
						{
							Path: &gatewayv1.HTTPPathMatch{
								Type:  &pathExact,
								Value: &prefixValue,
							},
						},
					},
					Filters: []gatewayv1.HTTPRouteFilter{
						{
							Type: gatewayv1.HTTPRouteFilterRequestRedirect,
							RequestRedirect: &gatewayv1.HTTPRequestRedirectFilter{
								Path: &gatewayv1.HTTPPathModifier{
									Type:            gatewayv1.FullPathHTTPPathModifier,
									ReplaceFullPath: ptr.To(prefixValue + "/#/"),
								},
								StatusCode: ptr.To(302), // Temporary redirect
							},
						},
					},
				},
				// Rule 2: Prefix match for all other paths (including #/)
				// This handles all sub-paths and rewrites them to the backend
				{
					Matches: []gatewayv1.HTTPRouteMatch{
						{
							Path: &gatewayv1.HTTPPathMatch{
								Type:  &pathPrefix,
								Value: &prefixValue,
							},
						},
					},
					BackendRefs: []gatewayv1.HTTPBackendRef{
						{
							BackendRef: gatewayv1.BackendRef{
								BackendObjectReference: gatewayv1.BackendObjectReference{
									Group:     &serviceGroup,
									Kind:      &serviceKind,
									Name:      gatewayv1.ObjectName(serviceName),
									Namespace: &serviceNamespace, // Cross-namespace reference enabled by ReferenceGrant
									Port:      &port,
								},
								Weight: &weight,
							},
						},
					},
					Filters: []gatewayv1.HTTPRouteFilter{
						{
							Type: gatewayv1.HTTPRouteFilterURLRewrite,
							URLRewrite: &gatewayv1.HTTPURLRewriteFilter{
								Path: &gatewayv1.HTTPPathModifier{
									Type:               gatewayv1.PrefixMatchHTTPPathModifier,
									ReplacePrefixMatch: &pathValue,
								},
							},
						},
					},
				},
			},
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update HTTPRoute: %w", err)
	}

	if opResult != controllerutil.OperationResultNone {
		logger.Info("HTTPRoute reconciled", "name", httpRoute.Name, "operation", opResult)
	}

	return nil
}

// cleanupOldHTTPRouteFromClusterNamespace removes HTTPRoutes that were created in the cluster namespace
// by the old implementation. This is a migration helper that runs during reconciliation.
func (r *AuthenticationController) cleanupOldHTTPRouteFromClusterNamespace(ctx context.Context, cluster *rayv1.RayCluster, logger logr.Logger) error {
	// Old implementation created HTTPRoute with name matching cluster name in the cluster's own namespace
	oldHttpRoute := &gatewayv1.HTTPRoute{}
	oldHttpRouteName := cluster.Name // Old naming convention

	err := r.Get(ctx, client.ObjectKey{Name: oldHttpRouteName, Namespace: cluster.Namespace}, oldHttpRoute)
	if err != nil {
		if errors.IsNotFound(err) {
			// No old HTTPRoute to clean up - this is the normal case
			logger.V(1).Info("No old HTTPRoute found in cluster namespace (already migrated or never existed)",
				"name", oldHttpRouteName,
				"namespace", cluster.Namespace)
			return nil
		}
		return fmt.Errorf("failed to get old HTTPRoute: %w", err)
	}

	// Old HTTPRoute exists - delete it
	logger.Info("Found old HTTPRoute in cluster namespace, deleting for migration",
		"name", oldHttpRouteName,
		"namespace", cluster.Namespace,
		"hasOwnerRef", len(oldHttpRoute.OwnerReferences) > 0)

	// Remove owner references if present (they might block deletion)
	if len(oldHttpRoute.OwnerReferences) > 0 {
		oldHttpRoute.OwnerReferences = nil
		if err := r.Update(ctx, oldHttpRoute); err != nil {
			logger.Info("Failed to remove owner references from old HTTPRoute", "error", err)
			// Continue anyway - try to delete
		}
	}

	if err := r.Delete(ctx, oldHttpRoute); err != nil {
		if errors.IsNotFound(err) {
			// Already deleted (race condition) - that's fine
			return nil
		}
		return fmt.Errorf("failed to delete old HTTPRoute: %w", err)
	}

	logger.Info("Deleted old HTTPRoute from cluster namespace (migration complete)",
		"name", oldHttpRouteName,
		"namespace", cluster.Namespace)
	r.Recorder.Event(cluster, "Normal", "HTTPRouteMigrated",
		fmt.Sprintf("Migrated HTTPRoute from %s to platform namespace", cluster.Namespace))

	return nil
}

// ensureReferenceGrant creates or updates a shared ReferenceGrant in the cluster's namespace.
//
// Design Decision: One Shared ReferenceGrant Per Namespace
// This creates a single ReferenceGrant per namespace, shared by all RayClusters in that namespace.
// The grant is only deleted when the last RayCluster with authentication is removed from the namespace.
//
// Why this approach:
//   - Semantically correct: Trust relationship is between namespaces, not individual clusters
//   - No resource redundancy: One grant regardless of cluster count
//   - Cleaner resource model: Single grant represents namespace-level permission
//   - Better UX: Users see one clear grant per namespace
//   - Minimal API churn: Grant created once per namespace, not per cluster
//
// Implementation notes:
//   - Uses consistent name: "kuberay-gateway-access"
//   - No owner reference (shared across clusters)
//   - Cleanup uses reference counting (see cleanupReferenceGrant)
//   - Idempotent: Multiple clusters can safely call ensureReferenceGrant
func (r *AuthenticationController) ensureReferenceGrant(ctx context.Context, cluster *rayv1.RayCluster, logger logr.Logger) error {
	platformNamespace, _ := utils.GetPlatformNamespace(r.RESTMapper)

	// Use a consistent name for the shared grant in this namespace
	grantName := "kuberay-gateway-access"

	referenceGrant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      grantName,
			Namespace: cluster.Namespace, // Grant lives in the target namespace (where Services are)
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "kuberay-operator",
				"app.kubernetes.io/component":  "gateway-access",
			},
		},
	}

	opResult, err := controllerutil.CreateOrUpdate(ctx, r.Client, referenceGrant, func() error {
		// NOTE: Do NOT set controller reference - this grant is shared across multiple clusters
		// Cleanup is handled by reference counting in cleanupReferenceGrant()

		// Configure ReferenceGrant to allow HTTPRoutes from platform namespace to reference Services in this namespace
		referenceGrant.Spec = gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{
					Group:     gatewayv1.GroupName,
					Kind:      "HTTPRoute",
					Namespace: gatewayv1.Namespace(platformNamespace),
				},
			},
			To: []gatewayv1beta1.ReferenceGrantTo{
				{
					Group: "", // Core API group for Services
					Kind:  "Service",
					// Name is optional - if omitted, allows all Services in this namespace
					// We leave it unset to allow any RayCluster service in the namespace
				},
			},
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update ReferenceGrant: %w", err)
	}

	if opResult != controllerutil.OperationResultNone {
		logger.Info("ReferenceGrant reconciled (shared)", "name", referenceGrant.Name, "namespace", cluster.Namespace, "operation", opResult)
	}

	return nil
}

// countClustersWithAuth counts RayClusters with authentication enabled in the namespace.
// If excludeCluster is provided, it will be excluded from the count.
func (r *AuthenticationController) countClustersWithAuth(ctx context.Context, namespace string, authMode utils.AuthenticationMode, excludeCluster *rayv1.RayCluster) (int, error) {
	clusterList := &rayv1.RayClusterList{}
	if err := r.List(ctx, clusterList, client.InNamespace(namespace)); err != nil {
		return 0, fmt.Errorf("failed to list RayClusters: %w", err)
	}

	count := 0
	for i := range clusterList.Items {
		c := &clusterList.Items[i]
		// Skip the excluded cluster if provided
		if excludeCluster != nil && c.Name == excludeCluster.Name {
			continue
		}
		// Check if this cluster has authentication enabled
		if utils.ShouldEnableOIDC(c, authMode) || utils.ShouldEnableOAuth(c, authMode) {
			count++
		}
	}
	return count, nil
}

// cleanupReferenceGrant removes the shared ReferenceGrant only if this is the last RayCluster
// with authentication in the namespace.
func (r *AuthenticationController) cleanupReferenceGrant(ctx context.Context, cluster *rayv1.RayCluster, authMode utils.AuthenticationMode, logger logr.Logger) error {
	grantName := "kuberay-gateway-access"

	// Count how many other RayClusters with authentication exist in this namespace
	authClustersCount, err := r.countClustersWithAuth(ctx, cluster.Namespace, authMode, cluster)
	if err != nil {
		return err
	}

	logger.Info("ReferenceGrant cleanup check",
		"namespace", cluster.Namespace,
		"otherAuthClusters", authClustersCount,
		"action", func() string {
			if authClustersCount == 0 {
				return "will delete"
			}
			return "will retain"
		}())

	// Only delete the ReferenceGrant if this is the last cluster with auth
	if authClustersCount == 0 {
		grant := &gatewayv1beta1.ReferenceGrant{}
		err := r.Get(ctx, client.ObjectKey{Name: grantName, Namespace: cluster.Namespace}, grant)
		if err != nil {
			if errors.IsNotFound(err) {
				logger.Info("ReferenceGrant already deleted", "name", grantName)
				return nil
			}
			return fmt.Errorf("failed to get ReferenceGrant: %w", err)
		}

		if err := r.Delete(ctx, grant); err != nil {
			return fmt.Errorf("failed to delete ReferenceGrant: %w", err)
		}
		logger.Info("Deleted shared ReferenceGrant (last cluster with auth)", "name", grantName, "namespace", cluster.Namespace)
	} else {
		logger.Info("ReferenceGrant retained (other clusters with auth exist)",
			"name", grantName,
			"namespace", cluster.Namespace,
			"remainingClusters", authClustersCount)
	}

	return nil
}

func (r *AuthenticationController) ensureOIDCConfigMap(ctx context.Context, cluster *rayv1.RayCluster, logger logr.Logger) error {
	namer := utils.NewResourceNamer(cluster)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namer.ConfigMapName(),
			Namespace: cluster.Namespace,
		},
	}

	opResult, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, configMap, r.Scheme); err != nil {
			return err
		}

		// Build ConfigMap data dynamically
		configYAML := fmt.Sprintf(`
authorization:
  resourceAttributes:
    # For an incoming request, the proxy will check if the user
    # has the "get" verb on the "services" resource.
    verb: "get"
    resource: "services"
    # The API group and resource name should match the target Service.
    apiGroup: ""
    resourceName: "%s"
`, cluster.Name+"-head-svc")

		// Set labels and data
		if configMap.Labels == nil {
			configMap.Labels = make(map[string]string)
		}
		configMap.Labels["app"] = "kube-rbac-proxy"
		configMap.Labels[utils.KubernetesCreatedByLabelKey] = utils.ComponentName

		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data["config.yaml"] = configYAML

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update OIDC ConfigMap: %w", err)
	}

	if opResult != controllerutil.OperationResultNone {
		logger.Info("OIDC ConfigMap reconciled", "name", configMap.Name, "operation", opResult)
	}

	return nil
}

// ensureServiceAccount creates or updates the service account for authentication (used for both OIDC and OAuth modes)
func (r *AuthenticationController) ensureServiceAccount(ctx context.Context, cluster *rayv1.RayCluster, authMode utils.AuthenticationMode, logger logr.Logger) error {
	namer := utils.NewResourceNamer(cluster)
	saName := namer.ServiceAccountName(authMode)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: cluster.Namespace,
		},
	}

	opResult, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, sa, r.Scheme); err != nil {
			return err
		}

		// Service account doesn't need special annotations for kube-rbac-proxy
		// It uses standard Kubernetes RBAC

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update service account: %w", err)
	}

	if opResult != controllerutil.OperationResultNone {
		logger.Info("Service account reconciled", "name", saName, "operation", opResult, "mode", authMode)
	}

	return nil
}

// ensureOAuthServiceAccount creates or updates the OAuth service account
func (r *AuthenticationController) ensureOAuthServiceAccount(ctx context.Context, cluster *rayv1.RayCluster, logger logr.Logger) error {
	namer := utils.NewResourceNamer(cluster)
	saName := namer.ServiceAccountName(utils.ModeIntegratedOAuth)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: cluster.Namespace,
		},
	}

	opResult, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, sa, r.Scheme); err != nil {
			return err
		}

		// Add service account annotation for OAuth
		if sa.Annotations == nil {
			sa.Annotations = make(map[string]string)
		}
		sa.Annotations["serviceaccounts.openshift.io/oauth-redirectreference.first"] = fmt.Sprintf(
			`{"kind":"OAuthRedirectReference","apiVersion":"v1","reference":{"kind":"Route","name":"%s"}}`,
			utils.GenerateRouteName(cluster.Name),
		)

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update OAuth service account: %w", err)
	}

	if opResult != controllerutil.OperationResultNone {
		logger.Info("OAuth service account reconciled", "name", saName, "operation", opResult)
	}

	return nil
}

// NOTE: Pod recreation logic removed - Kubernetes pods are immutable
// You cannot add containers to running pods, so we don't try to retrofit existing pods
// OAuth sidecar is automatically injected by RayCluster controller during pod creation
// Users must delete and recreate their RayCluster to enable OAuth on existing clusters

// generateSelfSignedCert generates a self-signed certificate for OAuth proxy
func generateSelfSignedCert(cluster *rayv1.RayCluster) ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"KubeRay OAuth Proxy"},
			CommonName:   cluster.Name + "-oauth-proxy",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames: []string{
			"localhost",
			cluster.Name,
			fmt.Sprintf("%s.%s", cluster.Name, cluster.Namespace),
			fmt.Sprintf("%s.%s.svc", cluster.Name, cluster.Namespace),
			fmt.Sprintf("%s.%s.svc.cluster.local", cluster.Name, cluster.Namespace),
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM, nil
}

// GetOAuthProxySidecar returns the OAuth proxy sidecar container configuration
// This can be used by the RayCluster controller to inject the sidecar
func GetOAuthProxySidecar(cluster *rayv1.RayCluster) corev1.Container {
	namer := utils.NewResourceNamer(cluster)
	return corev1.Container{
		Name:            oauthProxyContainerName,
		Image:           oauthProxyImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports: []corev1.ContainerPort{
			utils.CreateContainerPort(authProxyPort, oauthProxyPortName),
		},
		Args: []string{
			fmt.Sprintf("--https-address=:%d", authProxyPort),
			"--provider=openshift",
			fmt.Sprintf("--openshift-service-account=%s", namer.ServiceAccountName(utils.ModeIntegratedOAuth)),
			"--upstream=http://localhost:8265",
			"--tls-cert=/etc/tls/private/tls.crt",
			"--tls-key=/etc/tls/private/tls.key",
			"--cookie-secret=$(COOKIE_SECRET)",
			fmt.Sprintf("--openshift-delegate-urls=%s", utils.FormatOAuthDelegateURLs(cluster.Namespace)),
			"--skip-provider-button",
		},
		Env: []corev1.EnvVar{
			utils.CreateEnvVarFromSecret("COOKIE_SECRET", namer.SecretName(utils.ModeIntegratedOAuth), "cookie_secret"),
		},
		VolumeMounts: []corev1.VolumeMount{
			utils.CreateVolumeMount(oauthProxyVolumeName, "/etc/tls/private", true),
		},
		// Add resource limits to prevent excessive resource usage
		Resources: utils.StandardProxyResources(),
		// Add liveness probe to detect if OAuth proxy is healthy
		LivenessProbe: utils.CreateProbe(utils.ProbeConfig{
			Path:                "/oauth/healthz",
			Port:                authProxyPort,
			Scheme:              corev1.URISchemeHTTPS,
			InitialDelaySeconds: 30,
			TimeoutSeconds:      1,
			PeriodSeconds:       10,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		}),
		// Add readiness probe to prevent routing traffic before OAuth proxy is ready
		ReadinessProbe: utils.CreateProbe(utils.ProbeConfig{
			Path:                "/oauth/healthz",
			Port:                authProxyPort,
			Scheme:              corev1.URISchemeHTTPS,
			InitialDelaySeconds: 5,
			TimeoutSeconds:      1,
			PeriodSeconds:       5,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		}),
	}
}

// GetOAuthProxyVolumes returns the volumes needed for OAuth proxy sidecar
func GetOAuthProxyVolumes(cluster *rayv1.RayCluster) []corev1.Volume {
	namer := utils.NewResourceNamer(cluster)
	return []corev1.Volume{
		utils.CreateSecretVolume(oauthConfigVolumeName, namer.SecretName(utils.ModeIntegratedOAuth)),
		utils.CreateSecretVolume(oauthProxyVolumeName, namer.TLSSecretName(utils.ModeIntegratedOAuth)),
	}
}

func GetOIDCProxySidecar(cluster *rayv1.RayCluster) corev1.Container {
	namer := utils.NewResourceNamer(cluster)
	configMapName := namer.ConfigMapName()
	return corev1.Container{
		Name:            oidcProxyContainerName,
		Image:           oidcProxyContainerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports: []corev1.ContainerPort{
			utils.CreateContainerPort(authProxyPort, oidcProxyPortName),
		},
		Args: []string{
			fmt.Sprintf("--secure-listen-address=0.0.0.0:%d", authProxyPort),
			"--upstream=http://127.0.0.1:8265/",
			"--config-file=/etc/kube-rbac-proxy/config.yaml",
			"--logtostderr=true",
		},
		VolumeMounts: []corev1.VolumeMount{
			utils.CreateVolumeMount(configMapName, "/etc/kube-rbac-proxy/", true),
		},
		// Add resource limits to prevent excessive resource usage
		Resources: utils.StandardProxyResources(),
	}
}

func GetOIDCProxyVolumes(cluster *rayv1.RayCluster) []corev1.Volume {
	namer := utils.NewResourceNamer(cluster)
	configMapName := namer.ConfigMapName()
	return []corev1.Volume{
		utils.CreateConfigMapVolume(configMapName, configMapName),
	}
}

// cleanupOrphanedReferenceGrant checks if there are any RayClusters with auth in the namespace
// and removes the ReferenceGrant if none exist. This handles cases where the controller is
// triggered after a cluster is deleted.
func (r *AuthenticationController) cleanupOrphanedReferenceGrant(ctx context.Context, namespace string, logger logr.Logger) error {
	grantName := "kuberay-gateway-access"

	// Count clusters with authentication enabled in the namespace
	authMode := utils.DetectAuthenticationMode(r.options.IsOpenShift)
	authClustersCount, err := r.countClustersWithAuth(ctx, namespace, authMode, nil)
	if err != nil {
		return err
	}

	logger.Info("Checking for orphaned ReferenceGrant",
		"namespace", namespace,
		"authClusters", authClustersCount)

	// If no clusters with auth exist, delete the grant
	if authClustersCount == 0 {
		grant := &gatewayv1beta1.ReferenceGrant{}
		err := r.Get(ctx, client.ObjectKey{Name: grantName, Namespace: namespace}, grant)
		if err != nil {
			if errors.IsNotFound(err) {
				logger.Info("No orphaned ReferenceGrant found", "namespace", namespace)
				return nil
			}
			return fmt.Errorf("failed to get ReferenceGrant: %w", err)
		}

		if err := r.Delete(ctx, grant); err != nil {
			return fmt.Errorf("failed to delete orphaned ReferenceGrant: %w", err)
		}
		logger.Info("Deleted orphaned ReferenceGrant (no clusters with auth remain)",
			"name", grantName,
			"namespace", namespace)
	} else {
		logger.Info("ReferenceGrant not orphaned (clusters with auth exist)",
			"namespace", namespace,
			"authClusters", authClustersCount)
	}

	return nil
}

// enforceEnableIngressFalseOnOpenShift ensures enableIngress is set to false on OpenShift
// This provides upgrade idempotency - handles existing clusters that weren't processed by webhook
// The webhook sets this for new/edited clusters, this handles clusters from before the webhook was updated
// Placed in Authentication controller because HTTPRoute requires Gateway access (not basic Routes)
func (r *AuthenticationController) enforceEnableIngressFalseOnOpenShift(ctx context.Context, rayCluster *rayv1.RayCluster, logger logr.Logger) error {
	// Check if enableIngress is already false
	if rayCluster.Spec.HeadGroupSpec.EnableIngress != nil && !*rayCluster.Spec.HeadGroupSpec.EnableIngress {
		return nil // Already false, nothing to do
	}

	// Need to set to false
	wasTrue := rayCluster.Spec.HeadGroupSpec.EnableIngress != nil && *rayCluster.Spec.HeadGroupSpec.EnableIngress
	wasMissing := rayCluster.Spec.HeadGroupSpec.EnableIngress == nil

	logger.Info("Enforcing enableIngress to false on OpenShift (Gateway-only access)",
		"cluster", rayCluster.Name, "previousValue", rayCluster.Spec.HeadGroupSpec.EnableIngress)

	// Update the cluster spec
	rayCluster.Spec.HeadGroupSpec.EnableIngress = ptr.To(false)
	if err := r.Update(ctx, rayCluster); err != nil {
		logger.Error(err, "Failed to enforce enableIngress to false", "cluster", rayCluster.Name)
		return err
	}

	if wasTrue {
		logger.Info("Overrode user-specified enableIngress from true to false for Gateway-only access",
			"cluster", rayCluster.Name)
		r.Recorder.Eventf(rayCluster, corev1.EventTypeNormal,
			"EnableIngressEnforced",
			"Set enableIngress to false to enforce Gateway-only access (overrode user value)")
	} else if wasMissing {
		logger.Info("Set enableIngress to false for Gateway-only access (was not set)", "cluster", rayCluster.Name)
		r.Recorder.Eventf(rayCluster, corev1.EventTypeNormal,
			"EnableIngressSet",
			"Set enableIngress to false for Gateway-only access")
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager
func (r *AuthenticationController) SetupWithManager(mgr ctrl.Manager) error {
	// Predicate to only reconcile RayClusters when relevant changes occur
	rayClusterPredicate := predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
	)

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: 3}).
		// PRIMARY: Watch RayClusters
		For(&rayv1.RayCluster{}, builder.WithPredicates(rayClusterPredicate)).
		// OWNED: Watch resources with owner references back to RayClusters.
		// Note: HTTPRoute and ReferenceGrant are intentionally excluded here because
		// HTTPRoute is created in platformNamespace (cross-namespace owner refs are impossible)
		// and ReferenceGrant uses reference-counting cleanup without owner refs.
		// SafetyRequeueDelay compensates by periodically re-reconciling to detect drift.
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Service{}).
		Owns(&routev1.Route{}).
		Named("authentication").
		Complete(r)
}

// mapAuthResourceToRayClusters maps cluster-wide auth config changes to all RayClusters
// This ensures all clusters are re-evaluated when authentication mode changes
func (r *AuthenticationController) mapAuthResourceToRayClusters(ctx context.Context, obj client.Object) []reconcile.Request {
	logger := ctrl.LoggerFrom(ctx)

	// List all RayClusters in all namespaces
	rayClusterList := &rayv1.RayClusterList{}
	if err := r.List(ctx, rayClusterList); err != nil {
		logger.Error(err, "Failed to list RayClusters for authentication config mapping")
		return []reconcile.Request{}
	}

	// Create reconcile requests for all clusters
	requests := make([]reconcile.Request, 0)
	for _, cluster := range rayClusterList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		})
	}

	logger.Info("Mapping authentication config change to RayClusters",
		"authResource", obj.GetName(),
		"clusters", len(requests))

	return requests
}
