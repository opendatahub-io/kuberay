package tls

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = ctrl.Log.WithName("tls")

var openSSLToGoCipher = map[string]uint16{
	"TLS_AES_128_GCM_SHA256":               tls.TLS_AES_128_GCM_SHA256,
	"TLS_AES_256_GCM_SHA384":               tls.TLS_AES_256_GCM_SHA384,
	"TLS_CHACHA20_POLY1305_SHA256":         tls.TLS_CHACHA20_POLY1305_SHA256,
	"ECDHE-ECDSA-AES128-GCM-SHA256":        tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"ECDHE-RSA-AES128-GCM-SHA256":          tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"ECDHE-ECDSA-AES256-GCM-SHA384":        tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"ECDHE-RSA-AES256-GCM-SHA384":          tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"ECDHE-ECDSA-CHACHA20-POLY1305-SHA256": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	"ECDHE-RSA-CHACHA20-POLY1305-SHA256":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	"ECDHE-ECDSA-CHACHA20-POLY1305":        tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	"ECDHE-RSA-CHACHA20-POLY1305":          tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

var intermediateCiphers = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

var tlsVersionMap = map[configv1.TLSProtocolVersion]uint16{
	"VersionTLS10": tls.VersionTLS10,
	"VersionTLS11": tls.VersionTLS11,
	"VersionTLS12": tls.VersionTLS12,
	"VersionTLS13": tls.VersionTLS13,
}

var tlsVersionFlag = map[uint16]string{
	tls.VersionTLS10: "VersionTLS10",
	tls.VersionTLS11: "VersionTLS11",
	tls.VersionTLS12: "VersionTLS12",
	tls.VersionTLS13: "VersionTLS13",
}

// Result holds the resolved TLS configuration.
type Result struct {
	TLSOpts        []func(*tls.Config)
	Profile        *configv1.TLSSecurityProfile
	ProfileFetched bool
}

// Resolve reads the cluster TLS profile from apiservers.config.openshift.io/cluster
// and returns TLS option functions for controller-runtime servers.
// On non-OpenShift clusters or when the API is temporarily unavailable,
// it returns hardened Intermediate defaults.
// Returns an error only on unexpected failures that should prevent startup.
func Resolve(ctx context.Context, cfg *rest.Config) (Result, error) {
	scheme := runtime.NewScheme()
	if err := configv1.Install(scheme); err != nil {
		return Result{}, fmt.Errorf("installing OpenShift config scheme: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return Result{}, fmt.Errorf("creating bootstrap client for TLS profile: %w", err)
	}

	return resolveWithClient(ctx, k8sClient)
}

func resolveWithClient(ctx context.Context, k8sClient client.Client) (Result, error) {
	var result Result

	resolveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	apiServer := &configv1.APIServer{}
	if err := k8sClient.Get(resolveCtx, client.ObjectKey{Name: "cluster"}, apiServer); err != nil {
		switch {
		case meta.IsNoMatchError(err):
			log.Info("TLS profile not available, using hardened defaults (non-OpenShift cluster)")
		case apierrors.IsNotFound(err):
			log.Info("APIServer resource not found, using hardened defaults")
		case apierrors.IsServiceUnavailable(err),
			apierrors.IsTimeout(err),
			apierrors.IsServerTimeout(err),
			apierrors.IsTooManyRequests(err),
			errors.Is(err, context.DeadlineExceeded):
			log.Info("Transient API error reading TLS profile, using hardened defaults", "error", err)
			result.ProfileFetched = true
			result.Profile = &configv1.TLSSecurityProfile{Type: configv1.TLSProfileIntermediateType}
		default:
			return result, fmt.Errorf("reading APIServer TLS profile: %w", err)
		}
		result.TLSOpts = append(result.TLSOpts, intermediateWithALPN)
		return result, nil
	}

	result.ProfileFetched = true
	result.Profile = apiServer.Spec.TLSSecurityProfile
	minVersion, ciphers := parseProfile(apiServer.Spec.TLSSecurityProfile)
	if ciphers != nil && len(ciphers) == 0 {
		return result, fmt.Errorf("custom TLS profile specified ciphers but none are supported by Go")
	}

	result.TLSOpts = append(result.TLSOpts, func(c *tls.Config) {
		c.MinVersion = minVersion
		if len(ciphers) > 0 {
			c.CipherSuites = ciphers
		}
		c.NextProtos = []string{"h2", "http/1.1"}
	})
	return result, nil
}

func intermediateWithALPN(c *tls.Config) {
	c.MinVersion = tls.VersionTLS12
	c.CipherSuites = intermediateCiphers
	c.NextProtos = []string{"h2", "http/1.1"}
}

func parseProfile(profile *configv1.TLSSecurityProfile) (uint16, []uint16) {
	if profile == nil {
		return tls.VersionTLS12, intermediateCiphers
	}

	switch profile.Type {
	case configv1.TLSProfileIntermediateType, "":
		return tls.VersionTLS12, intermediateCiphers
	case configv1.TLSProfileModernType:
		return tls.VersionTLS13, nil
	case configv1.TLSProfileOldType:
		return tls.VersionTLS10, nil
	case configv1.TLSProfileCustomType:
		if profile.Custom == nil {
			log.Info("Custom TLS profile type specified but custom block is nil, falling back to Intermediate")
			return tls.VersionTLS12, intermediateCiphers
		}
		return parseCustomProfile(profile.Custom)
	default:
		log.Info("Unknown TLS profile type, falling back to Intermediate", "type", profile.Type)
		return tls.VersionTLS12, intermediateCiphers
	}
}

func parseCustomProfile(custom *configv1.CustomTLSProfile) (uint16, []uint16) {
	minVersion, ok := tlsVersionMap[custom.MinTLSVersion]
	if !ok {
		log.Info("Unknown minTLSVersion in custom profile, defaulting to TLS 1.2", "minTLSVersion", custom.MinTLSVersion)
		minVersion = tls.VersionTLS12
	}

	if len(custom.Ciphers) == 0 {
		return minVersion, nil
	}

	ciphers := make([]uint16, 0, len(custom.Ciphers))
	for _, name := range custom.Ciphers {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if id, ok := openSSLToGoCipher[name]; ok {
			ciphers = append(ciphers, id)
		} else {
			log.Info("Dropping unsupported cipher from custom TLS profile", "cipher", name)
		}
	}
	return minVersion, ciphers
}

// ProxyTLSArgs returns the kube-rbac-proxy flags that implement a TLS profile.
// kube-rbac-proxy expects Go crypto/tls constant names for both values.
func ProxyTLSArgs(profile *configv1.TLSSecurityProfile) []string {
	minVersion, ciphers := parseProfile(profile)
	args := []string{"--tls-min-version=" + tlsVersionFlag[minVersion]}
	if len(ciphers) == 0 {
		return args
	}

	cipherNames := make([]string, len(ciphers))
	for i, cipher := range ciphers {
		cipherNames[i] = tls.CipherSuiteName(cipher)
	}
	return append(args, "--tls-cipher-suites="+strings.Join(cipherNames, ","))
}

// SetupWatcher registers a controller that watches the APIServer resource
// and triggers a graceful restart when the TLS profile changes.
// On non-OpenShift clusters (ProfileFetched == false), this is a no-op.
func SetupWatcher(mgr manager.Manager, result Result, cancel context.CancelFunc) error {
	if !result.ProfileFetched {
		return nil
	}

	c, err := controller.New("tls-profile-watcher", mgr, controller.Options{
		NeedLeaderElection: boolPtr(false),
		Reconciler: &profileWatcher{
			client:         mgr.GetClient(),
			cancel:         cancel,
			initialProfile: result.Profile,
		},
	})
	if err != nil {
		return fmt.Errorf("creating TLS profile watcher controller: %w", err)
	}

	if err := c.Watch(source.Kind(
		mgr.GetCache(),
		&configv1.APIServer{},
		handler.TypedEnqueueRequestsFromMapFunc(
			func(_ context.Context, obj *configv1.APIServer) []reconcile.Request {
				if obj.Name == "cluster" {
					return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "cluster"}}}
				}
				return nil
			},
		),
	)); err != nil {
		return fmt.Errorf("watching APIServer resource: %w", err)
	}

	log.Info("TLS profile watcher registered")
	return nil
}

type profileWatcher struct {
	client         client.Client
	cancel         context.CancelFunc
	initialProfile *configv1.TLSSecurityProfile
}

func (w *profileWatcher) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	apiServer := &configv1.APIServer{}
	if err := w.client.Get(ctx, client.ObjectKey{Name: "cluster"}, apiServer); err != nil {
		log.Info("TLS profile watcher: unable to read APIServer, will retry", "error", err)
		return reconcile.Result{}, err
	}

	current := apiServer.Spec.TLSSecurityProfile
	if !profileEqual(w.initialProfile, current) {
		log.Info("TLS profile changed, initiating shutdown to reload")
		w.cancel()
	}
	return reconcile.Result{}, nil
}

func normalizeProfileType(t configv1.TLSProfileType) configv1.TLSProfileType {
	if t == "" {
		return configv1.TLSProfileIntermediateType
	}
	return t
}

func normalizeProfile(p *configv1.TLSSecurityProfile) *configv1.TLSSecurityProfile {
	if p == nil {
		return &configv1.TLSSecurityProfile{Type: configv1.TLSProfileIntermediateType}
	}
	return p
}

func profileEqual(a, b *configv1.TLSSecurityProfile) bool {
	a = normalizeProfile(a)
	b = normalizeProfile(b)
	if normalizeProfileType(a.Type) != normalizeProfileType(b.Type) {
		return false
	}
	if normalizeProfileType(a.Type) == configv1.TLSProfileCustomType {
		if a.Custom == nil && b.Custom == nil {
			return true
		}
		if a.Custom == nil || b.Custom == nil {
			return false
		}
		if a.Custom.MinTLSVersion != b.Custom.MinTLSVersion {
			return false
		}
		if len(a.Custom.Ciphers) != len(b.Custom.Ciphers) {
			return false
		}
		for i := range a.Custom.Ciphers {
			if a.Custom.Ciphers[i] != b.Custom.Ciphers[i] {
				return false
			}
		}
	}
	return true
}

func boolPtr(b bool) *bool { return &b }
