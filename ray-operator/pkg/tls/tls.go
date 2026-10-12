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

// openSSLToGoCipher maps OpenSSL-style cipher suite names -- as used by
// configv1.TLSProfiles and by operator-supplied Custom profiles -- to the
// Go crypto/tls cipher suite ID that implements them.
//
// Entries are included only when Go's crypto/tls package actually
// implements the cipher. Suites with no Go implementation at all (notably
// every DHE_* key-exchange suite, and a handful of RSA/ECDHE CBC variants
// Go never added) are intentionally omitted; resolveCipherNames drops and
// logs them as unsupported rather than silently matching the wrong ID.
var openSSLToGoCipher = map[string]uint16{
	"ECDHE-ECDSA-AES128-GCM-SHA256":        tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"ECDHE-RSA-AES128-GCM-SHA256":          tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"ECDHE-ECDSA-AES256-GCM-SHA384":        tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"ECDHE-RSA-AES256-GCM-SHA384":          tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"ECDHE-ECDSA-CHACHA20-POLY1305-SHA256": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	"ECDHE-RSA-CHACHA20-POLY1305-SHA256":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	"ECDHE-ECDSA-CHACHA20-POLY1305":        tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	"ECDHE-RSA-CHACHA20-POLY1305":          tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,

	// Additional aliases needed to resolve OpenShift's built-in "Old" TLS
	// profile (configv1.TLSProfiles[TLSProfileOldType]) against the actual
	// set of suites Go's crypto/tls implements.
	"ECDHE-ECDSA-AES128-SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	"ECDHE-RSA-AES128-SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
	"ECDHE-ECDSA-AES128-SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	"ECDHE-RSA-AES128-SHA":      tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	"ECDHE-ECDSA-AES256-SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	"ECDHE-RSA-AES256-SHA":      tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	"AES128-GCM-SHA256":         tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	"AES256-GCM-SHA384":         tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	"AES128-SHA256":             tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
	"AES128-SHA":                tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	"AES256-SHA":                tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	"DES-CBC3-SHA":              tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
}

// intermediateCiphers is the hardened cipher suite list used for the
// infrastructure fallback path (intermediateWithALPN), where no cluster
// TLS profile is available at all (non-OpenShift clusters, or transient
// API errors before the watcher can self-heal). It intentionally mirrors
// OpenShift's Intermediate profile restricted to TLS <=1.2 ECDHE suites.
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

// implementedCipherSuites returns every TLS <=1.2 cipher suite ID (and its
// IANA name) implemented by the Go crypto/tls package running this binary,
// covering both the secure suites returned by tls.CipherSuites() and the
// insecure suites returned by tls.InsecureCipherSuites(). TLS 1.3-only
// suites (e.g. TLS_AES_128_GCM_SHA256) are deliberately excluded: Go's
// tls.Config.CipherSuites only governs TLS <=1.2 negotiation, so a TLS
// 1.3-only ID would be meaningless clutter in that list. It is used to
// filter requested TLS profiles down to ciphers this binary can actually
// negotiate, rather than trusting a static, potentially stale map alone.
func implementedCipherSuites() (byID map[uint16]struct{}, byName map[string]uint16) {
	byID = make(map[uint16]struct{})
	byName = make(map[string]uint16)
	for _, group := range [][]*tls.CipherSuite{tls.CipherSuites(), tls.InsecureCipherSuites()} {
		for _, c := range group {
			if !supportsTLS12OrEarlier(c) {
				continue
			}
			byID[c.ID] = struct{}{}
			byName[c.Name] = c.ID
		}
	}
	return byID, byName
}

// supportsTLS12OrEarlier reports whether a cipher suite can be negotiated
// at TLS 1.2 or earlier (as opposed to TLS 1.3-only suites).
func supportsTLS12OrEarlier(c *tls.CipherSuite) bool {
	for _, v := range c.SupportedVersions {
		if v <= tls.VersionTLS12 {
			return true
		}
	}
	return false
}

// resolveCipherName resolves a single cipher suite name to a Go cipher
// suite ID. It accepts either the suite's IANA name (e.g.
// "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256") or an OpenSSL-style alias (e.g.
// "ECDHE-RSA-AES128-GCM-SHA256"), and returns ok=false if the name is
// empty, unrecognized, or not implemented by the running Go TLS stack.
func resolveCipherName(name string, byID map[uint16]struct{}, byName map[string]uint16) (uint16, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, false
	}
	if id, ok := byName[name]; ok {
		return id, true
	}
	if id, ok := openSSLToGoCipher[name]; ok {
		if _, implemented := byID[id]; implemented {
			return id, true
		}
	}
	return 0, false
}

// resolveCipherNames resolves a list of OpenSSL/IANA cipher suite names
// down to the Go cipher suite IDs actually implemented by this binary's
// TLS stack. Empty, unrecognized, or unimplemented names are dropped (and
// logged); callers are responsible for treating an all-dropped result as
// an error where that would leave the listener unusable.
func resolveCipherNames(names []string) []uint16 {
	byID, byName := implementedCipherSuites()
	ciphers := make([]uint16, 0, len(names))
	for _, name := range names {
		id, ok := resolveCipherName(name, byID, byName)
		if !ok {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				log.Info("Dropping unsupported cipher from TLS profile", "cipher", trimmed)
			}
			continue
		}
		ciphers = append(ciphers, id)
	}
	return ciphers
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
	minVersion, ciphers, err := parseProfile(apiServer.Spec.TLSSecurityProfile)
	if err != nil {
		return result, fmt.Errorf("resolving cluster TLS profile: %w", err)
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

// parseProfile resolves a cluster TLS security profile to a Go minimum TLS
// version and cipher suite list.
//
// It returns an error for malformed or unrecognized profiles -- an unknown
// profile type, a Custom profile with a nil custom block, or (via
// resolveNamedProfile/parseCustomProfile) an unknown minTLSVersion or a
// cipher list with no suites this binary's TLS stack can negotiate --
// instead of silently substituting Intermediate for the policy that was
// actually requested. Callers must treat an error as fatal to startup
// rather than falling back to a default.
//
// A nil profile, or one with an empty/unset Type, is not malformed: it is
// OpenShift's documented way of requesting the default Intermediate
// profile, so it continues to resolve to Intermediate rather than erroring.
func parseProfile(profile *configv1.TLSSecurityProfile) (uint16, []uint16, error) {
	if profile == nil || profile.Type == "" {
		return resolveNamedProfile(configv1.TLSProfileIntermediateType)
	}

	switch profile.Type {
	case configv1.TLSProfileIntermediateType, configv1.TLSProfileOldType, configv1.TLSProfileModernType:
		return resolveNamedProfile(profile.Type)
	case configv1.TLSProfileCustomType:
		if profile.Custom == nil {
			return 0, nil, errors.New("TLS profile type is Custom but the custom block is nil")
		}
		return parseCustomProfile(profile.Custom)
	default:
		return 0, nil, fmt.Errorf("unknown TLS profile type %q", profile.Type)
	}
}

// resolveNamedProfile resolves one of OpenShift's built-in named TLS
// profiles (Old, Intermediate, Modern) using the canonical minimum version
// and cipher list published in configv1.TLSProfiles, filtered down to the
// ciphers actually implemented by this binary's Go TLS stack.
func resolveNamedProfile(profileType configv1.TLSProfileType) (uint16, []uint16, error) {
	spec, ok := configv1.TLSProfiles[profileType]
	if !ok {
		return 0, nil, fmt.Errorf("no cipher/version definition available for TLS profile %q", profileType)
	}

	minVersion, ok := tlsVersionMap[spec.MinTLSVersion]
	if !ok {
		return 0, nil, fmt.Errorf("TLS profile %q specifies unsupported minTLSVersion %q", profileType, spec.MinTLSVersion)
	}

	// TLS 1.3 cipher suites are not configurable via tls.Config.CipherSuites;
	// the Go runtime always selects from its fixed, secure TLS 1.3 suite
	// set, so there is nothing to resolve or filter here.
	if minVersion == tls.VersionTLS13 {
		return minVersion, nil, nil
	}

	ciphers := resolveCipherNames(spec.Ciphers)
	if len(ciphers) == 0 {
		return 0, nil, fmt.Errorf("TLS profile %q: none of its ciphers are implemented by this binary's TLS stack", profileType)
	}
	return minVersion, ciphers, nil
}

// parseCustomProfile resolves an operator-supplied Custom TLS profile,
// filtering its requested ciphers down to the subset implemented by this
// binary's Go TLS stack. An unknown minTLSVersion, or a non-empty TLS <=1.2
// cipher list that resolves to zero usable suites, is treated as an error
// rather than silently falling back to a different policy.
func parseCustomProfile(custom *configv1.CustomTLSProfile) (uint16, []uint16, error) {
	minVersion, ok := tlsVersionMap[custom.MinTLSVersion]
	if !ok {
		return 0, nil, fmt.Errorf("custom TLS profile specifies unknown minTLSVersion %q", custom.MinTLSVersion)
	}

	// TLS 1.3 cipher suites are not configurable via tls.Config.CipherSuites.
	if minVersion == tls.VersionTLS13 {
		return minVersion, nil, nil
	}

	if len(custom.Ciphers) == 0 {
		return minVersion, nil, nil
	}

	ciphers := resolveCipherNames(custom.Ciphers)
	if len(ciphers) == 0 {
		return 0, nil, errors.New("custom TLS profile specified ciphers but none are implemented by this binary's TLS stack")
	}
	return minVersion, ciphers, nil
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
