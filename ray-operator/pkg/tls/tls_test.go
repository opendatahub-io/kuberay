package tls

import (
	"context"
	"crypto/tls"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestParseProfile(t *testing.T) {
	tests := []struct {
		profile        *configv1.TLSSecurityProfile
		name           string
		wantCiphers    []uint16
		wantMinVersion uint16
		wantErr        bool
	}{
		{
			name:           "nil profile resolves to Intermediate",
			profile:        nil,
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    intermediateCiphers,
		},
		{
			name:           "empty profile resolves to Intermediate",
			profile:        &configv1.TLSSecurityProfile{},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    intermediateCiphers,
		},
		{
			name: "Intermediate type",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileIntermediateType,
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    intermediateCiphers,
		},
		{
			name: "Modern returns TLS 1.3 with nil ciphers (CipherSuites does not control TLS 1.3)",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileModernType,
			},
			wantMinVersion: tls.VersionTLS13,
			wantCiphers:    nil,
		},
		{
			// The Old profile's canonical cipher list (configv1.TLSProfiles)
			// also lists DHE-RSA-* suites (Go has no DHE support at all) and
			// a number of CBC/3DES suites. CBC and 3DES are banned outright
			// by isBannedCipherName regardless of profile, so only the
			// AEAD (GCM/ChaCha20) suites survive filtering -- see
			// TestParseProfile_RejectsBannedCiphers for the explicit checks.
			name: "Old resolves to TLS 1.0 with explicit, runtime-supported, non-banned suites",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileOldType,
			},
			wantMinVersion: tls.VersionTLS10,
			wantCiphers: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			},
		},
		{
			name: "Unknown profile type returns an error, no silent Intermediate substitution",
			profile: &configv1.TLSSecurityProfile{
				Type: "SuperSecure",
			},
			wantErr: true,
		},
		{
			name: "Custom type with nil custom block returns an error",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
			},
			wantErr: true,
		},
		{
			name: "Custom with unknown minTLSVersion returns an error",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "NotAVersion",
						Ciphers:       []string{"ECDHE-ECDSA-AES128-GCM-SHA256"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Custom with valid ciphers",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384"},
					},
				},
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
		},
		{
			name: "Custom accepts IANA names as well as OpenSSL aliases",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", "ECDHE-RSA-AES256-GCM-SHA384"},
					},
				},
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
		},
		{
			name: "Custom with unsupported cipher skips it",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"ECDHE-ECDSA-AES128-GCM-SHA256", "UNSUPPORTED-CIPHER"},
					},
				},
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name: "Custom with all-unusable TLS <=1.2 ciphers returns an error",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"DHE-RSA-AES128-GCM-SHA256"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Custom with whitespace-padded cipher names still resolves",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"  ECDHE-ECDSA-AES128-GCM-SHA256  ", " ECDHE-RSA-AES256-GCM-SHA384"},
					},
				},
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
		},
		{
			name: "Custom with whitespace-only cipher entries are skipped",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"   ", "ECDHE-ECDSA-AES128-GCM-SHA256", "  "},
					},
				},
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name: "Custom Modern-equivalent minTLSVersion returns TLS 1.3 with nil ciphers",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS13",
						Ciphers:       []string{"TLS_AES_128_GCM_SHA256"},
					},
				},
			},
			wantMinVersion: tls.VersionTLS13,
			wantCiphers:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMinVersion, gotCiphers, err := parseProfile(tt.profile)

			if (err != nil) != tt.wantErr {
				t.Fatalf("parseProfile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if gotMinVersion != tt.wantMinVersion {
				t.Errorf("parseProfile() minVersion = %d, want %d", gotMinVersion, tt.wantMinVersion)
			}

			if tt.wantCiphers == nil {
				if gotCiphers != nil {
					t.Errorf("parseProfile() ciphers = %v, want nil", gotCiphers)
				}
				return
			}

			if len(gotCiphers) != len(tt.wantCiphers) {
				t.Errorf("parseProfile() ciphers = %v, want %v", gotCiphers, tt.wantCiphers)
				return
			}
			for i, c := range gotCiphers {
				if c != tt.wantCiphers[i] {
					t.Errorf("parseProfile() ciphers[%d] = %d, want %d", i, c, tt.wantCiphers[i])
				}
			}
		})
	}
}

// TestParseProfile_RejectsBannedCiphers is a regression test for a
// GitHub Advanced Security / CodeQL finding (tls-config-lint: "3DES/CBC
// cipher suite"): CBC-mode suites and 3DES must never be negotiable,
// regardless of whether they are requested via an OpenSSL-style alias (as
// listed in OpenShift's own "Old" named profile) or via a raw IANA cipher
// suite name in a Custom profile. RC4 is included for the same reason,
// since it is reachable the same way even though it was not itself
// flagged.
func TestParseProfile_RejectsBannedCiphers(t *testing.T) {
	bannedNames := []string{
		// OpenSSL-style aliases for suites OpenShift's "Old" profile lists.
		"DES-CBC3-SHA",
		"AES128-SHA",
		"AES256-SHA",
		"AES128-SHA256",
		"ECDHE-RSA-AES128-SHA",
		"ECDHE-ECDSA-AES128-SHA",
		"ECDHE-RSA-AES256-SHA",
		"ECDHE-ECDSA-AES256-SHA",
		// Raw IANA names: implemented by Go, and so reachable via the
		// byName path in resolveCipherName even though openSSLToGoCipher
		// has no entry for them.
		"TLS_RSA_WITH_3DES_EDE_CBC_SHA",
		"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
		"TLS_RSA_WITH_AES_128_CBC_SHA",
		"TLS_RSA_WITH_AES_256_CBC_SHA",
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
		"TLS_RSA_WITH_RC4_128_SHA",
		"TLS_ECDHE_RSA_WITH_RC4_128_SHA",
	}

	for _, name := range bannedNames {
		t.Run(name+" alone is rejected", func(t *testing.T) {
			profile := &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{name},
					},
				},
			}
			_, _, err := parseProfile(profile)
			if err == nil {
				t.Fatalf("parseProfile() with only banned cipher %q: error = nil, want error", name)
			}
		})

		t.Run(name+" is dropped when mixed with a good cipher", func(t *testing.T) {
			profile := &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{name, "ECDHE-ECDSA-AES128-GCM-SHA256"},
					},
				},
			}
			_, ciphers, err := parseProfile(profile)
			if err != nil {
				t.Fatalf("parseProfile() error = %v, want nil", err)
			}
			if len(ciphers) != 1 || ciphers[0] != tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 {
				t.Errorf("parseProfile() ciphers = %v, want only TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 (banned cipher %q must be dropped)", ciphers, name)
			}
		})
	}
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = configv1.Install(s)
	return s
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name               string
		objects            []runtime.Object
		wantErr            bool
		wantProfileFetched bool
		wantMinVersion     uint16
		wantNilCiphers     bool
		wantCiphers        []uint16
	}{
		{
			name:               "APIServer not found falls back to Intermediate",
			wantProfileFetched: false,
			wantMinVersion:     tls.VersionTLS12,
		},
		{
			name: "APIServer with Modern profile",
			objects: []runtime.Object{
				&configv1.APIServer{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
					Spec: configv1.APIServerSpec{
						TLSSecurityProfile: &configv1.TLSSecurityProfile{
							Type: configv1.TLSProfileModernType,
						},
					},
				},
			},
			wantProfileFetched: true,
			wantMinVersion:     tls.VersionTLS13,
			wantNilCiphers:     true,
		},
		{
			// Mirrors TestParseProfile's "Old resolves to TLS 1.0 with
			// explicit, runtime-supported, non-banned suites" case, but
			// asserts the suites actually land on a tls.Config once TLSOpts
			// is applied, not just the parseProfile() return value.
			name: "APIServer with Old profile resolves to explicit, runtime-supported suites",
			objects: []runtime.Object{
				&configv1.APIServer{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
					Spec: configv1.APIServerSpec{
						TLSSecurityProfile: &configv1.TLSSecurityProfile{
							Type: configv1.TLSProfileOldType,
						},
					},
				},
			},
			wantProfileFetched: true,
			wantMinVersion:     tls.VersionTLS10,
			wantCiphers: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			},
		},
		{
			// Asserts the applied tls.Config, not just parseProfile()'s
			// return value, ends up with exactly the one requested OpenSSL
			// alias resolved to its Go cipher suite ID.
			name: "APIServer with Custom profile using supported OpenSSL aliases",
			objects: []runtime.Object{
				&configv1.APIServer{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
					Spec: configv1.APIServerSpec{
						TLSSecurityProfile: &configv1.TLSSecurityProfile{
							Type: configv1.TLSProfileCustomType,
							Custom: &configv1.CustomTLSProfile{
								TLSProfileSpec: configv1.TLSProfileSpec{
									MinTLSVersion: "VersionTLS12",
									Ciphers:       []string{"ECDHE-ECDSA-AES128-GCM-SHA256"},
								},
							},
						},
					},
				},
			},
			wantProfileFetched: true,
			wantMinVersion:     tls.VersionTLS12,
			wantCiphers:        []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name: "APIServer with malformed Custom profile (unknown minTLSVersion) is a fatal error",
			objects: []runtime.Object{
				&configv1.APIServer{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
					Spec: configv1.APIServerSpec{
						TLSSecurityProfile: &configv1.TLSSecurityProfile{
							Type: configv1.TLSProfileCustomType,
							Custom: &configv1.CustomTLSProfile{
								TLSProfileSpec: configv1.TLSProfileSpec{
									MinTLSVersion: "NotAVersion",
								},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "APIServer with Custom profile where every cipher is unusable is a fatal error",
			objects: []runtime.Object{
				&configv1.APIServer{
					ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
					Spec: configv1.APIServerSpec{
						TLSSecurityProfile: &configv1.TLSSecurityProfile{
							Type: configv1.TLSProfileCustomType,
							Custom: &configv1.CustomTLSProfile{
								TLSProfileSpec: configv1.TLSProfileSpec{
									MinTLSVersion: "VersionTLS12",
									Ciphers:       []string{"DHE-RSA-AES128-GCM-SHA256"},
								},
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme()
			builder := fake.NewClientBuilder().WithScheme(s)
			for _, obj := range tt.objects {
				builder = builder.WithRuntimeObjects(obj)
			}
			c := builder.Build()

			result, err := resolveWithClient(context.Background(), c)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if result.ProfileFetched != tt.wantProfileFetched {
				t.Errorf("ProfileFetched = %v, want %v", result.ProfileFetched, tt.wantProfileFetched)
			}
			if len(result.TLSOpts) == 0 {
				t.Error("TLSOpts should not be empty")
			}
			cfg := &tls.Config{MinVersion: tls.VersionTLS12}
			if tt.wantMinVersion != tls.VersionTLS13 {
				cfg.MinVersion = tls.VersionTLS13
			}
			for _, fn := range result.TLSOpts {
				fn(cfg)
			}
			if len(cfg.NextProtos) == 0 {
				t.Error("NextProtos should be set")
			}
			if cfg.MinVersion != tt.wantMinVersion {
				t.Errorf("MinVersion = %d, want %d", cfg.MinVersion, tt.wantMinVersion)
			}
			if tt.wantNilCiphers && cfg.CipherSuites != nil {
				t.Errorf("CipherSuites should be nil for TLS 1.3, got %v", cfg.CipherSuites)
			}
			if tt.wantCiphers != nil {
				if len(cfg.CipherSuites) != len(tt.wantCiphers) {
					t.Errorf("CipherSuites = %v, want %v", cfg.CipherSuites, tt.wantCiphers)
				} else {
					for i, c := range cfg.CipherSuites {
						if c != tt.wantCiphers[i] {
							t.Errorf("CipherSuites = %v, want %v", cfg.CipherSuites, tt.wantCiphers)
							break
						}
					}
				}
			}
		})
	}
}

type errorClient struct {
	client.Client
	err error
}

func (c *errorClient) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return c.err
}

func TestResolve_TransientError(t *testing.T) {
	err := apierrors.NewServiceUnavailable("API unavailable")
	c := &errorClient{err: err}

	result, resolveErr := resolveWithClient(context.Background(), c)
	if resolveErr != nil {
		t.Fatalf("expected no error on transient failure, got %v", resolveErr)
	}
	if !result.ProfileFetched {
		t.Error("ProfileFetched should be true on transient errors (watcher self-healing)")
	}
	if result.Profile == nil {
		t.Fatal("Profile should be non-nil on transient errors (seeded with Intermediate for watcher comparison)")
	}
	if result.Profile.Type != configv1.TLSProfileIntermediateType {
		t.Errorf("Profile.Type = %v, want Intermediate", result.Profile.Type)
	}
}

func TestResolve_FatalError(t *testing.T) {
	err := apierrors.NewForbidden(
		schema.GroupResource{Group: "config.openshift.io", Resource: "apiservers"},
		"cluster", nil,
	)
	c := &errorClient{err: err}

	_, resolveErr := resolveWithClient(context.Background(), c)
	if resolveErr == nil {
		t.Fatal("expected error on Forbidden, got nil")
	}
}

func TestResolve_CustomAllUnsupportedCiphers(t *testing.T) {
	s := testScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(
		&configv1.APIServer{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
			Spec: configv1.APIServerSpec{
				TLSSecurityProfile: &configv1.TLSSecurityProfile{
					Type: configv1.TLSProfileCustomType,
					Custom: &configv1.CustomTLSProfile{
						TLSProfileSpec: configv1.TLSProfileSpec{
							MinTLSVersion: "VersionTLS12",
							Ciphers:       []string{"UNSUPPORTED-CIPHER-1", "UNSUPPORTED-CIPHER-2"},
						},
					},
				},
			},
		},
	).Build()

	_, err := resolveWithClient(context.Background(), c)
	if err == nil {
		t.Fatal("expected error when all ciphers are unsupported, got nil")
	}
}

func TestProfileEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b *configv1.TLSSecurityProfile
		want bool
	}{
		{"both nil", nil, nil, true},
		{"nil vs empty type (both Intermediate)", nil, &configv1.TLSSecurityProfile{}, true},
		{"nil vs explicit Intermediate", nil, &configv1.TLSSecurityProfile{Type: configv1.TLSProfileIntermediateType}, true},
		{"Intermediate vs Modern", &configv1.TLSSecurityProfile{Type: configv1.TLSProfileIntermediateType}, &configv1.TLSSecurityProfile{Type: configv1.TLSProfileModernType}, false},
		{"nil vs Modern", nil, &configv1.TLSSecurityProfile{Type: configv1.TLSProfileModernType}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := profileEqual(tt.a, tt.b); got != tt.want {
				t.Errorf("profileEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
