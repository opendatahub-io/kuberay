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
	}{
		{
			name:           "nil profile returns Intermediate defaults",
			profile:        nil,
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    intermediateCiphers,
		},
		{
			name:           "empty profile returns Intermediate defaults",
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
			name: "Modern returns TLS 1.3 with nil ciphers",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileModernType,
			},
			wantMinVersion: tls.VersionTLS13,
			wantCiphers:    nil,
		},
		{
			name: "Old returns TLS 1.0 with nil ciphers",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileOldType,
			},
			wantMinVersion: tls.VersionTLS10,
			wantCiphers:    nil,
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
			name: "Custom with all unsupported ciphers returns empty slice",
			profile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: "VersionTLS12",
						Ciphers:       []string{"DHE-RSA-AES128-GCM-SHA256"},
					},
				},
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    []uint16{},
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
			name: "Unknown type falls back to Intermediate",
			profile: &configv1.TLSSecurityProfile{
				Type: "SuperSecure",
			},
			wantMinVersion: tls.VersionTLS12,
			wantCiphers:    intermediateCiphers,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMinVersion, gotCiphers := parseProfile(tt.profile)

			if gotMinVersion != tt.wantMinVersion {
				t.Errorf("parseProfile() minVersion = %d, want %d", gotMinVersion, tt.wantMinVersion)
			}

			if tt.wantCiphers == nil {
				if gotCiphers != nil {
					t.Errorf("parseProfile() ciphers = %v, want nil", gotCiphers)
				}
				return
			}

			if gotCiphers == nil {
				t.Fatal("expected non-nil empty slice, got nil")
			}
			if len(gotCiphers) != len(tt.wantCiphers) {
				t.Errorf("parseProfile() ciphers length = %d, want %d", len(gotCiphers), len(tt.wantCiphers))
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
