package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	cfg "github.com/conductorone/baton-google-workspace/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/cli"
	googlecredentialguard "github.com/conductorone/google-credential-guard"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
)

func guardTestJSON(t *testing.T, tokenURI, credentialType string) []byte {
	t.Helper()
	value := map[string]string{
		"type":           credentialType,
		"project_id":     "test-project",
		"private_key_id": "test-key",
		"private_key":    testPrivateKey(t),
		"client_email":   "test@test-project.iam.gserviceaccount.com",
		"token_uri":      tokenURI,
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCredentialInputsAcceptServiceAccount(t *testing.T) {
	data := guardTestJSON(t, googlecredentialguard.TokenURL, "service_account")
	for _, input := range []string{"inline", "upload", "internal"} {
		t.Run(input, func(t *testing.T) {
			var connector *GoogleWorkspace
			var err error
			if input == "internal" {
				connector, err = NewConnector(context.Background(), Config{Credentials: data, AdministratorEmail: "admin@example.com"})
			} else {
				config := &cfg.GoogleWorkspace{AdministratorEmail: "admin@example.com"}
				if input == "inline" {
					config.CredentialsJson = string(data)
				} else {
					config.CredentialsJsonFilePath = data
				}
				builder, _, newErr := New(context.Background(), config, &cli.ConnectorOpts{})
				err = newErr
				if err == nil {
					connector = builder.(*GoogleWorkspace)
				}
			}
			if err != nil {
				t.Fatalf("valid key rejected: %v", err)
			}
			if connector.administratorEmail != "admin@example.com" {
				t.Fatal("delegation subject lost")
			}
			var canonical map[string]string
			if err := json.Unmarshal(connector.credentials, &canonical); err != nil {
				t.Fatal(err)
			}
			if canonical["token_uri"] != googlecredentialguard.TokenURL {
				t.Fatalf("unexpected token endpoint: %q", canonical["token_uri"])
			}
		})
	}
}

func TestGuardedServiceRejectsLegacyUnsafeCredentialBeforeTokenRequest(t *testing.T) {
	for _, input := range []struct{ name, tokenURI, kind string }{
		{"external_endpoint", "https://attacker.example/token", "service_account"},
		{"external_account", googlecredentialguard.TokenURL, "external_account"},
	} {
		t.Run(input.name, func(t *testing.T) {
			_, err := newGWSAdminServiceForScopes(
				context.Background(), guardTestJSON(t, input.tokenURI, input.kind),
				"admin@example.com", directoryAdmin.NewService,
				directoryAdmin.AdminDirectoryUserReadonlyScope,
			)
			if err == nil || !strings.Contains(err.Error(), "credential") {
				t.Fatalf("unsafe legacy credential reached SDK: %v", err)
			}
		})
	}
}

func TestGuardedServicePreservesDelegationSubject(t *testing.T) {
	var subject string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		parts := strings.Split(r.Form.Get("assertion"), ".")
		if len(parts) != 3 {
			t.Errorf("invalid JWT assertion")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Errorf("decode assertion: %v", err)
		}
		var claims struct {
			Subject string `json:"sub"`
		}
		if err := json.Unmarshal(payload, &claims); err != nil {
			t.Errorf("parse assertion: %v", err)
		}
		subject = claims.Subject
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized_client"}`))
	}))
	defer server.Close()
	useTestTokenServer(t, server)
	_, err := newGWSAdminServiceForScopes(
		context.Background(), guardTestJSON(t, googlecredentialguard.TokenURL, "service_account"),
		"admin@example.com", directoryAdmin.NewService,
		directoryAdmin.AdminDirectoryUserReadonlyScope,
	)
	if err == nil {
		t.Fatal("expected mock token server rejection")
	}
	if subject != "admin@example.com" {
		t.Fatalf("delegation subject = %q", subject)
	}
}

func TestCredentialInputsRejectUntrustedJSON(t *testing.T) {
	for _, tc := range []struct{ name, tokenURI, credentialType string }{
		{"external_token_endpoint", "https://attacker.example/token", "service_account"},
		{"non_service_account", "https://oauth2.googleapis.com/token", "external_account"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := guardTestJSON(t, tc.tokenURI, tc.credentialType)
			for _, input := range []string{"inline", "upload", "internal"} {
				t.Run(input, func(t *testing.T) {
					if input == "internal" {
						_, err := NewConnector(context.Background(), Config{Credentials: data})
						if err == nil {
							t.Fatal("untrusted credentials accepted")
						}
						return
					}
					config := &cfg.GoogleWorkspace{}
					if input == "inline" {
						config.CredentialsJson = string(data)
					} else {
						config.CredentialsJsonFilePath = data
					}
					_, _, err := New(context.Background(), config, &cli.ConnectorOpts{})
					if err == nil || !strings.Contains(err.Error(), "credential") {
						t.Fatalf("untrusted credentials accepted or wrong error: %v", err)
					}
				})
			}
		})
	}
}
