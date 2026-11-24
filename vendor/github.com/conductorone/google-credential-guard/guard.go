// Package googlecredentialguard validates externally supplied Google service-account keys
// before they are passed to a Google authentication library.
package googlecredentialguard

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"strings"
)

const (
	MaxJSONBytes = 64 << 10
	TokenURL     = "https://oauth2.googleapis.com/token"
	// AccountsTokenURL appears in Google's current Console key format. It is
	// accepted as input but never retained in canonical credentials.
	AccountsTokenURL = "https://accounts.google.com/o/oauth2/token"
)

// ServiceAccount is a validated service-account key. Its canonical JSON omits
// input fields that have no role in service-account authentication.
type ServiceAccount struct {
	canonical []byte
}

// String and GoString keep accidental formatted logging from printing key material.
func (s ServiceAccount) String() string   { return "ServiceAccount(redacted)" }
func (s ServiceAccount) GoString() string { return s.String() }

type serviceAccountJSON struct {
	Type         string `json:"type"`
	ProjectID    string `json:"project_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	TokenURI     string `json:"token_uri"`
}

// ParseServiceAccountJSON rejects untrusted credential formats and produces
// canonical bytes for the type-specific Google SDK loaders.
func ParseServiceAccountJSON(input []byte) (ServiceAccount, error) {
	if len(input) == 0 || len(input) > MaxJSONBytes {
		return ServiceAccount{}, errors.New("google credential: JSON size is invalid")
	}
	fields, err := parseObject(input)
	if err != nil {
		return ServiceAccount{}, err
	}
	allowed := map[string]bool{
		"type": true, "project_id": true, "private_key_id": true,
		"private_key": true, "client_email": true, "client_id": true,
		"auth_uri": true, "token_uri": true, "auth_provider_x509_cert_url": true,
		"client_x509_cert_url": true, "universe_domain": true,
	}
	values := make(map[string]string, len(fields))
	for name, raw := range fields {
		if !allowed[name] {
			if safeFieldName(name) {
				return ServiceAccount{}, fmt.Errorf("google credential: unsupported field %q", name)
			}
			return ServiceAccount{}, errors.New("google credential: unsupported field")
		}
		if len(raw) == 0 || raw[0] != '"' {
			return ServiceAccount{}, errors.New("google credential: field must be a string")
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return ServiceAccount{}, errors.New("google credential: invalid string field")
		}
		values[name] = value
	}
	if values["type"] != "service_account" {
		return ServiceAccount{}, errors.New("google credential: expected service_account type")
	}
	if values["project_id"] == "" || values["private_key_id"] == "" || values["client_email"] == "" {
		return ServiceAccount{}, errors.New("google credential: missing service-account field")
	}
	address, err := mail.ParseAddress(values["client_email"])
	if err != nil || address.Address != values["client_email"] || !strings.HasSuffix(address.Address, ".gserviceaccount.com") {
		return ServiceAccount{}, errors.New("google credential: invalid service-account email")
	}
	if values["universe_domain"] != "" && values["universe_domain"] != "googleapis.com" {
		return ServiceAccount{}, errors.New("google credential: unsupported universe domain")
	}
	if values["token_uri"] != "" && values["token_uri"] != TokenURL && values["token_uri"] != AccountsTokenURL {
		return ServiceAccount{}, errors.New("google credential: unsupported token endpoint")
	}
	block, rest := pem.Decode([]byte(values["private_key"]))
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return ServiceAccount{}, errors.New("google credential: invalid private key")
	}
	privateKey := values["private_key"]
	switch block.Type {
	case "PRIVATE KEY":
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return ServiceAccount{}, errors.New("google credential: invalid private key")
		}
		if _, ok := key.(*rsa.PrivateKey); !ok {
			return ServiceAccount{}, errors.New("google credential: unsupported private key type")
		}
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return ServiceAccount{}, errors.New("google credential: invalid private key")
		}
		encoded, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return ServiceAccount{}, errors.New("google credential: invalid private key")
		}
		privateKey = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}))
	default:
		return ServiceAccount{}, errors.New("google credential: unsupported private key type")
	}
	canonical, err := json.Marshal(serviceAccountJSON{
		Type: "service_account", ProjectID: values["project_id"],
		PrivateKeyID: values["private_key_id"], PrivateKey: privateKey,
		ClientEmail: values["client_email"], TokenURI: TokenURL,
	})
	if err != nil {
		return ServiceAccount{}, errors.New("google credential: failed to encode canonical JSON")
	}
	return ServiceAccount{canonical: canonical}, nil
}

func safeFieldName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, char := range name {
		if char != '_' && (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func parseObject(input []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("google credential: expected JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("google credential: invalid JSON object")
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("google credential: invalid field name")
		}
		if _, exists := fields[name]; exists {
			return nil, errors.New("google credential: duplicate field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("google credential: invalid field value")
		}
		fields[name] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("google credential: invalid JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("google credential: trailing JSON data")
	}
	return fields, nil
}

// CanonicalJSON returns a copy for APIs that only accept credential bytes.
// Prefer ClientOption or JWTConfig, which retain the type-specific loader choice.
func (s ServiceAccount) CanonicalJSON() []byte {
	return bytes.Clone(s.canonical)
}
