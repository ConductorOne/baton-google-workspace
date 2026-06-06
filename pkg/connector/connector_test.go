package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func testPrivateKey(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func testCredentials(t *testing.T, tokenURL string) []byte {
	t.Helper()

	creds := map[string]string{
		"type":         "service_account",
		"client_email": "svc@example.iam.gserviceaccount.com",
		"private_key":  testPrivateKey(t),
		"token_uri":    tokenURL,
	}
	rv, err := json.Marshal(creds)
	if err != nil {
		t.Fatalf("failed to marshal credentials: %v", err)
	}
	return rv
}

func newTokenStatusServer(status int) (*httptest.Server, *int) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"internal_failure","error_description":"test token failure"}`))
	}))
	return server, &calls
}

func TestResourceSyncersReturnFailingSyncersForNonAuthorizationInitError(t *testing.T) {
	tokenServer, calls := newTokenStatusServer(http.StatusServiceUnavailable)
	defer tokenServer.Close()

	c := &GoogleWorkspace{
		customerID:         "customer",
		administratorEmail: "admin@example.com",
		credentials:        testCredentials(t, tokenServer.URL),
		serviceCache:       map[string]any{},
	}

	syncers := c.ResourceSyncers(context.Background())
	if len(syncers) != 4 {
		t.Fatalf("expected failing syncers for all resource types, got %d", len(syncers))
	}

	var groupSyncer *failedResourceSyncer
	for _, syncer := range syncers {
		if syncer.ResourceType(context.Background()).GetId() == resourceTypeGroup.Id {
			var ok bool
			groupSyncer, ok = syncer.(*failedResourceSyncer)
			if !ok {
				t.Fatalf("expected group syncer to be failedResourceSyncer, got %T", syncer)
			}
			break
		}
	}
	if groupSyncer == nil {
		t.Fatalf("expected group resource type to be registered with a failing syncer")
	}

	_, _, err := groupSyncer.List(context.Background(), nil, rs.SyncOpAttrs{})
	if err == nil {
		t.Fatalf("expected group List to return client init error")
	}
	if !strings.Contains(err.Error(), "failed to initialize domain service") {
		t.Fatalf("expected init error to identify failed service, got %q", err.Error())
	}
	if *calls != 1 {
		t.Fatalf("expected one token request before aborting, got %d", *calls)
	}
}

func TestGetClientDoesNotCacheNonAuthorizationInitError(t *testing.T) {
	tokenServer, calls := newTokenStatusServer(http.StatusServiceUnavailable)
	defer tokenServer.Close()

	c := &GoogleWorkspace{
		customerID:         "customer",
		administratorEmail: "admin@example.com",
		credentials:        testCredentials(t, tokenServer.URL),
		serviceCache:       map[string]any{},
	}

	if _, err := c.getClient(context.Background()); err == nil {
		t.Fatalf("expected first getClient to fail")
	}
	if _, err := c.getClient(context.Background()); err == nil {
		t.Fatalf("expected second getClient to fail")
	}
	if *calls != 2 {
		t.Fatalf("expected failed init to be retried, got %d token requests", *calls)
	}
}

func TestGetClientAllowsAuthorizationInitErrors(t *testing.T) {
	tokenServer, _ := newTokenStatusServer(http.StatusUnauthorized)
	defer tokenServer.Close()

	c := &GoogleWorkspace{
		customerID:         "customer",
		administratorEmail: "admin@example.com",
		credentials:        testCredentials(t, tokenServer.URL),
		serviceCache:       map[string]any{},
	}

	client, err := c.getClient(context.Background())
	if err != nil {
		t.Fatalf("expected authorization errors to be non-fatal, got %v", err)
	}
	if client.GroupService != nil || client.GroupMemberService != nil {
		t.Fatalf("expected unauthorized group services to remain nil")
	}

	syncers := c.ResourceSyncers(context.Background())
	if len(syncers) != 0 {
		t.Fatalf("expected no syncers for missing scopes, got %d", len(syncers))
	}
}
