package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"golang.org/x/oauth2"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
)

// fastRetry returns retry settings that exercise the bounded backoff without
// slowing tests down. maxRetries retries follow the initial attempt.
func fastRetry(maxRetries int) (int, time.Duration, time.Duration) {
	return maxRetries, time.Millisecond, 2 * time.Millisecond
}

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

	maxRetries, base, maxDelay := fastRetry(2)
	c := &GoogleWorkspace{
		customerID:                "customer",
		administratorEmail:        "admin@example.com",
		credentials:               testCredentials(t, tokenServer.URL),
		serviceCache:              map[string]any{},
		serviceInitRetryMax:       maxRetries,
		serviceInitRetryBaseDelay: base,
		serviceInitRetryMaxDelay:  maxDelay,
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
	// A 503 is transient: the initial attempt plus maxRetries retries all fail,
	// then the sync aborts (rather than persisting a partial c1z).
	if want := 1 + maxRetries; *calls != want {
		t.Fatalf("expected %d token requests (initial + %d retries) before aborting, got %d", want, maxRetries, *calls)
	}
}

func TestGetClientDoesNotCacheNonAuthorizationInitError(t *testing.T) {
	tokenServer, calls := newTokenStatusServer(http.StatusServiceUnavailable)
	defer tokenServer.Close()

	maxRetries, base, maxDelay := fastRetry(2)
	c := &GoogleWorkspace{
		customerID:                "customer",
		administratorEmail:        "admin@example.com",
		credentials:               testCredentials(t, tokenServer.URL),
		serviceCache:              map[string]any{},
		serviceInitRetryMax:       maxRetries,
		serviceInitRetryBaseDelay: base,
		serviceInitRetryMaxDelay:  maxDelay,
	}

	if _, err := c.getClient(context.Background()); err == nil {
		t.Fatalf("expected first getClient to fail")
	}
	if _, err := c.getClient(context.Background()); err == nil {
		t.Fatalf("expected second getClient to fail")
	}
	// A failed init is not cached: each getClient re-runs the full attempt +
	// retry sequence against the token endpoint.
	if want := 2 * (1 + maxRetries); *calls != want {
		t.Fatalf("expected failed init to be retried fresh each call (%d token requests), got %d", want, *calls)
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

// newFlakyTokenServer fails the first failCount requests with the given status,
// then returns a valid access token. This simulates a transient Google outage.
func newFlakyTokenServer(failCount, failStatus int) (*httptest.Server, *int) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls <= failCount {
			w.WriteHeader(failStatus)
			_, _ = w.Write([]byte(`{"error":"internal_failure","error_description":"transient"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"fake-token","token_type":"Bearer","expires_in":3600}`))
	}))
	return server, &calls
}

func TestServiceInitRetriesTransientThenSucceeds(t *testing.T) {
	// Token endpoint fails twice with 503 (the IR-850 shape), then recovers.
	tokenServer, calls := newFlakyTokenServer(2, http.StatusServiceUnavailable)
	defer tokenServer.Close()

	maxRetries, base, maxDelay := fastRetry(4)
	c := &GoogleWorkspace{
		customerID:                "customer",
		administratorEmail:        "admin@example.com",
		credentials:               testCredentials(t, tokenServer.URL),
		serviceCache:              map[string]any{},
		serviceInitRetryMax:       maxRetries,
		serviceInitRetryBaseDelay: base,
		serviceInitRetryMaxDelay:  maxDelay,
	}

	svc, err := c.getDirectoryService(context.Background(), directoryAdmin.AdminDirectoryUserReadonlyScope)
	if err != nil {
		t.Fatalf("expected service init to succeed after transient failures, got %v", err)
	}
	if svc == nil {
		t.Fatalf("expected a non-nil directory service after recovery")
	}
	if *calls != 3 {
		t.Fatalf("expected 3 token requests (2 transient failures + 1 success), got %d", *calls)
	}
}

func TestServiceInitDoesNotRetryUnauthorized(t *testing.T) {
	// A 401 is a real authorization problem: surfaced immediately, never retried.
	tokenServer, calls := newTokenStatusServer(http.StatusUnauthorized)
	defer tokenServer.Close()

	maxRetries, base, maxDelay := fastRetry(4)
	c := &GoogleWorkspace{
		customerID:                "customer",
		administratorEmail:        "admin@example.com",
		credentials:               testCredentials(t, tokenServer.URL),
		serviceCache:              map[string]any{},
		serviceInitRetryMax:       maxRetries,
		serviceInitRetryBaseDelay: base,
		serviceInitRetryMaxDelay:  maxDelay,
	}

	// The user scope can be upgraded (.readonly stripped), so getService retries
	// the upgraded scope once on 401 — that's 2 token requests, not a backoff loop.
	_, err := c.getDirectoryService(context.Background(), directoryAdmin.AdminDirectoryUserReadonlyScope)
	if err == nil {
		t.Fatalf("expected unauthorized error")
	}
	var ae *GoogleWorkspaceOAuthUnauthorizedError
	if !errors.As(err, &ae) {
		t.Fatalf("expected GoogleWorkspaceOAuthUnauthorizedError, got %T: %v", err, err)
	}
	if *calls != 2 {
		t.Fatalf("expected 2 token requests (scope + upgraded scope, no backoff retries), got %d", *calls)
	}
}

func TestIsTransientServiceInitError(t *testing.T) {
	// retrieveErr builds an *oauth2.RetrieveError carrying the given status. The
	// http.Response is a bare struct literal (no body), so there is nothing to
	// close — bodyclose only tracks responses returned from function calls.
	retrieveErr := func(code int) *oauth2.RetrieveError {
		re := &oauth2.RetrieveError{}
		re.Response = &http.Response{StatusCode: code}
		return re
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"503 retrieve", retrieveErr(http.StatusServiceUnavailable), true},
		{"500 retrieve", retrieveErr(http.StatusInternalServerError), true},
		{"429 retrieve", retrieveErr(http.StatusTooManyRequests), true},
		{"401 retrieve", retrieveErr(http.StatusUnauthorized), false},
		{"400 retrieve", retrieveErr(http.StatusBadRequest), false},
		{"403 retrieve", retrieveErr(http.StatusForbidden), false},
		{"unauthorized wrapper", &GoogleWorkspaceOAuthUnauthorizedError{o: retrieveErr(http.StatusUnauthorized)}, false},
		{"googleapi 503", &googleapi.Error{Code: http.StatusServiceUnavailable}, true},
		{"googleapi 404", &googleapi.Error{Code: http.StatusNotFound}, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"net timeout", &net.OpError{Op: "dial", Err: errors.New("i/o timeout")}, true},
		{"plain error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientServiceInitError(tc.err); got != tc.want {
				t.Fatalf("isTransientServiceInitError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWithServiceInitRetryStopsOnPermanentError(t *testing.T) {
	c := &GoogleWorkspace{serviceInitRetryMax: 5, serviceInitRetryBaseDelay: time.Millisecond, serviceInitRetryMaxDelay: time.Millisecond}
	permanent := errors.New("permanent")
	attempts := 0
	err := c.withServiceInitRetry(context.Background(), func() error {
		attempts++
		return permanent
	})
	if !errors.Is(err, permanent) {
		t.Fatalf("expected permanent error returned, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected permanent error to short-circuit after 1 attempt, got %d", attempts)
	}
}

func TestWithServiceInitRetryExhaustsOnTransientError(t *testing.T) {
	c := &GoogleWorkspace{serviceInitRetryMax: 3, serviceInitRetryBaseDelay: time.Millisecond, serviceInitRetryMaxDelay: 2 * time.Millisecond}
	transient := &oauth2.RetrieveError{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}
	attempts := 0
	err := c.withServiceInitRetry(context.Background(), func() error {
		attempts++
		return transient
	})
	if !errors.As(err, new(*oauth2.RetrieveError)) {
		t.Fatalf("expected transient error returned after exhaustion, got %v", err)
	}
	if want := 1 + 3; attempts != want {
		t.Fatalf("expected %d attempts (initial + 3 retries), got %d", want, attempts)
	}
}

func TestWithServiceInitRetryHonorsContextCancellation(t *testing.T) {
	c := &GoogleWorkspace{serviceInitRetryMax: 10, serviceInitRetryBaseDelay: time.Hour, serviceInitRetryMaxDelay: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	transient := &oauth2.RetrieveError{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}
	attempts := 0
	go func() {
		// Give the first attempt time to fail and enter the backoff sleep.
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := c.withServiceInitRetry(ctx, func() error {
		attempts++
		return transient
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled while sleeping between retries, got %v", err)
	}
}
