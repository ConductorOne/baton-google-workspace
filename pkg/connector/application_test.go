package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	cloudidentity "google.golang.org/api/cloudidentity/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// TestIsCloudIdentityAPIDisabledError verifies the classifier only matches a permanent
// "API not enabled for project" 403 (via the structured SERVICE_DISABLED detail or the legacy
// accessNotConfigured reason) and nothing else — a plain 403, a 5xx, or a non-Google error must
// NOT be treated as feature-unavailable, so they keep propagating and protect against prune.
func TestIsCloudIdentityAPIDisabledError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"non-googleapi error", errors.New("boom"), false},
		{
			"403 SERVICE_DISABLED detail",
			&googleapi.Error{Code: http.StatusForbidden, Details: []interface{}{
				map[string]interface{}{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "SERVICE_DISABLED"},
			}},
			true,
		},
		{
			"403 accessNotConfigured legacy item",
			&googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "accessNotConfigured"}}},
			true,
		},
		{
			"wrapped 403 disabled error",
			fmt.Errorf("failed to list SAML profiles: %w",
				&googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "accessNotConfigured"}}}),
			true,
		},
		{
			"403 with a different reason (real permission denied)",
			&googleapi.Error{Code: http.StatusForbidden, Errors: []googleapi.ErrorItem{{Reason: "forbidden"}}},
			false,
		},
		{"403 with no reason", &googleapi.Error{Code: http.StatusForbidden}, false},
		{
			"500 even with disabled-looking reason",
			&googleapi.Error{Code: http.StatusInternalServerError, Errors: []googleapi.ErrorItem{{Reason: "accessNotConfigured"}}},
			false,
		},
		{"429", &googleapi.Error{Code: http.StatusTooManyRequests}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCloudIdentityAPIDisabledError(tc.err); got != tc.want {
				t.Fatalf("isCloudIdentityAPIDisabledError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// newCloudIdentityServiceReturning builds a Cloud Identity service whose every request resolves to
// the given status and JSON body, so tests exercise real googleapi error parsing.
func newCloudIdentityServiceReturning(t *testing.T, status int, body string) *cloudidentity.Service {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	svc, err := cloudidentity.NewService(context.Background(),
		option.WithEndpoint(server.URL+"/"),
		option.WithHTTPClient(server.Client()),
	)
	if err != nil {
		t.Fatalf("newCloudIdentityServiceReturning: %v", err)
	}
	return svc
}

// The real body Google returns when the Cloud Identity API is disabled for a project (trimmed).
const cloudIdentityDisabledBody = `{
  "error": {
    "code": 403,
    "message": "Cloud Identity API has not been used in project 843426744470 before or it is disabled.",
    "errors": [{"message": "Cloud Identity API has not been used in project 843426744470 before or it is disabled.", "domain": "usageLimits", "reason": "accessNotConfigured"}],
    "status": "PERMISSION_DENIED",
    "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "SERVICE_DISABLED", "domain": "googleapis.com", "metadata": {"service": "cloudidentity.googleapis.com"}}]
  }
}`

// TestBuildSAMLProfileMapDisabledAPIClassified proves that the actual error produced by the
// Cloud Identity API when it is disabled — parsed by googleapi for real, then wrapped by
// BuildSAMLProfileMap — is recognised by the classifier (so application.List falls back rather
// than aborting).
func TestBuildSAMLProfileMapDisabledAPIClassified(t *testing.T) {
	client := &gwclient.GoogleWorkspaceClient{
		CloudIdentityService: newCloudIdentityServiceReturning(t, http.StatusForbidden, cloudIdentityDisabledBody),
	}

	_, err := client.BuildSAMLProfileMap(context.Background(), "customer")
	if err == nil {
		t.Fatal("expected BuildSAMLProfileMap to return an error for a disabled API")
	}
	if !isCloudIdentityAPIDisabledError(err) {
		t.Fatalf("expected disabled-API 403 to be classified as API-disabled, got %v", err)
	}
}

// TestBuildSAMLProfileMapOtherErrorsNotClassified guards the prune-safety boundary: a generic 403
// (no SERVICE_DISABLED/accessNotConfigured) and a 503 must NOT be classified as API-disabled, so
// application.List keeps propagating them instead of silently falling back.
func TestBuildSAMLProfileMapOtherErrorsNotClassified(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &gwclient.GoogleWorkspaceClient{
				CloudIdentityService: newCloudIdentityServiceReturning(t, status, `{"error":{"code":`+fmt.Sprint(status)+`,"message":"injected failure"}}`),
			}

			_, err := client.BuildSAMLProfileMap(context.Background(), "customer")
			if err == nil {
				t.Fatalf("status %d: expected BuildSAMLProfileMap to return an error", status)
			}
			if isCloudIdentityAPIDisabledError(err) {
				t.Fatalf("status %d: did not expect this error to be classified as API-disabled: %v", status, err)
			}
		})
	}
}

// TestFetchSAMLProfileMapSwallowsDisabledAPI confirms the SAML event-feed path treats a disabled
// Cloud Identity API as a soft failure: nil map, nil error, so the caller falls back to
// display-name IDs (consistent with applicationResource.List).
func TestFetchSAMLProfileMapSwallowsDisabledAPI(t *testing.T) {
	client := &gwclient.GoogleWorkspaceClient{
		CloudIdentityService: newCloudIdentityServiceReturning(t, http.StatusForbidden, cloudIdentityDisabledBody),
	}

	m, err := fetchSAMLProfileMap(context.Background(), client, "customer")
	if err != nil {
		t.Fatalf("expected a disabled API to be a soft failure (nil error), got %v", err)
	}
	if m != nil {
		t.Fatalf("expected a nil profile map on disabled API, got %v", m)
	}
}

// TestFetchSAMLProfileMapPropagatesOtherErrors is the core of this change: every non-disabled
// failure (a generic 403, 5xx, 429) must now propagate instead of being silently swallowed,
// closing the second prune-risk call site that #116 left untouched.
func TestFetchSAMLProfileMapPropagatesOtherErrors(t *testing.T) {
	for _, status := range []int{
		http.StatusForbidden, // generic 403 (no SERVICE_DISABLED / accessNotConfigured)
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &gwclient.GoogleWorkspaceClient{
				CloudIdentityService: newCloudIdentityServiceReturning(t, status, `{"error":{"code":`+fmt.Sprint(status)+`,"message":"injected failure"}}`),
			}

			m, err := fetchSAMLProfileMap(context.Background(), client, "customer")
			if err == nil {
				t.Fatalf("status %d: expected fetchSAMLProfileMap to propagate the error, got nil (prune risk)", status)
			}
			if m != nil {
				t.Fatalf("status %d: expected nil map on error, got %v", status, m)
			}
		})
	}
}

// TestFetchSAMLProfileMapNilServiceIsSoftFailure confirms the scope-not-granted case (nil service)
// remains a soft failure with no error.
func TestFetchSAMLProfileMapNilServiceIsSoftFailure(t *testing.T) {
	m, err := fetchSAMLProfileMap(context.Background(), &gwclient.GoogleWorkspaceClient{}, "customer")
	if err != nil || m != nil {
		t.Fatalf("expected (nil, nil) when CloudIdentityService is nil, got (%v, %v)", m, err)
	}
}
