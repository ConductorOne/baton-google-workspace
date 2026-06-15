package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"golang.org/x/sync/semaphore"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/option"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// newStatusDirectoryService returns a directory service whose every request resolves to the
// given HTTP status. googleapi derives googleapi.Error.Code from the response status, so this is
// enough to simulate a 403/404/5xx from any Directory API endpoint.
func newStatusDirectoryService(t *testing.T, status int) *directoryAdmin.Service {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"injected failure"}}`))
	}))
	t.Cleanup(server.Close)

	ds, err := directoryAdmin.NewService(context.Background(),
		option.WithEndpoint(server.URL+"/"),
		option.WithHTTPClient(server.Client()),
	)
	if err != nil {
		t.Fatalf("newStatusDirectoryService: %v", err)
	}
	return ds
}

func testRoleResource() *v2.Resource {
	return &v2.Resource{
		Id: &v2.ResourceId{ResourceType: resourceTypeRole.Id, Resource: "1234567890"},
	}
}

// TestRoleGrantsPropagatesAPIErrors guards the prune-risk fix: a failed ListRoleAssignments must
// surface an error, never an empty grant slice with a nil error (which c1 reads as "this role has
// zero members" and prunes every existing role membership). This includes 404 — a role deleted or
// briefly unreachable mid-sync must not silently revoke all of its assignments.
func TestRoleGrantsPropagatesAPIErrors(t *testing.T) {
	for _, status := range []int{
		http.StatusForbidden,           // 403
		http.StatusNotFound,            // 404 (previously swallowed -> empty + nil)
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusServiceUnavailable,  // 503
	} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &gwclient.GoogleWorkspaceClient{RoleService: newStatusDirectoryService(t, status)}
			r := roleBuilder(client, "customer")

			grants, _, err := r.Grants(context.Background(), testRoleResource(), rs.SyncOpAttrs{})
			if err == nil {
				t.Fatalf("status %d: expected Grants to return an error, got nil (prune risk)", status)
			}
			if grants != nil {
				t.Fatalf("status %d: expected nil grants on error, got %d", status, len(grants))
			}
		})
	}
}

// TestRoleListPropagatesAPIErrors mirrors the above for List: a failed ListRoles must not yield an
// empty resource slice with a nil error, which would prune every role and all of their grants.
func TestRoleListPropagatesAPIErrors(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &gwclient.GoogleWorkspaceClient{RoleService: newStatusDirectoryService(t, status)}
			r := roleBuilder(client, "customer")

			resources, _, err := r.List(context.Background(), nil, rs.SyncOpAttrs{})
			if err == nil {
				t.Fatalf("status %d: expected List to return an error, got nil (prune risk)", status)
			}
			if resources != nil {
				t.Fatalf("status %d: expected nil resources on error, got %d", status, len(resources))
			}
		})
	}
}

// TestFetchUserTokensSurfacesTransientErrors guards the OAuth-app discovery fix: a transient/auth
// failure listing one user's tokens must abort discovery rather than silently skipping the user
// (which under-reports app-access grants and can drop apps entirely -> c1 prunes them).
func TestFetchUserTokensSurfacesTransientErrors(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &gwclient.GoogleWorkspaceClient{UserSecurityService: newStatusDirectoryService(t, status)}
			sem := semaphore.NewWeighted(appDiscoveryWorkers)

			_, err := fetchUserTokens(context.Background(), sem, client, []*directoryAdmin.User{{Id: "user-1"}})
			if err == nil {
				t.Fatalf("status %d: expected fetchUserTokens to surface the error, got nil (prune risk)", status)
			}
		})
	}
}

// TestFetchUserTokensToleratesUserDeletedMidSync confirms the one intentionally-benign case: a 404
// (user deleted between listing and token fetch) skips that user without failing discovery.
func TestFetchUserTokensToleratesUserDeletedMidSync(t *testing.T) {
	client := &gwclient.GoogleWorkspaceClient{UserSecurityService: newStatusDirectoryService(t, http.StatusNotFound)}
	sem := semaphore.NewWeighted(appDiscoveryWorkers)

	results, err := fetchUserTokens(context.Background(), sem, client, []*directoryAdmin.User{{Id: "deleted-user"}})
	if err != nil {
		t.Fatalf("expected 404 (deleted user) to be tolerated, got error: %v", err)
	}
	if len(results) != 1 || len(results[0].apps) != 0 {
		t.Fatalf("expected the deleted user to be skipped with no apps, got %+v", results)
	}
}
