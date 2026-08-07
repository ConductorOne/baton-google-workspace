package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

func newGroupDeleteTestServer(t *testing.T, statusCode int, reason string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/groups/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code":    statusCode,
				"message": "error",
				"errors":  []map[string]string{{"reason": reason, "message": "error"}},
			},
		})
	})
	return httptest.NewServer(mux)
}

func newTestGroupResourceType(t *testing.T, server *httptest.Server) *groupResourceType {
	t.Helper()
	dir := newTestDirectoryService(t, server.URL, server.Client())
	return &groupResourceType{
		resourceType: resourceTypeGroup,
		client: &gwclient.GoogleWorkspaceClient{
			GroupProvisioningService: dir,
		},
	}
}

func TestGroupDelete_ThrottledForbidden_StaysUnavailable(t *testing.T) {
	server := newGroupDeleteTestServer(t, http.StatusForbidden, "userRateLimitExceeded")
	defer server.Close()
	o := newTestGroupResourceType(t, server)

	_, err := o.Delete(context.Background(), &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group1"}, nil)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err), "a throttled 403 must not be rewritten to PermissionDenied")
}

func TestGroupDelete_GenuineForbidden_GetsScopeHint(t *testing.T) {
	server := newGroupDeleteTestServer(t, http.StatusForbidden, "forbidden")
	defer server.Close()
	o := newTestGroupResourceType(t, server)

	_, err := o.Delete(context.Background(), &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group1"}, nil)
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Contains(t, err.Error(), "check the")
}

func TestGroupDelete_NotFound_IsIdempotentSuccess(t *testing.T) {
	server := newGroupDeleteTestServer(t, http.StatusNotFound, "notFound")
	defer server.Close()
	o := newTestGroupResourceType(t, server)

	_, err := o.Delete(context.Background(), &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group1"}, nil)
	require.NoError(t, err)
}
