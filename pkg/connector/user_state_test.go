package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUserGetPreservesUnknownAndFalseState(t *testing.T) {
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads++
		w.Header().Set("Content-Type", "application/json")
		if reads == 1 {
			_, _ = w.Write(
				[]byte(`{"id":"user-id","customerId":"test-customer","primaryEmail":"user@example.com","suspended":false,"aliases":[],"customSchemas":{"spoof":{"google_user_state":"fake"}}}`),
			)
		} else {
			_, _ = w.Write([]byte(
				`{"id":"user-id","customerId":"test-customer","primaryEmail":"user@example.com",` +
					`"suspended":true,"archived":false,"isMailboxSetup":false,"changePasswordAtNextLogin":true,"aliases":["alias@example.com"]}`,
			))
		}
	}))
	defer server.Close()
	o := newTestUserResourceType(t, server)
	id := &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "user-id"}
	first, _, err := o.Get(context.Background(), id, nil)
	require.NoError(t, err)
	state := first.GetProfile().AsMap()["google_user_state"].(map[string]any)
	require.Equal(t, false, state["suspended"])
	require.NotContains(t, state, "archived", "absent provider flag must not become false")
	require.NotContains(t, state, "is_mailbox_setup")
	require.Equal(t, []any{}, state["aliases"])
	second, _, err := o.Get(context.Background(), id, nil)
	require.NoError(t, err)
	state = second.GetProfile().AsMap()["google_user_state"].(map[string]any)
	require.Equal(t, true, state["suspended"])
	require.Equal(t, false, state["archived"])
	require.Equal(t, false, state["is_mailbox_setup"])
	require.Equal(t, true, state["change_password_at_next_login"])
	require.Equal(t, []any{"alias@example.com"}, state["aliases"])
	require.Equal(t, v2.Status_RESOURCE_STATUS_DISABLED, second.GetStatus().GetStatus())
}

func TestUserGetFilteredIsNotAbsence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"user-id","customerId":"another-customer","primaryEmail":"user@example.com"}`))
	}))
	defer server.Close()
	result, _, err := newTestUserResourceType(t, server).Get(context.Background(), &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "user-id"}, nil)
	require.Nil(t, result)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestUserListPreservesStateWithoutPerUserReads(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "/admin/directory/v1/users", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"users":[{"id":"one","primaryEmail":"one@example.com","archived":false},{"id":"two","primaryEmail":"two@example.com","archived":true}],"nextPageToken":"next"}`))
	}))
	defer server.Close()
	users, page, err := newTestUserResourceType(t, server).List(context.Background(), nil, resource.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, users, 2)
	require.Equal(t, false, users[0].GetProfile().AsMap()["google_user_state"].(map[string]any)["archived"])
	require.Equal(t, true, users[1].GetProfile().AsMap()["google_user_state"].(map[string]any)["archived"])
	require.NotEmpty(t, page.NextPageToken, "additional page must not be discarded")
	require.Equal(t, 1, calls)
}
