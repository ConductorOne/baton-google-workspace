package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestProfileUpdateRejectsConcurrentChangeAndRetainsSkippedFields(t *testing.T) {
	writes := 0
	overwrote := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = w.Write(
				[]byte(`{"id":"user-id","etag":"\"old-version\"","primaryEmail":"user@example.com","name":{"fullName":"User"},"externalIds":[{"type":"organization","value":"old-employee"}]}`),
			)
			return
		}
		writes++
		if r.Header.Get("If-Match") == `"old-version"` {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`{"error":{"code":412,"message":"profile changed concurrently"}}`))
			return
		}
		overwrote = true
		_, _ = w.Write([]byte(`{"id":"user-id","name":{"fullName":"User"}}`))
	}))
	defer server.Close()
	result, _, err := newTestUserResourceType(t, server).updateUserProfileActionHandler(context.Background(), &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: strArg("user-id"), argEmployeeID: strArg(""), argManagerEmail: strArg("not-an-email"),
	}})
	require.Error(t, err)
	require.False(t, overwrote, "a stale full-object update must not overwrite a concurrent profile change")
	require.Equal(t, 1, writes, "do not retry using the same stale version")
	require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.Contains(t, result.GetFields()[fieldSkippedFields].GetStringValue(), "manager_email")
}

func TestProfileUpdateWithoutVersionDoesNotWrite(t *testing.T) {
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			writes++
		}
		_, _ = w.Write([]byte(`{"id":"user-id","primaryEmail":"user@example.com","externalIds":[{"type":"organization","value":"employee"}]}`))
	}))
	defer server.Close()
	_, _, err := newTestUserResourceType(t, server).updateUserProfileActionHandler(context.Background(), &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: strArg("user-id"), argEmployeeID: strArg(""),
	}})
	require.Error(t, err)
	require.Zero(t, writes)
}
