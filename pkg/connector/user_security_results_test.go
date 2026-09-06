package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestOAuthRevocationRetainsPartialResults(t *testing.T) {
	lists := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet:
			lists++
			if lists == 1 {
				_, _ = w.Write([]byte(`{"items":[{"clientId":"deleted"},{"clientId":"denied"},{}]}`))
			} else {
				_, _ = w.Write([]byte(`{"items":[{"clientId":"denied"},{"clientId":"new-token"}]}`))
			}
		case strings.HasSuffix(r.URL.Path, "/deleted"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"permission denied"}}`))
		}
	}))
	defer server.Close()
	o := newTestUserResourceTypeWithSecurity(t, server)
	result, _, err := o.deleteAllOAuthTokensActionHandler(context.Background(), &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: structpb.NewStringValue("stable-user"),
	}})
	require.Error(t, err)
	require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.Equal(t, float64(1), result.GetFields()["tokens_deleted"].GetNumberValue())
	require.Equal(t, []any{"deleted"}, result.AsMap()["deleted_ids"])
	require.Equal(t, []any{"denied"}, result.AsMap()["failed_ids"])
	require.Equal(t, []any{""}, result.AsMap()["skipped_ids"])
	require.Equal(t, []any{"denied", "new-token"}, result.AsMap()["remaining_ids"])
	require.True(t, result.GetFields()["inventory_complete"].GetBoolValue())
}

func TestApplicationPasswordRevocationRequiresFinalRead(t *testing.T) {
	lists := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		lists++
		if lists == 1 {
			_, _ = w.Write([]byte(`{"items":[{"codeId":42}]}`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":403,"message":"permission denied"}}`))
	}))
	defer server.Close()
	result, _, err := newTestUserResourceTypeWithSecurity(t, server).deleteAllApplicationPasswordsActionHandler(context.Background(), &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: structpb.NewStringValue("stable-user"),
	}})
	require.Error(t, err)
	require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.False(t, result.GetFields()["inventory_complete"].GetBoolValue())
	require.Equal(t, float64(1), result.GetFields()["passwords_deleted"].GetNumberValue())
	require.Equal(t, []any{"42"}, result.AsMap()["deleted_ids"])
}
