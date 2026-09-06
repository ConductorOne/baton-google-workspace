package connector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestGoogleUnchangedReadsProduceStableResources(t *testing.T) {
	var suspended atomic.Bool
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		body := fmt.Sprintf(`{"id":"user-id","customerId":"test-customer","primaryEmail":"user@example.com","suspended":%t,"archived":false,"aliases":[]}`, suspended.Load())
		if r.URL.Path == "/admin/directory/v1/users" {
			body = `{"users":[` + body + `]}`
		}
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	builder := newTestUserResourceType(t, server)
	id := &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "user-id"}
	first, _, err := builder.Get(t.Context(), id, nil)
	require.NoError(t, err)
	second, _, err := builder.Get(t.Context(), id, nil)
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load(), "separate provider reads must still occur")
	require.True(t, proto.Equal(first, second), "an observation clock must not churn stable resource content")
	listed, _, err := builder.List(t.Context(), nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.True(t, proto.Equal(first, listed[0]), "equivalent provider facts must produce equivalent List/Get resources")
	state := second.GetProfile().GetFields()["google_user_state"].GetStructValue().GetFields()
	require.NotContains(t, state, "observed_at")
	suspended.Store(true)
	changed, _, err := builder.Get(t.Context(), id, nil)
	require.NoError(t, err)
	require.False(t, proto.Equal(second, changed), "real provider changes must remain visible")
	require.True(t, changed.GetProfile().GetFields()["google_user_state"].GetStructValue().GetFields()["suspended"].GetBoolValue())
}
