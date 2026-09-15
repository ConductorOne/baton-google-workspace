package connector

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type googleFilterTestConnector struct{ user *userResourceType }

func (c *googleFilterTestConnector) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{DisplayName: "Filter fixture"}, nil
}

func (c *googleFilterTestConnector) Validate(context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (c *googleFilterTestConnector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{c.user}
}

func TestGoogleFilteredReadQualifierSurvivesSDKAndGRPC(t *testing.T) {
	for _, filter := range []string{"customer", "domain"} {
		t.Run(filter, func(t *testing.T) {
			var mode atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch mode.Load() {
				case 1:
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"error":{"code":404,"message":"not found"}}`))
				case 2:
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"error":{"code":403,"message":"permission denied"}}`))
				case 3:
					_, _ = w.Write([]byte(`{}`))
				default:
					_, _ = w.Write([]byte(`{"id":"user-id","customerId":"another-customer","primaryEmail":"user@example.com","organizations":[{"domain":"other.example"}]}`))
				}
			}))
			defer provider.Close()
			user := newTestUserResourceType(t, provider)
			if filter == "domain" {
				user.domain = "allowed.example"
			}
			connector, err := connectorbuilder.NewConnector(t.Context(), &googleFilterTestConnector{user: user})
			require.NoError(t, err)
			listenConfig := net.ListenConfig{}
			listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			server := grpc.NewServer()
			v2.RegisterResourceGetterServiceServer(server, connector)
			defer server.Stop()
			go func() { _ = server.Serve(listener) }()
			connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer connection.Close()
			client := v2.NewResourceGetterServiceClient(connection)
			request := &v2.ResourceGetterServiceGetResourceRequest{ResourceId: &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "user-id"}}
			_, err = client.GetResource(t.Context(), request)
			require.Equal(t, codes.NotFound, status.Code(err), "SDK targeted sync must retain its skip classification")
			var info *errdetails.ErrorInfo
			for _, detail := range status.Convert(err).Details() {
				if value, ok := detail.(*errdetails.ErrorInfo); ok {
					info = value
				}
			}
			require.NotNil(t, info)
			require.Equal(t, "RESOURCE_FILTERED", info.Reason)
			require.Equal(t, "baton-google-workspace", info.Domain)
			require.Equal(t, filter, info.Metadata["filter"])
			require.Equal(t, "user-id", info.Metadata["resource_id"])
			mode.Store(1)
			_, err = client.GetResource(t.Context(), request)
			require.Equal(t, codes.NotFound, status.Code(err))
			for _, detail := range status.Convert(err).Details() {
				if value, ok := detail.(*errdetails.ErrorInfo); ok {
					require.NotEqual(t, "RESOURCE_FILTERED", value.Reason)
				}
			}
			mode.Store(2)
			_, err = client.GetResource(t.Context(), request)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			mode.Store(3)
			_, err = client.GetResource(t.Context(), request)
			require.Error(t, err)
			require.NotEqual(t, codes.NotFound, status.Code(err), "malformed success must not become absence")
		})
	}
}

func TestGoogleFilterEncodingFailureCannotClaimAbsence(t *testing.T) {
	// Invalid UTF-8 cannot be encoded in ErrorInfo. The error path must not
	// return a bare NotFound that consumers could mistake for provider absence.
	err := filteredGoogleUserError(&v2.ResourceId{Resource: string([]byte{0xff})}, "domain")
	require.Equal(t, codes.Internal, status.Code(err))
}
