package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCreateAccountBlankOptionalOUUsesProviderDefault(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"new-user","primaryEmail":"user@example.com","name":{"fullName":"Test User"}}`))
	}))
	defer server.Close()
	profile, err := structpb.NewStruct(map[string]any{"email": "user@example.com", "given_name": "Test", "family_name": "User", "org_unit_path": ""})
	require.NoError(t, err)
	opts := &v2.LocalCredentialOptions{}
	opts.SetPlaintextPassword(&v2.LocalCredentialOptions_PlaintextPassword{PlaintextPassword: "fixture-only-password"})
	result, _, _, err := newTestUserResourceType(t, server).CreateAccount(t.Context(), &v2.AccountInfo{Profile: profile}, opts)
	require.NoError(t, err)
	_, ok := result.(*v2.CreateAccountResponse_SuccessResult)
	require.True(t, ok)
	require.NotContains(t, body, "orgUnitPath")
}

func TestGroupGetKeepsDirectoryResourceOnSettings503(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/admin/directory/v1/groups/group-id" {
			_, _ = w.Write([]byte(`{"id":"group-id","email":"group@example.com","name":"Group"}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":503,"message":"temporarily unavailable"}}`))
	}))
	defer server.Close()
	settings, err := groupssettings.NewService(t.Context(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
	require.NoError(t, err)
	builder := &groupResourceType{client: &gwclient.GoogleWorkspaceClient{GroupService: newTestDirectoryService(t, server.URL, server.Client()), GroupsSettingsService: settings}}
	resource, _, err := builder.Get(t.Context(), &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group-id"}, nil)
	require.NoError(t, err)
	require.Equal(t, "group-id", resource.GetId().GetResource())
	profile := resource.GetProfile().AsMap()
	require.Equal(t, "unknown", profile["group_settings_status"])
	require.NotContains(t, profile, "group_settings")
}

func TestTransfersRejectCaseInsensitiveSelfTarget(t *testing.T) {
	for _, operation := range []string{"drive", "calendar"} {
		t.Run(operation, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; w.WriteHeader(http.StatusInternalServerError) }))
			defer server.Close()
			connector := newTestConnector()
			primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
			args, err := structpb.NewStruct(map[string]any{"resource_id": "Alice@example.com", "target_resource_id": "alice@example.com"})
			require.NoError(t, err)
			if operation == "drive" {
				_, _, err = connector.transferUserDriveFiles(t.Context(), args)
			} else {
				_, _, err = connector.transferUserCalendar(t.Context(), args)
			}
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Zero(t, calls)
		})
	}
}

func TestOmittedBooleanSettingPreservesIntentAndRequiresObservation(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(map[bool]string{false: "still unknown", true: "observed false"}[observed], func(t *testing.T) {
			patched := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/admin/directory/v1/groups/group-id" {
					_, _ = w.Write([]byte(`{"id":"group-id","email":"group@example.com","name":"Group"}`))
					return
				}
				if r.Method == http.MethodPatch {
					var body map[string]any
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					require.Equal(t, "false", body["allowWebPosting"])
					patched = true
				}
				if patched && observed {
					_, _ = w.Write([]byte(`{"allowWebPosting":"false"}`))
				} else {
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer server.Close()
			settings, err := groupssettings.NewService(context.Background(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
			require.NoError(t, err)
			builder := &groupResourceType{client: &gwclient.GoogleWorkspaceClient{GroupService: newTestDirectoryService(t, server.URL, server.Client()), GroupsSettingsService: settings}}
			args, err := structpb.NewStruct(map[string]any{"group_key": "group-id", "allow_web_posting": false})
			require.NoError(t, err)
			result, _, err := builder.modifyGroupSettingsActionHandler(t.Context(), args)
			require.True(t, patched, "an omitted field is unknown, not already false")
			require.Equal(t, "false", result.GetFields()["new_allow_web_posting"].GetStringValue())
			if observed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, observed, result.GetFields()[fieldSuccess].GetBoolValue())
		})
	}
}
