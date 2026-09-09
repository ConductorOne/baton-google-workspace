package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/stretchr/testify/require"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGroupGetRejectsSettingsFailures(t *testing.T) {
	for _, tc := range []struct {
		name           string
		statusCode     int
		body           string
		missingService bool
		wantCode       codes.Code
	}{
		{
			name:       "permission denied",
			statusCode: http.StatusForbidden,
			body:       `{"error":{"code":403,"message":"permission denied"}}`,
			wantCode:   codes.PermissionDenied,
		},
		{
			name:       "service unavailable",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"error":{"code":503,"message":"temporarily unavailable"}}`,
			wantCode:   codes.Unavailable,
		},
		{
			name:       "settings missing is not group absence",
			statusCode: http.StatusNotFound,
			body:       `{"error":{"code":404,"message":"settings not found"}}`,
			wantCode:   codes.FailedPrecondition,
		},
		{
			name:       "empty settings response",
			statusCode: http.StatusOK,
			body:       `{}`,
			wantCode:   codes.DataLoss,
		},
		{
			name:       "different group settings",
			statusCode: http.StatusOK,
			body:       `{"email":"other@example.com"}`,
			wantCode:   codes.DataLoss,
		},
		{
			name:           "settings service missing",
			missingService: true,
			wantCode:       codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/admin/directory/v1/groups/group-id" {
					_, _ = w.Write([]byte(`{"id":"group-id","email":"team@example.com","name":"Team"}`))
					return
				}
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &gwclient.GoogleWorkspaceClient{
				GroupService: newTestDirectoryService(t, server.URL, server.Client()),
			}
			if !tc.missingService {
				settings, err := groupssettings.NewService(t.Context(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
				require.NoError(t, err)
				client.GroupsSettingsService = settings
			}
			builder := &groupResourceType{client: client}
			resource, _, err := builder.Get(t.Context(), &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group-id"}, nil)
			require.Equal(t, tc.wantCode, status.Code(err))
			require.Nil(t, resource, "a failed settings read must not return a successful partial group")
		})
	}
}

func TestGroupPrivacyReadbackRejectsProviderMismatch(t *testing.T) {
	var patch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/admin/directory/v1/groups/group-id" {
			_, _ = w.Write([]byte(`{"id":"group-id","email":"team@example.com","name":"Team"}`))
			return
		}
		if r.Method == http.MethodPatch {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&patch))
		}
		// Provider accepts the patch but the independent read does not confirm it.
		_, _ = w.Write([]byte(`{"email":"team@example.com","whoCanViewGroup":"ANYONE_CAN_VIEW","includeInGlobalAddressList":"true"}`))
	}))
	defer server.Close()
	settings, err := groupssettings.NewService(context.Background(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
	require.NoError(t, err)
	o := &groupResourceType{resourceType: resourceTypeGroup, client: &gwclient.GoogleWorkspaceClient{
		GroupService: newTestDirectoryService(t, server.URL, server.Client()), GroupsSettingsService: settings,
	}}
	args, err := structpb.NewStruct(map[string]any{"group_key": "group-id", "who_can_view_group": "ALL_MEMBERS_CAN_VIEW", "include_in_global_address_list": false})
	require.NoError(t, err)
	result, _, err := o.modifyGroupSettingsActionHandler(context.Background(), args)
	require.Error(t, err)
	require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.Equal(t, map[string]any{"whoCanViewGroup": "ALL_MEMBERS_CAN_VIEW", "includeInGlobalAddressList": "false"}, patch)
	require.Equal(t, "ALL_MEMBERS_CAN_VIEW", result.GetFields()["new_who_can_view_group"].GetStringValue())
	require.Contains(t, result.AsMap(), fieldResource, "retain the observed group alongside the failed verification")
}

func TestGroupSettingsPatchFailureRetainsEvidenceThroughSDK(t *testing.T) {
	var patches, settingsReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/admin/directory/v1/groups/group-id":
			_, _ = w.Write([]byte(`{"id":"group-id","email":"team@example.com","name":"Team"}`))
		case r.Method == http.MethodPatch:
			patches.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":503,"message":"temporarily unavailable"}}`))
		default:
			settingsReads.Add(1)
			_, _ = w.Write([]byte(`{"email":"team@example.com","whoCanViewGroup":"ANYONE_CAN_VIEW"}`))
		}
	}))
	defer server.Close()
	settings, err := groupssettings.NewService(t.Context(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
	require.NoError(t, err)
	o := &groupResourceType{resourceType: resourceTypeGroup, client: &gwclient.GoogleWorkspaceClient{
		GroupService: newTestDirectoryService(t, server.URL, server.Client()), GroupsSettingsService: settings,
	}}
	manager := actions.NewActionManager(t.Context())
	registry, err := manager.GetTypeRegistry(t.Context(), resourceTypeGroup.Id)
	require.NoError(t, err)
	require.NoError(t, o.ResourceActions(t.Context(), registry))
	args, err := structpb.NewStruct(map[string]any{"group_key": "group-id", "who_can_view_group": "ALL_MEMBERS_CAN_VIEW"})
	require.NoError(t, err)
	_, outcome, result, _, err := manager.InvokeAction(t.Context(), "modify_group_settings", resourceTypeGroup.Id, args)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, outcome)
	require.NotNil(t, result)
	require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.Equal(t, "ANYONE_CAN_VIEW", result.GetFields()["previous_who_can_view_group"].GetStringValue())
	require.Equal(t, "ALL_MEMBERS_CAN_VIEW", result.GetFields()["new_who_can_view_group"].GetStringValue())
	require.Contains(t, result.GetFields()["error"].GetStringValue(), "Unavailable")
	require.NotContains(t, result.AsMap(), fieldResource, "failed write must not fabricate a verified post-state")
	require.Equal(t, int32(1), patches.Load(), "unknown mutation must not be replayed")
	require.Equal(t, int32(1), settingsReads.Load())
}

func TestGroupGrantConflictUsesExactMemberRead(t *testing.T) {
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"code":409,"message":"already exists"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"wrong-user","type":"USER"}`))
	}))
	defer server.Close()
	o := &groupResourceType{client: &gwclient.GoogleWorkspaceClient{GroupMemberProvisioningService: newTestDirectoryService(t, server.URL, server.Client())}}
	grants, _, err := o.Grant(
		context.Background(),
		&v2.Resource{Id: &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "expected-user"}},
		&v2.Entitlement{Resource: &v2.Resource{Id: &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group-id"}}},
	)
	require.Error(t, err)
	require.Empty(t, grants)
	require.Equal(t, []string{"POST /admin/directory/v1/groups/group-id/members", "GET /admin/directory/v1/groups/group-id/members/expected-user"}, paths)
}

func TestGroupPrivacyPreservesOmittedSettings(t *testing.T) {
	current := map[string]any{
		"email": "team@example.com", "allowExternalMembers": "true",
		"whoCanViewGroup": "ANYONE_CAN_VIEW", "whoCanViewMembership": "ALL_IN_DOMAIN_CAN_VIEW",
		"whoCanDiscoverGroup": "ANYONE_CAN_DISCOVER", "includeInGlobalAddressList": "true",
		"whoCanJoin": "ANYONE_CAN_JOIN",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/admin/directory/v1/groups/group-id" {
			_, _ = w.Write([]byte(`{"id":"group-id","email":"team@example.com","name":"Team"}`))
			return
		}
		if r.Method == http.MethodPatch {
			var patch map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&patch))
			for key, value := range patch {
				current[key] = value
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(current))
	}))
	defer server.Close()
	settings, err := groupssettings.NewService(context.Background(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
	require.NoError(t, err)
	o := &groupResourceType{resourceType: resourceTypeGroup, client: &gwclient.GoogleWorkspaceClient{
		GroupService: newTestDirectoryService(t, server.URL, server.Client()), GroupsSettingsService: settings,
	}}
	args, err := structpb.NewStruct(map[string]any{
		"group_key": "group-id", "who_can_view_group": "ALL_MEMBERS_CAN_VIEW",
		"who_can_view_membership": "ALL_MANAGERS_CAN_VIEW", "who_can_discover_group": "ALL_MEMBERS_CAN_DISCOVER",
		"include_in_global_address_list": false, "who_can_join": "INVITED_CAN_JOIN",
	})
	require.NoError(t, err)
	result, _, err := o.modifyGroupSettingsActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.True(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.Equal(t, "true", current["allowExternalMembers"], "omitted setting must not be reset")
	id := &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group-id"}
	group, _, err := o.Get(context.Background(), id, nil)
	require.NoError(t, err)
	profile := group.GetProfile().AsMap()
	require.Equal(t, "INVITED_CAN_JOIN", profile["group_settings"].(map[string]any)["who_can_join"])
	require.Equal(t, "false", profile["group_settings"].(map[string]any)["include_in_global_address_list"])
}

func TestGroupReadsStayStableUntilProviderSettingsChange(t *testing.T) {
	settingsReads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/admin/directory/v1/groups/group-id" {
			_, _ = w.Write([]byte(`{"id":"group-id","email":"team@example.com","name":"Team"}`))
			return
		}
		settingsReads++
		if settingsReads < 3 {
			_, _ = w.Write([]byte(`{"email":"team@example.com","includeInGlobalAddressList":"true"}`))
		} else {
			_, _ = w.Write([]byte(`{"email":"team@example.com","includeInGlobalAddressList":"false"}`))
		}
	}))
	defer server.Close()
	settings, err := groupssettings.NewService(t.Context(), option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL+"/"))
	require.NoError(t, err)
	builder := &groupResourceType{client: &gwclient.GoogleWorkspaceClient{
		GroupService: newTestDirectoryService(t, server.URL, server.Client()), GroupsSettingsService: settings,
	}}
	id := &v2.ResourceId{ResourceType: resourceTypeGroup.Id, Resource: "group-id"}
	first, _, err := builder.Get(t.Context(), id, nil)
	require.NoError(t, err)
	second, _, err := builder.Get(t.Context(), id, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(first, second), "unchanged provider facts must not churn the resource")
	changed, _, err := builder.Get(t.Context(), id, nil)
	require.NoError(t, err)
	require.False(t, proto.Equal(first, changed), "an actual provider change must remain visible")
	require.Equal(t, "false", changed.GetProfile().AsMap()["group_settings"].(map[string]any)["include_in_global_address_list"])
}
