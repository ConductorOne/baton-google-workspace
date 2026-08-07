package connector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/protobuf/types/known/structpb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// resourceIDArg builds the resource-id struct argument that the global
// update_user action reads via actions.GetResourceIDArg.
func resourceIDArg(resourceType, id string) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"resource_type": strArg(resourceType),
			"resource":      strArg(id),
		},
	}}}
}

func TestProfileFromJSON(t *testing.T) {
	t.Run("snake_case keys", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"given_name":     "Ada",
			"family_name":    "Lovelace",
			"recovery_email": "ada@example.com",
			"recovery_phone": "+14155550100",
		})
		require.NoError(t, err)
		require.NotNil(t, patch.givenName)
		require.Equal(t, "Ada", *patch.givenName)
		require.NotNil(t, patch.familyName)
		require.Equal(t, "Lovelace", *patch.familyName)
		require.NotNil(t, patch.recoveryEmail)
		require.Equal(t, "ada@example.com", *patch.recoveryEmail)
		require.NotNil(t, patch.recoveryPhone)
		require.Equal(t, "+14155550100", *patch.recoveryPhone)
		require.Nil(t, patch.customSchemas)
	})

	t.Run("camelCase aliases", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"givenName":     "Grace",
			"familyName":    "Hopper",
			"recoveryEmail": "grace@example.com",
			"recoveryPhone": "+14155550111",
		})
		require.NoError(t, err)
		require.NotNil(t, patch.givenName)
		require.Equal(t, "Grace", *patch.givenName)
		require.NotNil(t, patch.familyName)
		require.Equal(t, "Hopper", *patch.familyName)
		require.NotNil(t, patch.recoveryEmail)
		require.Equal(t, "grace@example.com", *patch.recoveryEmail)
		require.NotNil(t, patch.recoveryPhone)
		require.Equal(t, "+14155550111", *patch.recoveryPhone)
	})

	t.Run("employee information keys", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"department":    "Engineering",
			"job_title":     "Staff Engineer",
			"cost_center":   "CC-42",
			"employee_type": "Full-time",
			"employee_id":   "E12345",
			"manager_email": "manager@example.com",
		})
		require.NoError(t, err)
		require.NotNil(t, patch.department)
		require.Equal(t, "Engineering", *patch.department)
		require.NotNil(t, patch.jobTitle)
		require.Equal(t, "Staff Engineer", *patch.jobTitle)
		require.NotNil(t, patch.costCenter)
		require.Equal(t, "CC-42", *patch.costCenter)
		require.NotNil(t, patch.employeeType)
		require.Equal(t, "Full-time", *patch.employeeType)
		require.NotNil(t, patch.employeeID)
		require.Equal(t, "E12345", *patch.employeeID)
		require.NotNil(t, patch.managerEmail)
		require.Equal(t, "manager@example.com", *patch.managerEmail)
	})

	t.Run("employee information camelCase aliases", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"jobTitle":     "Director",
			"costCenter":   "CC-99",
			"employeeType": "Intern",
			"employeeId":   "E99999",
			"managerEmail": "boss@example.com",
		})
		require.NoError(t, err)
		require.NotNil(t, patch.jobTitle)
		require.Equal(t, "Director", *patch.jobTitle)
		require.NotNil(t, patch.costCenter)
		require.Equal(t, "CC-99", *patch.costCenter)
		require.NotNil(t, patch.employeeType)
		require.Equal(t, "Intern", *patch.employeeType)
		require.NotNil(t, patch.employeeID)
		require.Equal(t, "E99999", *patch.employeeID)
		require.NotNil(t, patch.managerEmail)
		require.Equal(t, "boss@example.com", *patch.managerEmail)
	})

	t.Run("empty object yields empty patch", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{})
		require.NoError(t, err)
		require.Nil(t, patch.givenName)
		require.Nil(t, patch.familyName)
		require.Nil(t, patch.recoveryEmail)
		require.Nil(t, patch.recoveryPhone)
		require.Nil(t, patch.customSchemas)
	})

	t.Run("empty string is preserved (clear intent)", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{"recovery_email": ""})
		require.NoError(t, err)
		require.NotNil(t, patch.recoveryEmail)
		require.Equal(t, "", *patch.recoveryEmail)
	})

	t.Run("valid custom_schemas object", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"custom_schemas": map[string]any{
				"EmployeeInfo": map[string]any{"region": "emea"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, patch.customSchemas)
		raw, ok := patch.customSchemas["EmployeeInfo"]
		require.True(t, ok)
		require.Contains(t, string(raw), "emea")
	})

	t.Run("custom_schemas must be an object", func(t *testing.T) {
		_, err := profileFromJSON(map[string]any{"custom_schemas": "not-an-object"})
		require.Error(t, err)
	})

	t.Run("empty custom_schemas object leaves field nil", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{"custom_schemas": map[string]any{}})
		require.NoError(t, err)
		require.Nil(t, patch.customSchemas)
	})
}

func newTestGlobalConnector(t *testing.T, dir *directoryAdmin.Service) *GoogleWorkspace {
	t.Helper()
	return &GoogleWorkspace{
		serviceCache: map[string]any{},
		client: &gwclient.GoogleWorkspaceClient{
			UserService:             dir,
			UserProvisioningService: dir,
		},
	}
}

func TestUpdateUserGlobal_PatchesProfile(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "t@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Old", FamilyName: "Name"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:      resourceIDArg("user", "user123"),
		"user_profile": strArg(`{"family_name":"New"}`),
	}}

	resp, _, err := c.updateUserActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.True(t, resp.GetFields()["success"].GetBoolValue())
	require.Equal(t, 1, state.patchCount)
	// read-modify-write of Name: family_name changed, given_name preserved.
	require.Equal(t, "New", state.lastPatchBody.Name.FamilyName)
	require.Equal(t, "Old", state.lastPatchBody.Name.GivenName)
}

func TestUpdateUserGlobal_EmployeeInfoAndManagerViaProfile(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "t@example.com",
				Organizations: []directoryAdmin.UserOrganization{
					{Primary: true, Title: "Old Title"},
				},
				Relations: []directoryAdmin.UserRelation{
					{Type: "manager", Value: "old-manager@example.com"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:      resourceIDArg("user", "user123"),
		"user_profile": strArg(`{"department":"Engineering","manager_email":"new-manager@example.com"}`),
	}}

	resp, _, err := c.updateUserActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.True(t, resp.GetFields()["success"].GetBoolValue())
	require.Equal(t, 1, state.patchCount)
	// Both department and manager_email need a read-modify-write GET; this
	// must be a single shared GET, not one per field.
	require.Equal(t, 1, state.getCount)

	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	require.NoError(t, err)
	require.Len(t, orgs, 1)
	require.Equal(t, "Engineering", orgs[0].Department)
	require.Equal(t, "Old Title", orgs[0].Title, "sibling org field must be preserved")

	rels, err := extractFromInterface[*directoryAdmin.UserRelation](state.lastPatchBody.Relations)
	require.NoError(t, err)
	require.Len(t, rels, 1)
	require.Equal(t, "manager", rels[0].Type)
	require.Equal(t, "new-manager@example.com", rels[0].Value)
}

func TestUpdateUserGlobal_CustomSchemasViaProfile(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {Id: "user123", PrimaryEmail: "t@example.com"},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:      resourceIDArg("user", "user123"),
		"user_profile": strArg(`{"custom_schemas":{"EmployeeInfo":{"region":"emea"}}}`),
	}}

	_, _, err := c.updateUserActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, 1, state.patchCount)
	raw, ok := state.lastPatchBody.CustomSchemas["EmployeeInfo"]
	require.True(t, ok)
	require.Contains(t, string(raw), "emea")
}

func TestUpdateUserGlobal_MissingUserProfile(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: resourceIDArg("user", "user123"),
	}}

	_, _, err := c.updateUserActionHandler(context.Background(), args)
	require.Error(t, err)
	require.Equal(t, 0, state.patchCount)
}

func TestUpdateUserGlobal_MissingUserID(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_profile": strArg(`{"family_name":"New"}`),
	}}

	_, _, err := c.updateUserActionHandler(context.Background(), args)
	require.Error(t, err)
	require.Equal(t, 0, state.patchCount)
}
