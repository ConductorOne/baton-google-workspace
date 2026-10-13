package connector

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func newAddressTestState(addresses []directoryAdmin.UserAddress) *testProfileServerState {
	u := &directoryAdmin.User{
		Id:           "user123",
		PrimaryEmail: "test@example.com",
		Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
		Organizations: []directoryAdmin.UserOrganization{
			{Primary: true, Department: "Engineering", Title: "Engineer"},
		},
	}
	if addresses != nil {
		u.Addresses = addresses
	}
	return &testProfileServerState{users: map[string]*directoryAdmin.User{"user123": u}}
}

func TestParseAddresses(t *testing.T) {
	t.Run("valid list", func(t *testing.T) {
		addrs, err := parseAddresses([]byte(`[
			{"type":"work","primary":true,"streetAddress":"1 Main St","locality":"London","postalCode":"EC1A 1BB","countryCode":"GB"},
			{"type":"custom","customType":"office","formatted":"Floor 3"}
		]`))
		require.NoError(t, err)
		require.Len(t, addrs, 2)
		require.Equal(t, "work", addrs[0].Type)
		require.True(t, addrs[0].Primary)
		require.Equal(t, "1 Main St", addrs[0].StreetAddress)
		require.Equal(t, "EC1A 1BB", addrs[0].PostalCode)
		require.Equal(t, "office", addrs[1].CustomType)
	})

	t.Run("empty array is a non-nil clear request", func(t *testing.T) {
		addrs, err := parseAddresses([]byte(`[]`))
		require.NoError(t, err)
		require.NotNil(t, addrs)
		require.Empty(t, addrs)
	})

	t.Run("null is nil (not provided)", func(t *testing.T) {
		addrs, err := parseAddresses([]byte(`null`))
		require.NoError(t, err)
		require.Nil(t, addrs)
	})

	for name, raw := range map[string]string{
		"not JSON":               `nope`,
		"object not array":       `{"type":"work"}`,
		"trailing data":          `[] []`,
		"trailing bracket":       `[] ]`,
		"missing type":           `[{"locality":"London"}]`,
		"invalid type":           `[{"type":"office"}]`,
		"custom without type":    `[{"type":"custom"}]`,
		"unknown snake_case key": `[{"type":"work","postal_code":"EC1A 1BB"}]`,
		"two primaries":          `[{"type":"work","primary":true},{"type":"home","primary":true}]`,
		"sourceIsStructured":     `[{"type":"work","sourceIsStructured":true}]`,
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			_, err := parseAddresses([]byte(raw))
			require.Error(t, err)
		})
	}
}

func TestProfileFromJSON_Addresses(t *testing.T) {
	t.Run("native array", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"addresses": []any{map[string]any{"type": "work", "locality": "London"}},
		})
		require.NoError(t, err)
		require.Len(t, patch.addresses, 1)
		require.Equal(t, "London", patch.addresses[0].Locality)
	})

	t.Run("string holding an array", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{
			"addresses": `[{"type":"home","region":"CA"}]`,
		})
		require.NoError(t, err)
		require.Len(t, patch.addresses, 1)
		require.Equal(t, "CA", patch.addresses[0].Region)
	})

	t.Run("null is treated as absent", func(t *testing.T) {
		patch, err := profileFromJSON(map[string]any{"addresses": nil, "department": "Sales"})
		require.NoError(t, err)
		require.Nil(t, patch.addresses)
	})

	t.Run("blank string is treated as absent", func(t *testing.T) {
		for _, raw := range []string{"", "   "} {
			patch, err := profileFromJSON(map[string]any{"addresses": raw, "department": "Sales"})
			require.NoError(t, err)
			require.Nil(t, patch.addresses)
			require.NotNil(t, patch.department)
		}
	})

	t.Run("invalid entry is an error", func(t *testing.T) {
		_, err := profileFromJSON(map[string]any{
			"addresses": []any{map[string]any{"type": "nope"}},
		})
		require.Error(t, err)
	})
}

func TestAddressesDiffer(t *testing.T) {
	base := directoryAdmin.UserAddress{Type: "work", Locality: "London"}

	t.Run("server-populated fields the caller omitted are ignored", func(t *testing.T) {
		current := base
		current.Formatted = "London"
		current.SourceIsStructured = true
		require.False(t, addressesDiffer([]directoryAdmin.UserAddress{current}, []directoryAdmin.UserAddress{base}))
	})

	t.Run("caller-set formatted is compared", func(t *testing.T) {
		current := base
		current.Formatted = "London"
		updated := base
		updated.Formatted = "1 Main St, London"
		require.True(t, addressesDiffer([]directoryAdmin.UserAddress{current}, []directoryAdmin.UserAddress{updated}))
	})

	t.Run("changed structured field is a difference", func(t *testing.T) {
		updated := base
		updated.Locality = "Paris"
		require.True(t, addressesDiffer([]directoryAdmin.UserAddress{base}, []directoryAdmin.UserAddress{updated}))
	})

	t.Run("order matters", func(t *testing.T) {
		home := directoryAdmin.UserAddress{Type: "home", Locality: "Leeds"}
		require.True(t, addressesDiffer(
			[]directoryAdmin.UserAddress{base, home},
			[]directoryAdmin.UserAddress{home, base},
		))
	})
}

func TestUpdateUserProfile_Addresses_SetUsesPatchAndPreservesOtherFields(t *testing.T) {
	state := newAddressTestState(nil)
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"job_title": strArg("Senior Engineer"),
		"addresses": strArg(`[{"type":"work","primary":true,"streetAddress":"1 Main St","locality":"London","countryCode":"GB"}]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, http.MethodPatch, state.lastMethod)
	require.Equal(t, 1, state.getCount, "addresses and job_title must share a single read-modify-write GET")

	addrs, err := extractFromInterface[*directoryAdmin.UserAddress](state.lastPatchBody.Addresses)
	require.NoError(t, err)
	require.Len(t, addrs, 1)
	require.Equal(t, "work", addrs[0].Type)
	require.True(t, addrs[0].Primary)
	require.Equal(t, "1 Main St", addrs[0].StreetAddress)
	require.Equal(t, "London", addrs[0].Locality)

	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	require.NoError(t, err)
	require.Len(t, orgs, 1)
	require.Equal(t, "Senior Engineer", orgs[0].Title)
	require.Equal(t, "Engineering", orgs[0].Department, "sibling org field must be preserved")

	require.Len(t, testAddresses(state.users["user123"]), 1, "mock server should have stored the address")
}

func TestUpdateUserProfile_Addresses_ShrinkUsesPut(t *testing.T) {
	state := newAddressTestState([]directoryAdmin.UserAddress{
		{Type: "work", Locality: "London"},
		{Type: "home", Locality: "Brighton"},
	})
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"addresses": strArg(`[{"type":"work","locality":"Manchester"}]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, http.MethodPut, state.lastMethod, "shrinking a repeated field must use Update (PUT)")

	// PUT sends the full current object, so unrelated fields ride along intact.
	require.NotNil(t, state.lastPatchBody.Name)
	require.Equal(t, "Test", state.lastPatchBody.Name.GivenName)
	addrs := testAddresses(state.users["user123"])
	require.Len(t, addrs, 1)
	require.Equal(t, "Manchester", addrs[0].Locality)
}

func TestUpdateUserProfile_Addresses_EmptyArrayClears(t *testing.T) {
	state := newAddressTestState([]directoryAdmin.UserAddress{{Type: "work", Locality: "London"}})
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"addresses": strArg(`[]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, http.MethodPut, state.lastMethod)
	require.Empty(t, testAddresses(state.users["user123"]), "clearing must actually remove the stored address")
}

func TestUpdateUserProfile_Addresses_IdempotentResend_SucceedsAsNoOp(t *testing.T) {
	state := newAddressTestState([]directoryAdmin.UserAddress{{Type: "work", Locality: "London", Primary: true}})
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"addresses": strArg(`[{"type":"work","locality":"London","primary":true}]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, 0, state.patchCount, "an unchanged address list must not issue a write")
}

func TestUpdateUserProfile_Addresses_IdempotentResendWithServerPopulatedFields_SucceedsAsNoOp(t *testing.T) {
	state := newAddressTestState([]directoryAdmin.UserAddress{
		{Type: "work", Locality: "London", Formatted: "London", SourceIsStructured: true},
	})
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"addresses": strArg(`[{"type":"work","locality":"London"}]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.Equal(t, 0, state.patchCount, "fields Google populates on read must not make a re-send look changed")
}

func TestUpdateUserGlobal_BlankAddressesDoesNotFailOtherFields(t *testing.T) {
	state := newAddressTestState([]directoryAdmin.UserAddress{{Type: "work", Locality: "London"}})
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:      resourceIDArg("user", "user123"),
		"user_profile": strArg(`{"job_title":"AE","addresses":""}`),
	}}

	resp, _, err := c.updateUserActionHandler(context.Background(), args)
	require.NoError(t, err, "a blank addresses value must not fail the rest of the update")
	require.True(t, resp.GetFields()["success"].GetBoolValue())
	require.NotContains(t, resp.GetFields()["updated_fields"].GetStringValue(), "Addresses")
	require.Len(t, testAddresses(state.users["user123"]), 1, "a blank addresses value must leave existing addresses untouched")
}

func TestUpdateUserProfile_Addresses_ClearWhenAlreadyEmpty_SucceedsAsNoOp(t *testing.T) {
	state := newAddressTestState(nil)
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"addresses": strArg(`[]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.NoError(t, err, "a satisfied clear must not be rejected as 'nothing provided'")
	require.Equal(t, 0, state.patchCount)
}

func TestUpdateUserProfile_Addresses_InvalidRejectedWithoutAPICall(t *testing.T) {
	state := newAddressTestState(nil)
	server := newTestProfileServer(state)
	defer server.Close()
	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:   strArg("user123"),
		"addresses": strArg(`[{"type":"work","postal_code":"EC1A 1BB"}]`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid addresses")
	require.Equal(t, 0, state.getCount)
	require.Equal(t, 0, state.patchCount)
}

func TestUpdateUserGlobal_AddressesViaProfile(t *testing.T) {
	state := newAddressTestState(nil)
	server := newTestProfileServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestGlobalConnector(t, dir)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: resourceIDArg("user", "user123"),
		"user_profile": strArg(`{"department":"Sales","job_title":"AE",` +
			`"addresses":[{"type":"work","primary":true,"locality":"London","countryCode":"GB"}]}`),
	}}

	resp, _, err := c.updateUserActionHandler(context.Background(), args)
	require.NoError(t, err)
	require.True(t, resp.GetFields()["success"].GetBoolValue())
	require.Contains(t, resp.GetFields()["updated_fields"].GetStringValue(), "Addresses")
	require.Contains(t, resp.GetFields()["updated_fields"].GetStringValue(), "Organizations")
	require.Equal(t, 1, state.patchCount)

	addrs, err := extractFromInterface[*directoryAdmin.UserAddress](state.lastPatchBody.Addresses)
	require.NoError(t, err)
	require.Len(t, addrs, 1)
	require.Equal(t, "GB", addrs[0].CountryCode)

	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	require.NoError(t, err)
	require.Len(t, orgs, 1)
	require.Equal(t, "Sales", orgs[0].Department)
	require.Equal(t, "AE", orgs[0].Title)
}
