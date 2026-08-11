package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// testInsertServerState backs a mock Directory API supporting the POST
// (users.insert) endpoint exercised by CreateAccount.
type testInsertServerState struct {
	mtx         sync.Mutex
	insertCount int
	// lastInsertRawBody is the raw JSON of the last insert, kept alongside its
	// decoded form so tests can assert on wire-level details (e.g. that no
	// empty "organizations" array was sent) that decoding into a Go struct
	// would hide.
	lastInsertRawBody []byte
	lastInsertBody    *directoryAdmin.User
}

func newTestInsertServer(state *testInsertServerState) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		state.mtx.Lock()
		defer state.mtx.Unlock()

		raw, _ := io.ReadAll(r.Body)
		body := &directoryAdmin.User{}
		_ = json.Unmarshal(raw, body)
		state.insertCount++
		state.lastInsertRawBody = raw
		state.lastInsertBody = body

		// Echo the created user back the way the real API does, with a
		// server-assigned id.
		if body.Name != nil && body.Name.FullName == "" {
			body.Name.FullName = body.Name.GivenName + " " + body.Name.FamilyName
		}
		_ = json.NewEncoder(w).Encode(safeUserResponse{
			Id:            "newuser123",
			PrimaryEmail:  body.PrimaryEmail,
			Name:          body.Name,
			Organizations: extractOrganizations(body),
			ExternalIDs:   testExternalIDs(body),
			Relations:     extractRelations(body),
		})
	})

	return httptest.NewServer(mux)
}

// createAccountProfile builds an AccountInfo from a profile map, mirroring what
// ConductorOne sends into CreateAccount.
func createAccountProfile(t *testing.T, profile map[string]any) *v2.AccountInfo {
	t.Helper()
	s, err := structpb.NewStruct(profile)
	require.NoError(t, err)
	return &v2.AccountInfo{Profile: s}
}

// baseCreateProfile is the minimum CreateAccount has always required.
func baseCreateProfile() map[string]any {
	return map[string]any{
		"email":       "new.user@example.com",
		"given_name":  "New",
		"family_name": "User",
	}
}

func TestCreateAccount_EmployeeInformation_SentOnInsert(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["department"] = "Engineering"
	profile["job_title"] = "Staff Engineer"
	profile["cost_center"] = "CC-42"
	profile["employee_type"] = "Full-time"
	profile["employee_id"] = "E-1234"
	profile["manager_email"] = "manager@example.com"

	resp, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, state.insertCount)

	orgs := extractOrganizations(state.lastInsertBody)
	require.Len(t, orgs, 1)
	require.True(t, orgs[0].Primary, "the organization created for a brand-new account is the primary one")
	require.Equal(t, "Engineering", orgs[0].Department)
	require.Equal(t, "Staff Engineer", orgs[0].Title)
	require.Equal(t, "CC-42", orgs[0].CostCenter)
	require.Equal(t, "Full-time", orgs[0].Description, "employee_type maps to Organization.Description")

	ids := testExternalIDs(state.lastInsertBody)
	require.Len(t, ids, 1)
	require.Equal(t, externalIDTypeOrganization, ids[0].Type, "employee_id is the 'organization' external ID")
	require.Equal(t, "E-1234", ids[0].Value)

	rels := extractRelations(state.lastInsertBody)
	require.Len(t, rels, 1)
	require.Equal(t, relTypeManager, rels[0].Type)
	require.Equal(t, "manager@example.com", rels[0].Value)

	// The resource CreateAccount returns must already carry the attributes, so
	// the joiner flow sees them without waiting for the next sync.
	successResp, ok := resp.(*v2.CreateAccountResponse_SuccessResult)
	require.True(t, ok)
	require.Equal(t, "newuser123", successResp.Resource.GetId().GetResource())
	returned := successResp.Resource.GetProfile().GetFields()
	require.Equal(t, "Engineering", returned["department"].GetStringValue())
	require.Equal(t, "Staff Engineer", returned[argJobTitle].GetStringValue())
	require.Equal(t, "CC-42", returned["cost_center"].GetStringValue())
	require.Equal(t, "Full-time", returned[argEmployeeType].GetStringValue())
	require.Equal(t, "E-1234", returned[argEmployeeID].GetStringValue())
	require.Equal(t, "manager@example.com", returned[argManagerEmail].GetStringValue())
}

func TestCreateAccount_NoEmployeeInformation_SendsNoEmptyArrays(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, baseCreateProfile()), &v2.LocalCredentialOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, state.insertCount)

	// Organizations/ExternalIds/Relations are interface{}-typed fields Google's
	// marshaller serializes whenever they are non-nil - even when empty - so
	// assert on the raw body, which is where a stray "organizations":[] shows up.
	raw := string(state.lastInsertRawBody)
	require.NotContains(t, raw, "organizations")
	require.NotContains(t, raw, "externalIds")
	require.NotContains(t, raw, "relations")
}

func TestCreateAccount_EmptyEmployeeInformationValues_AreDropped(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Empty strings mean "clear" on the update path; a brand-new account has
	// nothing to clear, so they must not create a phantom empty organization.
	profile := baseCreateProfile()
	profile["department"] = ""
	profile["job_title"] = ""
	profile["employee_id"] = ""
	profile["manager_email"] = ""

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err, "an empty manager_email is simply absent on create, not an invalid address")
	require.Equal(t, 1, state.insertCount)

	raw := string(state.lastInsertRawBody)
	require.NotContains(t, raw, "organizations")
	require.NotContains(t, raw, "externalIds")
	require.NotContains(t, raw, "relations")
}

func TestCreateAccount_PartialEmployeeInformation(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["department"] = "Support"

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err)

	orgs := extractOrganizations(state.lastInsertBody)
	require.Len(t, orgs, 1)
	require.Equal(t, "Support", orgs[0].Department)
	require.Empty(t, orgs[0].Title)

	raw := string(state.lastInsertRawBody)
	require.NotContains(t, raw, "externalIds", "an absent employee_id must not send an external ID entry")
	require.NotContains(t, raw, "relations", "an absent manager_email must not send a relation entry")
}

func TestCreateAccount_JobTitleAliases(t *testing.T) {
	// The account profile and the update action's user_profile object must
	// accept the same key aliases, so a joiner and a later mover agree on where
	// the job title comes from.
	for _, key := range []string{"job_title", "jobTitle", "title"} {
		t.Run(key, func(t *testing.T) {
			state := &testInsertServerState{}
			server := newTestInsertServer(state)
			defer server.Close()

			userRT := newTestUserResourceType(t, server)

			profile := baseCreateProfile()
			profile[key] = "Analyst"

			_, _, _, err := userRT.CreateAccount(context.Background(),
				createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
			require.NoError(t, err)

			orgs := extractOrganizations(state.lastInsertBody)
			require.Len(t, orgs, 1)
			require.Equal(t, "Analyst", orgs[0].Title)
		})
	}
}

func TestCreateAccount_CamelCaseAliases(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["costCenter"] = "CC-7"
	profile["employeeType"] = "Contractor"
	profile["employeeId"] = "E-77"
	profile["managerEmail"] = "lead@example.com"

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err)

	orgs := extractOrganizations(state.lastInsertBody)
	require.Len(t, orgs, 1)
	require.Equal(t, "CC-7", orgs[0].CostCenter)
	require.Equal(t, "Contractor", orgs[0].Description)

	ids := testExternalIDs(state.lastInsertBody)
	require.Len(t, ids, 1)
	require.Equal(t, "E-77", ids[0].Value)

	rels := extractRelations(state.lastInsertBody)
	require.Len(t, rels, 1)
	require.Equal(t, "lead@example.com", rels[0].Value)
}

func TestCreateAccount_InvalidManagerEmail_FailsBeforeInsert(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["manager_email"] = "not-an-email"

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.Error(t, err, "CreateAccount has no skipped_fields channel, so an invalid manager must fail loudly")
	require.Equal(t, 0, state.insertCount, "validation must happen before the account is created")
}

func TestCreateAccount_WrongTypedAttribute_FailsBeforeInsert(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// A numeric employee_id would otherwise be silently dropped; the update
	// path rejects it, and so must this one.
	profile := baseCreateProfile()
	profile["employee_id"] = 12345

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.Error(t, err)
	require.Equal(t, 0, state.insertCount)
}

func TestEmployeeInfoFromProfile_IgnoresOutOfScopeKeys(t *testing.T) {
	// Recovery details and custom schemas stay action-only (out of scope for
	// account provisioning); reading them here would quietly widen what the
	// create path can write.
	patch, err := employeeInfoFromProfile(map[string]any{
		"department":     "Engineering",
		"recovery_email": "recovery@example.com",
		"recovery_phone": "+14155550100",
		"custom_schemas": map[string]any{"MySchema": map[string]any{"region": "emea"}},
		"given_name":     "New",
	})
	require.NoError(t, err)
	require.NotNil(t, patch.department)
	require.Equal(t, "Engineering", *patch.department)
	require.Nil(t, patch.recoveryEmail)
	require.Nil(t, patch.recoveryPhone)
	require.Nil(t, patch.customSchemas)
	require.Nil(t, patch.givenName)
}

func TestApplyEmployeeInfoToNewUser_EmptyPatchLeavesUserUntouched(t *testing.T) {
	user := &directoryAdmin.User{PrimaryEmail: "a@example.com"}
	require.NoError(t, applyEmployeeInfoToNewUser(user, userProfilePatch{}))
	require.Nil(t, user.Organizations)
	require.Nil(t, user.ExternalIds)
	require.Nil(t, user.Relations)
}
