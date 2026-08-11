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

// TestCreateAccount_InvalidManagerEmail_StillCreatesAccount pins that an
// unusable manager does not cost the joiner their account. A manager who has not
// been provisioned yet, or a display name where an address was expected, is a
// routine state for an HRIS-sourced profile; the account is created without the
// relation and update_user fills it in later.
func TestCreateAccount_InvalidManagerEmail_StillCreatesAccount(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["department"] = "Engineering"
	profile["manager_email"] = "not-an-email"

	resp, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err, "an unusable manager_email must not block account creation")
	require.NotNil(t, resp)
	require.Equal(t, 1, state.insertCount)

	require.Nil(t, extractRelations(state.lastInsertBody), "the unusable manager relation is dropped")

	// The valid attributes in the same profile still land.
	orgs := extractOrganizations(state.lastInsertBody)
	require.Len(t, orgs, 1)
	require.Equal(t, "Engineering", orgs[0].Department)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(state.lastInsertRawBody, &raw))
	require.NotContains(t, raw, "relations")
}

// TestCreateAccount_WrongTypedAttribute_StillCreatesAccount covers the numeric
// employee_id / cost_center an HRIS routinely sends. These attributes enrich an
// account rather than define it, so a type mismatch drops the field instead of
// failing the insert. The update path stays strict - there the account already
// exists, so failing loudly costs nothing.
func TestCreateAccount_WrongTypedAttribute_StillCreatesAccount(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["employee_id"] = 12345
	profile["cost_center"] = 4200
	profile["department"] = "Engineering"

	resp, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err, "a wrong-typed enrichment attribute must not block account creation")
	require.NotNil(t, resp)
	require.Equal(t, 1, state.insertCount)

	// The wrong-typed fields are dropped; the well-formed one in the same
	// profile still applies.
	orgs := extractOrganizations(state.lastInsertBody)
	require.Len(t, orgs, 1)
	require.Equal(t, "Engineering", orgs[0].Department)
	require.Empty(t, orgs[0].CostCenter)
	require.Nil(t, testExternalIDs(state.lastInsertBody))
}

// TestEmployeeInfoFromProfile_WrongTypedValuesAreReported pins the descriptions
// CreateAccount logs, so an operator can tell which attribute was dropped and
// why rather than discovering a blank field on the next sync.
func TestEmployeeInfoFromProfile_WrongTypedValuesAreReported(t *testing.T) {
	patch, dropped := employeeInfoFromProfile(map[string]any{
		"department":  "Engineering",
		"employee_id": float64(12345),
		"cost_center": true,
	})
	require.NotNil(t, patch.department)
	require.Equal(t, "Engineering", *patch.department)
	require.Nil(t, patch.employeeID)
	require.Nil(t, patch.costCenter)
	require.ElementsMatch(t, []string{
		"cost_center (expected a JSON string, got a boolean)",
		"employee_id (expected a JSON string, got a number)",
	}, dropped)
}

// TestEmployeeInfoFromProfile_WrongTypedAliasDoesNotMaskValidOne covers the
// skip-and-continue behavior across aliases: job_title and title are the same
// attribute, so a wrong-typed job_title must not discard a usable title.
func TestEmployeeInfoFromProfile_WrongTypedAliasDoesNotMaskValidOne(t *testing.T) {
	patch, dropped := employeeInfoFromProfile(map[string]any{
		"job_title": float64(7),
		"title":     "Staff Engineer",
	})
	require.NotNil(t, patch.jobTitle)
	require.Equal(t, "Staff Engineer", *patch.jobTitle)
	require.Empty(t, dropped, "nothing to report once an alias resolves the attribute")
}

func TestEmployeeInfoFromProfile_IgnoresOutOfScopeKeys(t *testing.T) {
	// Recovery details and custom schemas stay action-only (out of scope for
	// account provisioning); reading them here would quietly widen what the
	// create path can write.
	patch, dropped := employeeInfoFromProfile(map[string]any{
		"department":     "Engineering",
		"recovery_email": "recovery@example.com",
		"recovery_phone": "+14155550100",
		"custom_schemas": map[string]any{"MySchema": map[string]any{"region": "emea"}},
		"given_name":     "New",
	})
	require.Empty(t, dropped)
	require.NotNil(t, patch.department)
	require.Equal(t, "Engineering", *patch.department)
	require.Nil(t, patch.recoveryEmail)
	require.Nil(t, patch.recoveryPhone)
	require.Nil(t, patch.customSchemas)
	require.Nil(t, patch.givenName)
}

func TestApplyEmployeeInfoToNewUser_EmptyPatchLeavesUserUntouched(t *testing.T) {
	user := &directoryAdmin.User{PrimaryEmail: "a@example.com"}
	require.Empty(t, applyEmployeeInfoToNewUser(user, userProfilePatch{}))
	require.Nil(t, user.Organizations)
	require.Nil(t, user.ExternalIds)
	require.Nil(t, user.Relations)
}

// TestCreateAccount_WhitespaceOnlyValues_AreDropped covers the same "empty means
// absent" rule as the empty-string case, for the padding HRIS- and CSV-sourced
// account profiles carry in practice. A whitespace-only manager_email is the one
// that matters most: before it was treated as empty it reached mail.ParseAddress
// and failed the entire create, so a profile carrying no manager at all produced
// no account.
func TestCreateAccount_WhitespaceOnlyValues_AreDropped(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["department"] = "   "
	profile["job_title"] = "\t"
	profile["cost_center"] = " "
	profile["employee_type"] = "  "
	profile["employee_id"] = " \n "
	profile["manager_email"] = "   "

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err, "a whitespace-only value must not fail account creation")
	require.Equal(t, 1, state.insertCount)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(state.lastInsertRawBody, &raw))
	require.NotContains(t, raw, "organizations")
	require.NotContains(t, raw, "externalIds")
	require.NotContains(t, raw, "relations")
}

// TestCreateAccount_ManagerEmailDisplayNameForm_StoresBareAddress pins that the
// manager relation carries the bare address. mail.ParseAddress accepts the
// display-name form, but Google resolves relations[].value only as an email, so
// storing the raw input would silently produce a manager relation matching no
// user.
func TestCreateAccount_ManagerEmailDisplayNameForm_StoresBareAddress(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["manager_email"] = "Jane Doe <jane@example.com>"

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err)

	rels := extractRelations(state.lastInsertBody)
	require.Len(t, rels, 1)
	require.Equal(t, relTypeManager, rels[0].Type)
	require.Equal(t, "jane@example.com", rels[0].Value,
		"the display name must be stripped; Google matches relations[].value as a bare email")
}

// TestCreateAccount_ManagerEmailSurroundingWhitespace_IsTrimmed guards the same
// normalization for the far more common case of an otherwise-valid address that
// arrives padded.
func TestCreateAccount_ManagerEmailSurroundingWhitespace_IsTrimmed(t *testing.T) {
	state := &testInsertServerState{}
	server := newTestInsertServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	profile := baseCreateProfile()
	profile["manager_email"] = "  manager@example.com  "

	_, _, _, err := userRT.CreateAccount(context.Background(),
		createAccountProfile(t, profile), &v2.LocalCredentialOptions{})
	require.NoError(t, err)

	rels := extractRelations(state.lastInsertBody)
	require.Len(t, rels, 1)
	require.Equal(t, "manager@example.com", rels[0].Value)
}
