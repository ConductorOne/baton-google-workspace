package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/structpb"
)

// testProfileServerState backs a mock Directory API that supports the PATCH
// (users.patch) and POST (users.makeAdmin) endpoints exercised by the
// update_user_profile and make_admin actions.
type testProfileServerState struct {
	mtx          sync.Mutex
	users        map[string]*directoryAdmin.User
	getCount     int
	patchCount   int
	makeAdminCnt int
	// lastMethod records the HTTP verb (PATCH or PUT) of the last write
	// request, so tests can assert which one applyUserProfilePatch actually
	// chose instead of only observing verb-blind side effects.
	lastMethod string
	// lastPatchRawBody is the raw JSON bytes of the last PATCH request, kept
	// alongside lastPatchBody (its decoded form) to assert on wire-level
	// details (e.g. an explicitly forced empty string) that decoding back into
	// a Go struct would otherwise hide.
	lastPatchRawBody []byte
	lastPatchBody    *directoryAdmin.User
	lastAdminBody    *directoryAdmin.UserMakeAdmin
}

// testExternalIDs extracts a typed ExternalIds slice from a user's interface{}
// field for the mock server's JSON responses; parse errors are treated as empty.
func testExternalIDs(u *directoryAdmin.User) []*directoryAdmin.UserExternalId {
	ids, _ := extractFromInterface[*directoryAdmin.UserExternalId](u.ExternalIds)
	return ids
}

func newTestProfileServer(state *testProfileServerState) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		parts := strings.Split(path, "/")
		userKey := parts[0]

		state.mtx.Lock()
		defer state.mtx.Unlock()

		// POST /users/{userKey}/makeAdmin
		if len(parts) == 2 && parts[1] == "makeAdmin" && r.Method == http.MethodPost {
			body := &directoryAdmin.UserMakeAdmin{}
			_ = json.NewDecoder(r.Body).Decode(body)
			if _, ok := state.users[userKey]; !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			state.makeAdminCnt++
			state.lastAdminBody = body
			w.WriteHeader(http.StatusOK)
			return
		}

		u, ok := state.users[userKey]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		switch r.Method {
		case http.MethodGet:
			state.getCount++
			_ = json.NewEncoder(w).Encode(safeUserResponse{
				Id:            u.Id,
				PrimaryEmail:  u.PrimaryEmail,
				Name:          u.Name,
				RecoveryEmail: u.RecoveryEmail,
				CustomSchemas: u.CustomSchemas,
				Organizations: extractOrganizations(u),
				ExternalIDs:   testExternalIDs(u),
				Relations:     extractRelations(u),
			})
		case http.MethodPatch, http.MethodPut:
			state.patchCount++
			state.lastMethod = r.Method
			raw, _ := io.ReadAll(r.Body)
			state.lastPatchRawBody = raw
			body := &directoryAdmin.User{}
			_ = json.Unmarshal(raw, body)
			state.lastPatchBody = body
			// Apply a minimal merge so the echoed resource reflects the change.
			if body.Name != nil {
				u.Name = body.Name
			}
			if body.RecoveryEmail != "" {
				u.RecoveryEmail = body.RecoveryEmail
			}
			if body.CustomSchemas != nil {
				u.CustomSchemas = body.CustomSchemas
			}
			if body.Organizations != nil {
				u.Organizations = body.Organizations
			}
			// Users.Patch does not reliably shrink ExternalIds down to empty
			// (confirmed against the real Directory API - see applyUserProfilePatch);
			// Users.Update (PUT) does. Model that quirk here so a regression back
			// to Patch for the clearing path fails TestUpdateUserProfile_EmployeeID_EmptyClears
			// instead of silently passing against a mock more lenient than the real API.
			if body.ExternalIds != nil {
				emptyArray := false
				if arr, ok := body.ExternalIds.([]interface{}); ok && len(arr) == 0 {
					emptyArray = true
				}
				if !emptyArray || r.Method == http.MethodPut {
					u.ExternalIds = body.ExternalIds
				}
			}
			if body.Relations != nil {
				u.Relations = body.Relations
			}
			_ = json.NewEncoder(w).Encode(safeUserResponse{
				Id:            u.Id,
				PrimaryEmail:  u.PrimaryEmail,
				Name:          u.Name,
				RecoveryEmail: u.RecoveryEmail,
				CustomSchemas: u.CustomSchemas,
				Organizations: extractOrganizations(u),
				ExternalIDs:   testExternalIDs(u),
				Relations:     extractRelations(u),
			})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func strArg(v string) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v}}
}

func boolArg(v bool) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: v}}
}

func TestUpdateUserProfile_NameFields_MergesAndPatches(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Old", FamilyName: "Name"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:    strArg("user123"),
		"given_name": strArg("New"),
	}}

	resp, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true")
	}
	if state.patchCount != 1 {
		t.Fatalf("expected 1 PATCH, got %d", state.patchCount)
	}
	if state.lastMethod != http.MethodPatch {
		t.Fatalf("expected a single-name-field update to use PATCH, not a full-object PUT, got %s", state.lastMethod)
	}
	if state.getCount != 1 {
		t.Fatalf("expected 1 GET for read-modify-write of name, got %d", state.getCount)
	}
	// given_name updated, family_name preserved from the read.
	if got := state.lastPatchBody.Name.GivenName; got != "New" {
		t.Fatalf("expected GivenName 'New', got %q", got)
	}
	if got := state.lastPatchBody.Name.FamilyName; got != "Name" {
		t.Fatalf("expected FamilyName preserved as 'Name', got %q", got)
	}
}

func TestUpdateUserProfile_EmptyNameValue_IsIgnored(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Old", FamilyName: "Name"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Empty given_name must NOT blank the name; the non-empty family_name still applies.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:     strArg("user123"),
		"given_name":  strArg(""),
		"family_name": strArg("Changed"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.patchCount != 1 {
		t.Fatalf("expected 1 PATCH, got %d", state.patchCount)
	}
	if got := state.lastPatchBody.Name.GivenName; got != "Old" {
		t.Fatalf("expected GivenName preserved as 'Old', got %q", got)
	}
	if got := state.lastPatchBody.Name.FamilyName; got != "Changed" {
		t.Fatalf("expected FamilyName 'Changed', got %q", got)
	}
}

func TestUpdateUserProfile_OnlyEmptyName_NoUpdatableField(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Only an empty name value -> nothing to update -> validation error, no PATCH.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:    strArg("user123"),
		"given_name": strArg(""),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err == nil {
		t.Fatalf("expected error when the only provided field is an empty name")
	}
	if state.patchCount != 0 {
		t.Fatalf("expected 0 PATCH, got %d", state.patchCount)
	}
}

func TestUpdateUserProfile_CustomSchemas_SentVerbatim(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			// The real Directory API echoes back the full user (incl. Name) on patch.
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:        strArg("user123"),
		"custom_schemas": strArg(`{"EmployeeInfo":{"region":"emea"}}`),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.patchCount != 1 {
		t.Fatalf("expected 1 PATCH, got %d", state.patchCount)
	}
	// No name fields -> no read-modify-write GET.
	if state.getCount != 0 {
		t.Fatalf("expected 0 GET, got %d", state.getCount)
	}
	raw, ok := state.lastPatchBody.CustomSchemas["EmployeeInfo"]
	if !ok {
		t.Fatalf("expected EmployeeInfo schema in patch body, got %+v", state.lastPatchBody.CustomSchemas)
	}
	if !strings.Contains(string(raw), "emea") {
		t.Fatalf("expected region 'emea' in schema, got %s", string(raw))
	}
}

// TestUpdateUserProfile_DepartmentOnly_UsesPatchAndOmitsCustomSchemas locks in
// that department (like the other Employee Information fields, name, and
// manager_email) uses the narrow Patch path, not a full-object Update: a user
// with pre-existing CustomSchemas, patched by touching only department, must
// not report "CustomSchemas" as changed, and - since Organizations/ExternalIds
// never shrink here, so Patch (not Update) is the write - the wire body must
// not carry CustomSchemas at all (Patch is sparse; the server leaves anything
// not present in the request untouched, so there is nothing to inherit from
// `current` client-side).
func TestUpdateUserProfile_DepartmentOnly_UsesPatchAndOmitsCustomSchemas(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				CustomSchemas: map[string]googleapi.RawMessage{
					"EmployeeInfo": googleapi.RawMessage(`{"region":"emea"}`),
				},
				Organizations: []directoryAdmin.UserOrganization{
					{Primary: true, Department: "Old Dept"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	patch := userProfilePatch{department: strPtr("New Dept")}
	_, updatedFields, err := applyUserProfilePatch(context.Background(), userRT.client, "user123", patch)
	if err != nil {
		t.Fatalf("applyUserProfilePatch: %v", err)
	}
	// Organizations requires a GET first regardless of which write verb is used.
	if state.getCount != 1 {
		t.Fatalf("expected 1 GET, got %d", state.getCount)
	}
	if state.lastMethod != http.MethodPatch {
		t.Fatalf("expected department-only update to use PATCH (Organizations never shrinks), got %s", state.lastMethod)
	}
	for _, f := range updatedFields {
		if f == "CustomSchemas" {
			t.Fatalf("CustomSchemas falsely reported as changed: updatedFields=%v", updatedFields)
		}
	}
	if state.lastPatchBody.CustomSchemas != nil {
		t.Fatalf("expected CustomSchemas to be absent from a sparse Patch body, got %+v", state.lastPatchBody.CustomSchemas)
	}
}

// TestUpdateUserProfile_CustomSchemas_SendsVerbatimNotMerged documents (and
// guards) a deliberate design decision raised in review: alongside another
// field that requires a GET (here department, via the Organizations
// read-modify-write), when the caller also sets custom_schemas, the code
// sends patch.customSchemas as-is rather than merging it into the inherited
// current.CustomSchemas map, unlike the Organizations/ExternalIds/Relations
// read-modify-write above it. This is safe because - confirmed against a live
// tenant - Google merges custom schema fields server-side even over a
// full-object Update (a sibling field on the same schema survived when
// omitted from the request), unlike the repeated/array fields this file
// otherwise has to read-modify-write around; the same holds a fortiori for
// the narrower Patch this scenario now actually uses (see the usePut
// narrowing above - department alone never triggers Update). If server-side
// merging ever stops being true, a local merge would need to be added here;
// this test at least locks in that the code currently sends the patch
// verbatim rather than pre-emptively (and redundantly) merging it
// client-side.
func TestUpdateUserProfile_CustomSchemas_SendsVerbatimNotMerged(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				CustomSchemas: map[string]googleapi.RawMessage{
					"QATestSchema": googleapi.RawMessage(`{"region":"emea","costCenter":"CC99"}`),
				},
				Organizations: []directoryAdmin.UserOrganization{
					{Primary: true, Department: "Old Dept"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	patch := userProfilePatch{
		department:    strPtr("New Dept"),
		customSchemas: map[string]googleapi.RawMessage{"QATestSchema": googleapi.RawMessage(`{"region":"apac"}`)},
	}
	_, _, err := applyUserProfilePatch(context.Background(), userRT.client, "user123", patch)
	if err != nil {
		t.Fatalf("applyUserProfilePatch: %v", err)
	}
	if state.getCount != 1 {
		t.Fatalf("expected 1 GET (Organizations read-modify-write), got %d", state.getCount)
	}
	if state.lastMethod != http.MethodPatch {
		t.Fatalf("expected department+custom_schemas update to use PATCH, got %s", state.lastMethod)
	}
	if raw, ok := state.lastPatchBody.CustomSchemas["QATestSchema"]; !ok || !strings.Contains(string(raw), "apac") {
		t.Fatalf("expected QATestSchema with region=apac on the wire, got %+v", state.lastPatchBody.CustomSchemas)
	}
}

func TestUpdateUserProfile_NoUpdatableFields(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: strArg("user123"),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when no updatable field is provided")
	}
	if state.patchCount != 0 {
		t.Fatalf("expected 0 PATCH on validation failure, got %d", state.patchCount)
	}
}

func TestUpdateUserProfile_InvalidCustomSchemasJSON(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:        strArg("user123"),
		"custom_schemas": strArg("{not valid json"),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error on invalid custom_schemas JSON")
	}
	if state.patchCount != 0 {
		t.Fatalf("expected 0 PATCH on invalid JSON, got %d", state.patchCount)
	}
}

func TestUpdateUserProfile_InvalidRecoveryEmail(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:        strArg("user123"),
		"recovery_email": strArg("not-an-email"),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error on malformed recovery_email")
	}
	if state.patchCount != 0 {
		t.Fatalf("expected 0 PATCH on invalid recovery_email, got %d", state.patchCount)
	}
}

func TestUpdateUserProfile_EmptyRecoveryEmail_Clears(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:            "user123",
				PrimaryEmail:  "test@example.com",
				RecoveryEmail: "old@example.com",
				Name:          &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Empty string is a legitimate "clear" request and must still patch.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:        strArg("user123"),
		"recovery_email": strArg(""),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.patchCount != 1 {
		t.Fatalf("expected 1 PATCH for recovery_email clear, got %d", state.patchCount)
	}
}

func TestUpdateUserProfile_MissingUserId(t *testing.T) {
	state := &testProfileServerState{users: map[string]*directoryAdmin.User{}}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"given_name": strArg("New"),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when user_id is missing")
	}
}

func TestUpdateUserProfile_Department_PreservesSiblingOrgFields(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				Organizations: []directoryAdmin.UserOrganization{
					{Primary: true, Department: "Old Dept", Title: "Old Title", CostCenter: "CC1"},
					{Primary: false, Department: "Secondary Dept"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:    strArg("user123"),
		"department": strArg("New Dept"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.getCount != 1 {
		t.Fatalf("expected 1 GET for read-modify-write of organizations, got %d", state.getCount)
	}
	if state.patchCount != 1 {
		t.Fatalf("expected 1 PATCH, got %d", state.patchCount)
	}

	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	if err != nil || len(orgs) != 2 {
		t.Fatalf("expected 2 organizations in patch body, got %+v (err=%v)", state.lastPatchBody.Organizations, err)
	}
	var primary, secondary *directoryAdmin.UserOrganization
	for _, o := range orgs {
		if o.Primary {
			primary = o
		} else {
			secondary = o
		}
	}
	if primary == nil {
		t.Fatalf("expected a primary organization in patch body")
	}
	if primary.Department != "New Dept" {
		t.Fatalf("expected Department 'New Dept', got %q", primary.Department)
	}
	if primary.Title != "Old Title" {
		t.Fatalf("expected sibling Title preserved as 'Old Title', got %q", primary.Title)
	}
	if primary.CostCenter != "CC1" {
		t.Fatalf("expected sibling CostCenter preserved as 'CC1', got %q", primary.CostCenter)
	}
	if secondary == nil || secondary.Department != "Secondary Dept" {
		t.Fatalf("expected secondary organization preserved, got %+v", secondary)
	}
}

func TestUpdateUserProfile_Department_ClearedToEmpty_OnWire(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				Organizations: []directoryAdmin.UserOrganization{
					{Primary: true, Department: "Old Dept", Title: "Old Title"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:    strArg("user123"),
		"department": strArg(""),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}

	// UserOrganization has its own ForceSendFields separate from the top-level
	// User.ForceSendFields; without it Google's MarshalJSON omits an empty
	// Department from the wire body (standard omitempty), silently failing to
	// clear it. Assert on the raw JSON, since decoding back into a Go struct
	// cannot distinguish "sent as empty" from "omitted".
	if !strings.Contains(string(state.lastPatchRawBody), `"department":""`) {
		t.Fatalf("expected wire body to explicitly send department=\"\", got %s", state.lastPatchRawBody)
	}
	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	if err != nil || len(orgs) != 1 {
		t.Fatalf("expected 1 organization in patch body, got %+v (err=%v)", state.lastPatchBody.Organizations, err)
	}
	if orgs[0].Department != "" {
		t.Fatalf("expected Department cleared to empty, got %q", orgs[0].Department)
	}
	if orgs[0].Title != "Old Title" {
		t.Fatalf("expected sibling Title preserved as 'Old Title', got %q", orgs[0].Title)
	}
}

func TestUpdateUserProfile_Department_NoPrimaryFlagged_UpdatesExistingOrgWithoutPromotingPrimary(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				// No organization is flagged Primary - this happens for
				// accounts provisioned via GCDS or third-party sync. The read
				// path (extractPrimaryOrganizations) falls back to orgs[0];
				// the write path must edit that same entry in place (not
				// append a second one), but must not silently persist a
				// Primary flag Google never set as a side effect of an
				// unrelated field edit.
				Organizations: []directoryAdmin.UserOrganization{
					{Department: "Sales", Title: "Rep", CostCenter: "CC9"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:    strArg("user123"),
		"department": strArg("Engineering"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}

	if state.lastMethod != http.MethodPatch {
		t.Fatalf("expected department-only update to use PATCH, got %s", state.lastMethod)
	}

	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	if err != nil || len(orgs) != 1 {
		t.Fatalf("expected the single existing organization to be updated in place, got %+v (err=%v)", state.lastPatchBody.Organizations, err)
	}
	if orgs[0].Primary {
		t.Fatalf("expected the existing organization's Primary flag to remain false, not be silently promoted")
	}
	if orgs[0].Department != "Engineering" {
		t.Fatalf("expected Department 'Engineering', got %q", orgs[0].Department)
	}
	if orgs[0].Title != "Rep" {
		t.Fatalf("expected sibling Title preserved as 'Rep', got %q", orgs[0].Title)
	}
	if orgs[0].CostCenter != "CC9" {
		t.Fatalf("expected sibling CostCenter preserved as 'CC9', got %q", orgs[0].CostCenter)
	}
}

func TestUpdateUserProfile_EmployeeType_MapsToOrgDescription(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:       strArg("user123"),
		"employee_type": strArg("Contractor"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}

	orgs, err := extractFromInterface[*directoryAdmin.UserOrganization](state.lastPatchBody.Organizations)
	if err != nil || len(orgs) != 1 {
		t.Fatalf("expected 1 organization in patch body, got %+v (err=%v)", state.lastPatchBody.Organizations, err)
	}
	if !orgs[0].Primary {
		t.Fatalf("expected the created organization to be primary")
	}
	if orgs[0].Description != "Contractor" {
		t.Fatalf("expected Description 'Contractor', got %q", orgs[0].Description)
	}
}

func TestUpdateUserProfile_EmployeeID_PreservesOtherExternalIds(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				ExternalIds: []directoryAdmin.UserExternalId{
					{Type: "organization", Value: "E-OLD"},
					{Type: "login_id", Value: "alogin"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:     strArg("user123"),
		"employee_id": strArg("E-NEW"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.getCount != 1 {
		t.Fatalf("expected 1 GET for read-modify-write of external ids, got %d", state.getCount)
	}

	extIDs, err := extractFromInterface[*directoryAdmin.UserExternalId](state.lastPatchBody.ExternalIds)
	if err != nil || len(extIDs) != 2 {
		t.Fatalf("expected 2 external ids in patch body, got %+v (err=%v)", state.lastPatchBody.ExternalIds, err)
	}
	var org, login *directoryAdmin.UserExternalId
	for _, id := range extIDs {
		switch id.Type {
		case "organization":
			org = id
		case "login_id":
			login = id
		}
	}
	if org == nil || org.Value != "E-NEW" {
		t.Fatalf("expected organization external id updated to 'E-NEW', got %+v", org)
	}
	if login == nil || login.Value != "alogin" {
		t.Fatalf("expected login_id external id preserved as 'alogin', got %+v", login)
	}
}

func TestUpdateUserProfile_EmployeeID_EmptyClears(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				ExternalIds: []directoryAdmin.UserExternalId{
					{Type: "organization", Value: "E-OLD"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:     strArg("user123"),
		"employee_id": strArg(""),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}

	// Clearing the only ExternalIds entry down to empty is the one case that
	// genuinely needs Update (PUT): Patch does not reliably shrink a repeated
	// field, confirmed against a live tenant.
	if state.lastMethod != http.MethodPut {
		t.Fatalf("expected employee_id clear-to-empty to use PUT, got %s", state.lastMethod)
	}

	// ExternalIds must be sent as a non-nil (possibly empty) slice: a nil value
	// would be omitted from the request entirely, leaving the stale entry in
	// place instead of clearing it.
	if state.lastPatchBody.ExternalIds == nil {
		t.Fatalf("expected ExternalIds to be sent (non-nil) so the stale entry is actually cleared, got nil")
	}
	extIDs, err := extractFromInterface[*directoryAdmin.UserExternalId](state.lastPatchBody.ExternalIds)
	if err != nil {
		t.Fatalf("failed to parse external ids: %v", err)
	}
	if len(extIDs) != 0 {
		t.Fatalf("expected 0 external ids after clearing the only entry, got %+v", extIDs)
	}
}

func TestUpdateUserProfile_ManagerEmail_PreservesOtherRelations(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
				Relations: []directoryAdmin.UserRelation{
					{Type: "manager", Value: "old-manager@example.com"},
					{Type: "assistant", Value: "assistant@example.com"},
				},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:       strArg("user123"),
		"manager_email": strArg("new-manager@example.com"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.getCount != 1 {
		t.Fatalf("expected 1 GET for read-modify-write of relations, got %d", state.getCount)
	}
	if state.lastMethod != http.MethodPatch {
		t.Fatalf("expected manager_email-only update to use PATCH (Relations never shrinks), got %s", state.lastMethod)
	}

	rels, err := extractFromInterface[*directoryAdmin.UserRelation](state.lastPatchBody.Relations)
	if err != nil || len(rels) != 2 {
		t.Fatalf("expected 2 relations in patch body, got %+v (err=%v)", state.lastPatchBody.Relations, err)
	}
	var manager, assistant *directoryAdmin.UserRelation
	for _, r := range rels {
		switch r.Type {
		case "manager":
			manager = r
		case "assistant":
			assistant = r
		}
	}
	if manager == nil || manager.Value != "new-manager@example.com" {
		t.Fatalf("expected manager relation updated to 'new-manager@example.com', got %+v", manager)
	}
	if assistant == nil || assistant.Value != "assistant@example.com" {
		t.Fatalf("expected assistant relation preserved, got %+v", assistant)
	}
}

func TestUpdateUserProfile_ManagerEmail_EmptyIsNotProvided(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// An empty manager_email is treated as "not provided" (matching the name
	// fields), not an error by itself; with no other field set, the request
	// fails the generic "at least one updatable field" guard instead - and
	// crucially does not perform the read-modify-write GET for Relations.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:       strArg("user123"),
		"manager_email": strArg(""),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when manager_email is the only (empty) field provided")
	}
	if state.getCount != 0 {
		t.Fatalf("expected 0 GET for an empty manager_email, got %d", state.getCount)
	}
	if state.patchCount != 0 {
		t.Fatalf("expected 0 PATCH on empty manager_email, got %d", state.patchCount)
	}
}

func TestUpdateUserProfile_ManagerEmail_Empty_OtherFieldStillApplies(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				Name:         &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
			},
		},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// An empty manager_email alongside a real field must not abort the whole
	// patch - it is simply ignored, like an empty given_name/family_name.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:        strArg("user123"),
		"manager_email":  strArg(""),
		"recovery_email": strArg("new@example.com"),
	}}

	if _, _, err := userRT.updateUserProfileActionHandler(context.Background(), args); err != nil {
		t.Fatalf("updateUserProfile: %v", err)
	}
	if state.patchCount != 1 {
		t.Fatalf("expected 1 PATCH, got %d", state.patchCount)
	}
	if state.lastPatchBody.Relations != nil {
		t.Fatalf("expected Relations untouched when manager_email is empty, got %+v", state.lastPatchBody.Relations)
	}
}

func TestUpdateUserProfile_ManagerEmail_InvalidEmailRejected(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID:       strArg("user123"),
		"manager_email": strArg("not-an-email"),
	}}

	_, _, err := userRT.updateUserProfileActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid manager_email")
	}
	// Validation happens before the read-modify-write GET, so a malformed
	// address must fail fast without spending an API call.
	if state.getCount != 0 {
		t.Fatalf("expected 0 GET on invalid manager_email (fail fast before RMW read), got %d", state.getCount)
	}
	if state.patchCount != 0 {
		t.Fatalf("expected 0 PATCH on invalid manager_email, got %d", state.patchCount)
	}
}

func TestMakeAdmin_GrantAndRevoke(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Grant super-admin.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: strArg("user123"),
		"status":  boolArg(true),
	}}
	resp, _, err := userRT.makeAdminActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("makeAdmin grant: %v", err)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true")
	}
	if state.makeAdminCnt != 1 || state.lastAdminBody == nil || !state.lastAdminBody.Status {
		t.Fatalf("expected makeAdmin called with status=true, got count=%d body=%+v", state.makeAdminCnt, state.lastAdminBody)
	}

	// Revoke super-admin.
	args.Fields["status"] = boolArg(false)
	if _, _, err := userRT.makeAdminActionHandler(context.Background(), args); err != nil {
		t.Fatalf("makeAdmin revoke: %v", err)
	}
	if state.makeAdminCnt != 2 || state.lastAdminBody.Status {
		t.Fatalf("expected makeAdmin called with status=false, got count=%d body=%+v", state.makeAdminCnt, state.lastAdminBody)
	}
}

func TestMakeAdmin_MissingStatus(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		argUserID: strArg("user123"),
	}}

	_, _, err := userRT.makeAdminActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when status is missing")
	}
	if state.makeAdminCnt != 0 {
		t.Fatalf("expected 0 makeAdmin calls on validation failure, got %d", state.makeAdminCnt)
	}
}
