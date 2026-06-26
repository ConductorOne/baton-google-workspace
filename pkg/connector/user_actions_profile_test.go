package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// testProfileServerState backs a mock Directory API that supports the PATCH
// (users.patch) and POST (users.makeAdmin) endpoints exercised by the
// update_user_profile and make_admin actions.
type testProfileServerState struct {
	mtx           sync.Mutex
	users         map[string]*directoryAdmin.User
	getCount      int
	patchCount    int
	makeAdminCnt  int
	lastPatchBody *directoryAdmin.User
	lastAdminBody *directoryAdmin.UserMakeAdmin
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
			})
		case http.MethodPatch:
			state.patchCount++
			body := &directoryAdmin.User{}
			_ = json.NewDecoder(r.Body).Decode(body)
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
			_ = json.NewEncoder(w).Encode(safeUserResponse{
				Id:            u.Id,
				PrimaryEmail:  u.PrimaryEmail,
				Name:          u.Name,
				RecoveryEmail: u.RecoveryEmail,
				CustomSchemas: u.CustomSchemas,
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
		"user_id":    strArg("user123"),
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
		"user_id":        strArg("user123"),
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

func TestUpdateUserProfile_NoUpdatableFields(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": strArg("user123"),
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
		"user_id":        strArg("user123"),
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

func TestMakeAdmin_GrantAndRevoke(t *testing.T) {
	state := &testProfileServerState{
		users: map[string]*directoryAdmin.User{"user123": {Id: "user123"}},
	}
	server := newTestProfileServer(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Grant super-admin.
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": strArg("user123"),
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
		"user_id": strArg("user123"),
	}}

	_, _, err := userRT.makeAdminActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when status is missing")
	}
	if state.makeAdminCnt != 0 {
		t.Fatalf("expected 0 makeAdmin calls on validation failure, got %d", state.makeAdminCnt)
	}
}
