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

type testUserWithOrgUnit struct {
	Id           string
	PrimaryEmail string
	OrgUnitPath  string
}

type testServerStateWithOrgUnit struct {
	mtx      sync.Mutex
	users    map[string]*testUserWithOrgUnit
	putCount int
	getCount int
}

func newTestServerWithOrgUnit(state *testServerStateWithOrgUnit) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		// path suffix is userKey
		userKey := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		state.mtx.Lock()
		defer state.mtx.Unlock()
		switch r.Method {
		case http.MethodGet:
			state.getCount++
			u := state.users[userKey]
			if u == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			resp := &directoryAdmin.User{
				Id:           u.Id,
				PrimaryEmail: u.PrimaryEmail,
				OrgUnitPath:  u.OrgUnitPath,
				Name: &directoryAdmin.UserName{
					FullName: u.PrimaryEmail, // Use email as fallback for full name
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		case http.MethodPut:
			state.putCount++
			var body directoryAdmin.User
			_ = json.NewDecoder(r.Body).Decode(&body)
			u := state.users[userKey]
			if u == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			// Update org unit path if provided
			if body.OrgUnitPath != "" {
				u.OrgUnitPath = body.OrgUnitPath
			}
			resp := &directoryAdmin.User{
				Id:           u.Id,
				PrimaryEmail: u.PrimaryEmail,
				OrgUnitPath:  u.OrgUnitPath,
				Name: &directoryAdmin.UserName{
					FullName: u.PrimaryEmail, // Use email as fallback for full name
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func newTestUserResourceType(t *testing.T, server *httptest.Server) *userResourceType {
	t.Helper()
	dir := newTestDirectoryService(t, server.URL, server.Client())
	return &userResourceType{
		resourceType:            resourceTypeUser,
		userService:             dir,
		userProvisioningService: dir,
		customerId:              "test-customer",
		domain:                  "",
	}
}

func TestChangeUserOrgUnit_Success(t *testing.T) {
	state := &testServerStateWithOrgUnit{
		users: map[string]*testUserWithOrgUnit{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				OrgUnitPath:  "/",
			},
		},
	}
	server := newTestServerWithOrgUnit(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/sales"}},
	}}

	resp, _, err := userRT.changeUserOrgUnitActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("changeUserOrgUnit: %v", err)
	}

	// Verify user was updated
	if state.users["user123"].OrgUnitPath != "/sales" {
		t.Fatalf("expected org unit path to be '/sales', got '%s'", state.users["user123"].OrgUnitPath)
	}

	// Verify response
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}
	if resp.GetFields()["resource"] == nil {
		t.Fatalf("expected resource in response")
	}

	// Verify PUT was called
	if state.putCount != 1 {
		t.Fatalf("expected 1 PUT, got %d", state.putCount)
	}
}

func TestChangeUserOrgUnit_Idempotent(t *testing.T) {
	state := &testServerStateWithOrgUnit{
		users: map[string]*testUserWithOrgUnit{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				OrgUnitPath:  "/sales",
			},
		},
	}
	server := newTestServerWithOrgUnit(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/sales"}},
	}}

	// First call - should succeed without PUT since already in target org unit
	resp, _, err := userRT.changeUserOrgUnitActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("changeUserOrgUnit: %v", err)
	}

	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify PUT was NOT called (idempotent)
	if state.putCount != 0 {
		t.Fatalf("expected 0 PUT calls (idempotent), got %d", state.putCount)
	}

	// Verify GET was called to check current state
	if state.getCount < 1 {
		t.Fatalf("expected at least 1 GET call, got %d", state.getCount)
	}
}

func TestChangeUserOrgUnit_MissingUserId(t *testing.T) {
	state := &testServerStateWithOrgUnit{users: map[string]*testUserWithOrgUnit{}}
	server := newTestServerWithOrgUnit(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/sales"}},
	}}

	_, _, err := userRT.changeUserOrgUnitActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing user_id")
	}
	if !strings.Contains(err.Error(), "missing user_id") {
		t.Fatalf("expected error message to contain 'missing user_id', got: %v", err)
	}
}

func TestChangeUserOrgUnit_MissingOrgUnitPath(t *testing.T) {
	state := &testServerStateWithOrgUnit{users: map[string]*testUserWithOrgUnit{}}
	server := newTestServerWithOrgUnit(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	_, _, err := userRT.changeUserOrgUnitActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing org_unit_path")
	}
	if !strings.Contains(err.Error(), "missing org_unit_path") {
		t.Fatalf("expected error message to contain 'missing org_unit_path', got: %v", err)
	}
}

func TestChangeUserOrgUnit_InvalidOrgUnitPathFormat(t *testing.T) {
	state := &testServerStateWithOrgUnit{
		users: map[string]*testUserWithOrgUnit{
			"user123": {
				Id:           "user123",
				PrimaryEmail: "test@example.com",
				OrgUnitPath:  "/",
			},
		},
	}
	server := newTestServerWithOrgUnit(state)
	defer server.Close()

	userRT := newTestUserResourceType(t, server)

	// Test with path that doesn't start with '/'
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "sales"}},
	}}

	_, _, err := userRT.changeUserOrgUnitActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid org_unit_path format")
	}
	if !strings.Contains(err.Error(), "must start with '/'") {
		t.Fatalf("expected error message to contain 'must start with '/'', got: %v", err)
	}

	// Test with empty path
	argsEmpty := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: ""}},
	}}

	_, _, err = userRT.changeUserOrgUnitActionHandler(context.Background(), argsEmpty)
	if err == nil {
		t.Fatalf("expected error for empty org_unit_path")
	}
}
