package connector

import (
	"context"
	"encoding/json"
	"fmt"
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

type testSecurityState struct {
	mtx            sync.Mutex
	users          map[string]bool                    // track if user exists
	tokens         map[string][]*directoryAdmin.Token // userID -> tokens
	asps           map[string][]*directoryAdmin.Asp   // userID -> application passwords
	signOutCount   int
	tokenListCount int
	tokenDelCount  int
	aspListCount   int
	aspDelCount    int
}

func newTestSecurityServer(state *testSecurityState) *httptest.Server {
	mux := http.NewServeMux()

	// Handle sign out: POST /admin/directory/v1/users/{userId}/signOut
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		parts := strings.Split(path, "/")
		userKey := parts[0]

		state.mtx.Lock()
		defer state.mtx.Unlock()

		// Sign out endpoint
		if len(parts) == 2 && parts[1] == "signOut" && r.Method == http.MethodPost {
			if !state.users[userKey] {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			state.signOutCount++
			w.WriteHeader(http.StatusOK)
			return
		}

		// Tokens endpoints
		if len(parts) == 2 && parts[1] == "tokens" {
			if r.Method == http.MethodGet {
				state.tokenListCount++
				if !state.users[userKey] {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				tokens := state.tokens[userKey]
				if tokens == nil {
					tokens = []*directoryAdmin.Token{}
				}
				resp := &directoryAdmin.Tokens{
					Items: tokens,
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
		}

		// Delete token: DELETE /admin/directory/v1/users/{userId}/tokens/{clientId}
		if len(parts) == 3 && parts[1] == "tokens" && r.Method == http.MethodDelete {
			state.tokenDelCount++
			clientId := parts[2]
			if !state.users[userKey] {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			// Remove token from state
			tokens := state.tokens[userKey]
			newTokens := []*directoryAdmin.Token{}
			for _, t := range tokens {
				if t.ClientId != clientId {
					newTokens = append(newTokens, t)
				}
			}
			state.tokens[userKey] = newTokens
			w.WriteHeader(http.StatusOK)
			return
		}

		// ASPs endpoints
		if len(parts) == 2 && parts[1] == "asps" {
			if r.Method == http.MethodGet {
				state.aspListCount++
				if !state.users[userKey] {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				asps := state.asps[userKey]
				if asps == nil {
					asps = []*directoryAdmin.Asp{}
				}
				resp := &directoryAdmin.Asps{
					Items: asps,
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
		}

		// Delete ASP: DELETE /admin/directory/v1/users/{userId}/asps/{codeId}
		if len(parts) == 3 && parts[1] == "asps" && r.Method == http.MethodDelete {
			state.aspDelCount++
			codeIdStr := parts[2]
			if !state.users[userKey] {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			// Parse codeId and remove from state
			var codeId int64
			_, _ = fmt.Sscanf(codeIdStr, "%d", &codeId)
			asps := state.asps[userKey]
			newAsps := []*directoryAdmin.Asp{}
			for _, asp := range asps {
				if asp.CodeId != codeId {
					newAsps = append(newAsps, asp)
				}
			}
			state.asps[userKey] = newAsps
			w.WriteHeader(http.StatusOK)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	return httptest.NewServer(mux)
}

func newTestUserResourceTypeWithSecurity(t *testing.T, server *httptest.Server) *userResourceType {
	t.Helper()
	dir := newTestDirectoryService(t, server.URL, server.Client())
	securityDir := newTestDirectoryService(t, server.URL, server.Client())
	return &userResourceType{
		resourceType:            resourceTypeUser,
		userService:             dir,
		userProvisioningService: dir,
		userSecurityService:     securityDir,
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

// Tests for sign_out_user action

func TestSignOutUser_Success(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{
			"user123": true,
		},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	resp, _, err := userRT.signOutUserActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("signOutUser: %v", err)
	}

	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	if state.signOutCount != 1 {
		t.Fatalf("expected 1 sign out call, got %d", state.signOutCount)
	}
}

func TestSignOutUser_UserNotFound(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "nonexistent"}},
	}}

	_, _, err := userRT.signOutUserActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for nonexistent user")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected error message to contain 'not found', got: %v", err)
	}
}

func TestSignOutUser_MissingUserId(t *testing.T) {
	state := &testSecurityState{users: map[string]bool{}}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{}}

	_, _, err := userRT.signOutUserActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing user_id")
	}
	if !strings.Contains(err.Error(), "missing user_id") {
		t.Fatalf("expected error message to contain 'missing user_id', got: %v", err)
	}
}

func TestSignOutUser_NoSecurityService(t *testing.T) {
	userRT := &userResourceType{
		userSecurityService: nil,
	}

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	_, _, err := userRT.signOutUserActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when security service is nil")
	}
	if !strings.Contains(err.Error(), "user security service not available") {
		t.Fatalf("expected error about missing service, got: %v", err)
	}
}

// Tests for delete_all_oauth_tokens action

func TestDeleteAllOAuthTokens_Success_NoTokens(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{
			"user123": true,
		},
		tokens: map[string][]*directoryAdmin.Token{
			"user123": {},
		},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	resp, _, err := userRT.deleteAllOAuthTokensActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("deleteAllOAuthTokens: %v", err)
	}

	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	tokensDeleted := resp.GetFields()["tokens_deleted"].GetNumberValue()
	if tokensDeleted != 0 {
		t.Fatalf("expected tokens_deleted to be 0, got %f", tokensDeleted)
	}

	if state.tokenListCount != 1 {
		t.Fatalf("expected 1 token list call, got %d", state.tokenListCount)
	}
}

func TestDeleteAllOAuthTokens_Success_WithTokens(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{
			"user123": true,
		},
		tokens: map[string][]*directoryAdmin.Token{
			"user123": {
				{ClientId: "client1", DisplayText: "App 1"},
				{ClientId: "client2", DisplayText: "App 2"},
				{ClientId: "client3", DisplayText: "App 3"},
			},
		},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	resp, _, err := userRT.deleteAllOAuthTokensActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("deleteAllOAuthTokens: %v", err)
	}

	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	tokensDeleted := resp.GetFields()["tokens_deleted"].GetNumberValue()
	if tokensDeleted != 3 {
		t.Fatalf("expected tokens_deleted to be 3, got %f", tokensDeleted)
	}

	if state.tokenListCount != 1 {
		t.Fatalf("expected 1 token list call, got %d", state.tokenListCount)
	}
	if state.tokenDelCount != 3 {
		t.Fatalf("expected 3 token delete calls, got %d", state.tokenDelCount)
	}

	// Verify all tokens were deleted
	if len(state.tokens["user123"]) != 0 {
		t.Fatalf("expected all tokens to be deleted, got %d remaining", len(state.tokens["user123"]))
	}
}

func TestDeleteAllOAuthTokens_SkipsEmptyClientId(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{
			"user123": true,
		},
		tokens: map[string][]*directoryAdmin.Token{
			"user123": {
				{ClientId: "client1", DisplayText: "App 1"},
				{ClientId: "", DisplayText: "Invalid Token"}, // Should be skipped
				{ClientId: "client2", DisplayText: "App 2"},
			},
		},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	resp, _, err := userRT.deleteAllOAuthTokensActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("deleteAllOAuthTokens: %v", err)
	}

	// Should delete 2 tokens (skipping the one with empty ClientId)
	tokensDeleted := resp.GetFields()["tokens_deleted"].GetNumberValue()
	if tokensDeleted != 2 {
		t.Fatalf("expected tokens_deleted to be 2, got %f", tokensDeleted)
	}

	if state.tokenDelCount != 2 {
		t.Fatalf("expected 2 token delete calls, got %d", state.tokenDelCount)
	}
}

func TestDeleteAllOAuthTokens_UserNotFound(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "nonexistent"}},
	}}

	_, _, err := userRT.deleteAllOAuthTokensActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for nonexistent user")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected error message to contain 'not found', got: %v", err)
	}
}

// Tests for delete_all_application_passwords action

func TestDeleteAllApplicationPasswords_Success_NoPasswords(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{
			"user123": true,
		},
		asps: map[string][]*directoryAdmin.Asp{
			"user123": {},
		},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	resp, _, err := userRT.deleteAllApplicationPasswordsActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("deleteAllApplicationPasswords: %v", err)
	}

	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	passwordsDeleted := resp.GetFields()["passwords_deleted"].GetNumberValue()
	if passwordsDeleted != 0 {
		t.Fatalf("expected passwords_deleted to be 0, got %f", passwordsDeleted)
	}

	if state.aspListCount != 1 {
		t.Fatalf("expected 1 ASP list call, got %d", state.aspListCount)
	}
}

func TestDeleteAllApplicationPasswords_Success_WithPasswords(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{
			"user123": true,
		},
		asps: map[string][]*directoryAdmin.Asp{
			"user123": {
				{CodeId: 1, Name: "App Password 1"},
				{CodeId: 2, Name: "App Password 2"},
				{CodeId: 3, Name: "App Password 3"},
			},
		},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "user123"}},
	}}

	resp, _, err := userRT.deleteAllApplicationPasswordsActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("deleteAllApplicationPasswords: %v", err)
	}

	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	passwordsDeleted := resp.GetFields()["passwords_deleted"].GetNumberValue()
	if passwordsDeleted != 3 {
		t.Fatalf("expected passwords_deleted to be 3, got %f", passwordsDeleted)
	}

	if state.aspListCount != 1 {
		t.Fatalf("expected 1 ASP list call, got %d", state.aspListCount)
	}
	if state.aspDelCount != 3 {
		t.Fatalf("expected 3 ASP delete calls, got %d", state.aspDelCount)
	}

	// Verify all ASPs were deleted
	if len(state.asps["user123"]) != 0 {
		t.Fatalf("expected all ASPs to be deleted, got %d remaining", len(state.asps["user123"]))
	}
}

func TestDeleteAllApplicationPasswords_UserNotFound(t *testing.T) {
	state := &testSecurityState{
		users: map[string]bool{},
	}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "nonexistent"}},
	}}

	_, _, err := userRT.deleteAllApplicationPasswordsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for nonexistent user")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected error message to contain 'not found', got: %v", err)
	}
}

func TestDeleteAllApplicationPasswords_MissingUserId(t *testing.T) {
	state := &testSecurityState{users: map[string]bool{}}
	server := newTestSecurityServer(state)
	defer server.Close()

	userRT := newTestUserResourceTypeWithSecurity(t, server)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{}}

	_, _, err := userRT.deleteAllApplicationPasswordsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing user_id")
	}
	if !strings.Contains(err.Error(), "missing user_id") {
		t.Fatalf("expected error message to contain 'missing user_id', got: %v", err)
	}
}
