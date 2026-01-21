package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

type testUser struct {
	Suspended     bool
	PrimaryEmail  string
	OrgUnitPath   string
	RecoveryEmail string
}

type transferRecord struct {
	Id       string
	OldOwner string
	NewOwner string
	AppID    int64
	Status   string
	Params   map[string][]string
}

type testServerState struct {
	mtx       sync.Mutex
	users     map[string]*testUser
	transfers []*transferRecord
	putCount  int
	postCount int
}

func newTestServer(state *testServerState) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		// path suffix is userKey
		userKey := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		state.mtx.Lock()
		defer state.mtx.Unlock()
		switch r.Method {
		case http.MethodGet:
			u := state.users[userKey]
			if u == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			resp := &directoryAdmin.User{
				Suspended:     u.Suspended,
				PrimaryEmail:  u.PrimaryEmail,
				OrgUnitPath:   u.OrgUnitPath,
				RecoveryEmail: u.RecoveryEmail,
			}
			_ = json.NewEncoder(w).Encode(resp)
		case http.MethodPut:
			state.putCount++
			// Read body as bytes so we can unmarshal twice
			var bodyBytes []byte
			if r.Body != nil {
				bodyBytes, _ = io.ReadAll(r.Body)
			}

			// Unmarshal to both map (for field detection) and User struct
			var rawMap map[string]interface{}
			var body directoryAdmin.User
			_ = json.Unmarshal(bodyBytes, &rawMap)
			_ = json.Unmarshal(bodyBytes, &body)

			u := state.users[userKey]
			if u == nil {
				u = &testUser{}
				state.users[userKey] = u
			}
			// Apply incoming fields
			if body.PrimaryEmail != "" {
				u.PrimaryEmail = body.PrimaryEmail
			}
			if body.OrgUnitPath != "" {
				u.OrgUnitPath = body.OrgUnitPath
			}
			// RecoveryEmail: Check if field exists in JSON (handles both set and clear)
			if _, hasRecoveryEmail := rawMap["recoveryEmail"]; hasRecoveryEmail {
				u.RecoveryEmail = body.RecoveryEmail
			}
			// Suspended is bool; accept as-is
			u.Suspended = body.Suspended
			resp := &directoryAdmin.User{
				Suspended:     u.Suspended,
				PrimaryEmail:  u.PrimaryEmail,
				OrgUnitPath:   u.OrgUnitPath,
				RecoveryEmail: u.RecoveryEmail,
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/admin/datatransfer/v1/transfers", func(w http.ResponseWriter, r *http.Request) {
		state.mtx.Lock()
		defer state.mtx.Unlock()
		switch r.Method {
		case http.MethodGet:
			q := r.URL.Query()
			oldOwner := q.Get("oldOwnerUserId")
			newOwner := q.Get("newOwnerUserId")
			pageToken := q.Get("pageToken")
			_ = pageToken // single page for tests
			var items []*datatransferAdmin.DataTransfer
			for _, tr := range state.transfers {
				if tr.OldOwner == oldOwner && tr.NewOwner == newOwner {
					items = append(items, &datatransferAdmin.DataTransfer{
						Id:                        tr.Id,
						OldOwnerUserId:            tr.OldOwner,
						NewOwnerUserId:            tr.NewOwner,
						OverallTransferStatusCode: tr.Status,
						ApplicationDataTransfers: []*datatransferAdmin.ApplicationDataTransfer{{
							ApplicationId: tr.AppID,
						}},
					})
				}
			}
			resp := &datatransferAdmin.DataTransfersListResponse{
				DataTransfers: items,
				NextPageToken: "",
			}
			_ = json.NewEncoder(w).Encode(resp)
		case http.MethodPost:
			state.postCount++
			var body datatransferAdmin.DataTransfer
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := "tr_" + strings.ReplaceAll(url.QueryEscape(body.OldOwnerUserId+"_"+body.NewOwnerUserId), "%", "")
			var appID int64
			if len(body.ApplicationDataTransfers) > 0 {
				appID = body.ApplicationDataTransfers[0].ApplicationId
			}
			tr := &transferRecord{
				Id:       id,
				OldOwner: body.OldOwnerUserId,
				NewOwner: body.NewOwnerUserId,
				AppID:    appID,
				Status:   "NEW",
				Params:   map[string][]string{},
			}
			state.transfers = append(state.transfers, tr)
			resp := &datatransferAdmin.DataTransfer{
				Id:                        id,
				OverallTransferStatusCode: tr.Status,
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func newTestDirectoryService(t *testing.T, baseURL string, hc *http.Client) *directoryAdmin.Service {
	t.Helper()
	srv, err := directoryAdmin.NewService(context.Background(), option.WithEndpoint(baseURL+"/"), option.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("newTestDirectoryService: %v", err)
	}
	return srv
}

func newTestDataTransferService(t *testing.T, baseURL string, hc *http.Client) *datatransferAdmin.Service {
	t.Helper()
	srv, err := datatransferAdmin.NewService(context.Background(), option.WithEndpoint(baseURL+"/"), option.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("newTestDataTransferService: %v", err)
	}
	return srv
}

func primeServiceCache(c *GoogleWorkspace, dir *directoryAdmin.Service, dt *datatransferAdmin.Service) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.serviceCache == nil {
		c.serviceCache = map[string]any{}
	}
	if dir != nil {
		c.serviceCache[directoryAdmin.AdminDirectoryUserScope] = dir
		c.serviceCache[directoryAdmin.AdminDirectoryGroupScope] = dir
	}
	if dt != nil {
		c.serviceCache[datatransferAdmin.AdminDatatransferScope] = dt
	}
}

func newTestConnector() *GoogleWorkspace {
	return &GoogleWorkspace{
		serviceCache: map[string]any{},
	}
}

func TestDisableEnableUser_IdempotentAndPayload(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{"alice": {Suspended: false, PrimaryEmail: "alice@example.com"}}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// disable user
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
	}}
	if _, _, err := c.disableUserActionHandler(context.Background(), args); err != nil {
		t.Fatalf("disableUser: %v", err)
	}
	if !state.users["alice"].Suspended {
		t.Fatalf("expected alice to be suspended")
	}
	// call again should not PUT again
	prevPut := state.putCount
	if _, _, err := c.disableUserActionHandler(context.Background(), args); err != nil {
		t.Fatalf("disableUser second: %v", err)
	}
	if state.putCount != prevPut {
		t.Fatalf("expected no additional PUT on idempotent disable, got %d vs %d", state.putCount, prevPut)
	}

	// enable user
	argsEnable := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
	}}
	if _, _, err := c.enableUserActionHandler(context.Background(), argsEnable); err != nil {
		t.Fatalf("enableUser: %v", err)
	}
	if state.users["alice"].Suspended {
		t.Fatalf("expected alice to be active")
	}
}

func TestChangePrimaryEmail(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{"bob": {Suspended: false, PrimaryEmail: "bob@old.example.com"}}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":       {Kind: &structpb.Value_StringValue{StringValue: "bob"}},
		"new_primary_email": {Kind: &structpb.Value_StringValue{StringValue: "bob@new.example.com"}},
	}}
	resp, _, err := c.changeUserPrimaryEmail(context.Background(), args)
	if err != nil {
		t.Fatalf("changeUserPrimaryEmail: %v", err)
	}
	if state.users["bob"].PrimaryEmail != "bob@new.example.com" {
		t.Fatalf("expected primary email updated")
	}
	if resp.GetFields()["previous_primary_email"].GetStringValue() != "bob@old.example.com" {
		t.Fatalf("expected previous primary email in response")
	}
}

func TestChangePrimaryEmail_InvalidEmail(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{"bob": {Suspended: false, PrimaryEmail: "bob@old.example.com"}}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// Test with invalid email format
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":       {Kind: &structpb.Value_StringValue{StringValue: "bob"}},
		"new_primary_email": {Kind: &structpb.Value_StringValue{StringValue: "invalid-email"}},
	}}
	_, _, err := c.changeUserPrimaryEmail(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid email format")
	}
	if !strings.Contains(err.Error(), "invalid email address") {
		t.Fatalf("expected error message to contain 'invalid email address', got: %v", err)
	}

	// Test with empty email
	argsEmpty := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":       {Kind: &structpb.Value_StringValue{StringValue: "bob"}},
		"new_primary_email": {Kind: &structpb.Value_StringValue{StringValue: ""}},
	}}
	_, _, err = c.changeUserPrimaryEmail(context.Background(), argsEmpty)
	if err == nil {
		t.Fatalf("expected error for empty email")
	}
	if !strings.Contains(err.Error(), "invalid email address") {
		t.Fatalf("expected error message to contain 'invalid email address', got: %v", err)
	}

	// Test with malformed email (missing @)
	argsMalformed := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":       {Kind: &structpb.Value_StringValue{StringValue: "bob"}},
		"new_primary_email": {Kind: &structpb.Value_StringValue{StringValue: "bob.example.com"}},
	}}
	_, _, err = c.changeUserPrimaryEmail(context.Background(), argsMalformed)
	if err == nil {
		t.Fatalf("expected error for malformed email")
	}
	if !strings.Contains(err.Error(), "invalid email address") {
		t.Fatalf("expected error message to contain 'invalid email address', got: %v", err)
	}
}

func TestTransferDrive_IdempotentAndPrivacyLevels(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{"src": {}, "dst": {}}, transfers: []*transferRecord{}}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	// First call: no existing, expect POST
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":        {Kind: &structpb.Value_StringValue{StringValue: "src"}},
		"target_resource_id": {Kind: &structpb.Value_StringValue{StringValue: "dst"}},
		// omit privacy_levels -> default both
	}}
	if _, _, err := c.transferUserDriveFiles(context.Background(), args); err != nil {
		t.Fatalf("transferUserDriveFiles: %v", err)
	}
	if state.postCount != 1 {
		t.Fatalf("expected 1 POST, got %d", state.postCount)
	}

	// Second call: idempotent should List and return existing, no new POST
	prevPost := state.postCount
	if _, _, err := c.transferUserDriveFiles(context.Background(), args); err != nil {
		t.Fatalf("transferUserDriveFiles idempotent: %v", err)
	}
	if state.postCount != prevPost {
		t.Fatalf("expected no additional POST on idempotent transfer")
	}

	// Invalid privacy_levels type
	badArgs := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":        {Kind: &structpb.Value_StringValue{StringValue: "src"}},
		"target_resource_id": {Kind: &structpb.Value_StringValue{StringValue: "dst"}},
		"privacy_levels":     {Kind: &structpb.Value_StringValue{StringValue: "PRIVATE"}}, // not a list
	}}
	if _, _, err := c.transferUserDriveFiles(context.Background(), badArgs); err == nil {
		t.Fatalf("expected error on privacy_levels wrong type")
	}
}

func TestTransferCalendar_ReleaseResources(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{"src": {}, "dst": {}}, transfers: []*transferRecord{}}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":        {Kind: &structpb.Value_StringValue{StringValue: "src"}},
		"target_resource_id": {Kind: &structpb.Value_StringValue{StringValue: "dst"}},
		"release_resources":  {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	if _, _, err := c.transferUserCalendar(context.Background(), args); err != nil {
		t.Fatalf("transferUserCalendar: %v", err)
	}
	if state.postCount != 1 {
		t.Fatalf("expected 1 POST, got %d", state.postCount)
	}
}

func TestMoveAccountToOrgUnit_Success(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"alice": {Suspended: false, PrimaryEmail: "alice@example.com", OrgUnitPath: "/Engineering"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/Sales"}},
	}}
	resp, _, err := c.moveAccountToOrgUnit(context.Background(), args)
	if err != nil {
		t.Fatalf("moveAccountToOrgUnit: %v", err)
	}
	if state.users["alice"].OrgUnitPath != "/Sales" {
		t.Fatalf("expected org unit path updated to /Sales, got %s", state.users["alice"].OrgUnitPath)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true in response")
	}
	if resp.GetFields()["previous_org_unit_path"].GetStringValue() != "/Engineering" {
		t.Fatalf("expected previous_org_unit_path=/Engineering in response")
	}
	if resp.GetFields()["new_org_unit_path"].GetStringValue() != "/Sales" {
		t.Fatalf("expected new_org_unit_path=/Sales in response")
	}
}

func TestMoveAccountToOrgUnit_Idempotent(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"bob": {Suspended: false, PrimaryEmail: "bob@example.com", OrgUnitPath: "/Sales"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "bob"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/Sales"}},
	}}

	// First call - user already at target org unit
	prevPut := state.putCount
	resp, _, err := c.moveAccountToOrgUnit(context.Background(), args)
	if err != nil {
		t.Fatalf("moveAccountToOrgUnit: %v", err)
	}
	if state.putCount != prevPut {
		t.Fatalf("expected no PUT on idempotent move, got %d vs %d", state.putCount, prevPut)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true in response")
	}
	if resp.GetFields()["previous_org_unit_path"].GetStringValue() != "/Sales" {
		t.Fatalf("expected previous_org_unit_path=/Sales in response")
	}
	if resp.GetFields()["new_org_unit_path"].GetStringValue() != "/Sales" {
		t.Fatalf("expected new_org_unit_path=/Sales in response")
	}
}

func TestMoveAccountToOrgUnit_RootOrgUnit(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"charlie": {Suspended: false, PrimaryEmail: "charlie@example.com", OrgUnitPath: "/Engineering"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// Move user to root "/"
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "charlie"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/"}},
	}}
	resp, _, err := c.moveAccountToOrgUnit(context.Background(), args)
	if err != nil {
		t.Fatalf("moveAccountToOrgUnit: %v", err)
	}
	if state.users["charlie"].OrgUnitPath != "/" {
		t.Fatalf("expected org unit path updated to /, got %s", state.users["charlie"].OrgUnitPath)
	}
	if resp.GetFields()["previous_org_unit_path"].GetStringValue() != "/Engineering" {
		t.Fatalf("expected previous_org_unit_path=/Engineering in response")
	}
	if resp.GetFields()["new_org_unit_path"].GetStringValue() != "/" {
		t.Fatalf("expected new_org_unit_path=/ in response")
	}
}

func TestMoveAccountToOrgUnit_EmptyOrgUnitPathTreatedAsRoot(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"david": {Suspended: false, PrimaryEmail: "david@example.com", OrgUnitPath: ""},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// User's current OrgUnitPath is empty (root), try to move to explicit "/"
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "david"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/"}},
	}}

	prevPut := state.putCount
	resp, _, err := c.moveAccountToOrgUnit(context.Background(), args)
	if err != nil {
		t.Fatalf("moveAccountToOrgUnit: %v", err)
	}
	// Should be idempotent - empty string is treated as "/"
	if state.putCount != prevPut {
		t.Fatalf("expected no PUT when moving from empty (root) to / (root)")
	}
	if resp.GetFields()["previous_org_unit_path"].GetStringValue() != "/" {
		t.Fatalf("expected previous_org_unit_path=/ in response (empty treated as root)")
	}
}

func TestMoveAccountToOrgUnit_ValidationErrors(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"alice": {Suspended: false, PrimaryEmail: "alice@example.com", OrgUnitPath: "/Engineering"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// Missing user_id
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/Sales"}},
	}}
	_, _, err := c.moveAccountToOrgUnit(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing user_id")
	}
	if !strings.Contains(err.Error(), "missing user_id") {
		t.Fatalf("expected error message to contain 'missing user_id', got: %v", err)
	}

	// Missing org_unit_path
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
	}}
	_, _, err = c.moveAccountToOrgUnit(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing org_unit_path")
	}
	if !strings.Contains(err.Error(), "missing org_unit_path") {
		t.Fatalf("expected error message to contain 'missing org_unit_path', got: %v", err)
	}

	// Empty user_id after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "   "}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/Sales"}},
	}}
	_, _, err = c.moveAccountToOrgUnit(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty user_id")
	}
	if !strings.Contains(err.Error(), "user_id must be non-empty") {
		t.Fatalf("expected error message to contain 'user_id must be non-empty', got: %v", err)
	}

	// Empty org_unit_path after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "   "}},
	}}
	_, _, err = c.moveAccountToOrgUnit(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty org_unit_path")
	}
	if !strings.Contains(err.Error(), "org_unit_path must be non-empty") {
		t.Fatalf("expected error message to contain 'org_unit_path must be non-empty', got: %v", err)
	}

	// org_unit_path without leading "/"
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "Sales"}},
	}}
	_, _, err = c.moveAccountToOrgUnit(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for org_unit_path without leading /")
	}
	if !strings.Contains(err.Error(), "org_unit_path must start with '/'") {
		t.Fatalf("expected error message to contain 'org_unit_path must start with '/'', got: %v", err)
	}
}

func TestMoveAccountToOrgUnit_UserNotFound(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":       {Kind: &structpb.Value_StringValue{StringValue: "nonexistent"}},
		"org_unit_path": {Kind: &structpb.Value_StringValue{StringValue: "/Sales"}},
	}}
	_, _, err := c.moveAccountToOrgUnit(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for user not found")
	}
}

func TestUpdateEmergencyEmail_Success(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"alice": {Suspended: false, PrimaryEmail: "alice@example.com", RecoveryEmail: "oldrecovery@example.com"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":         {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: "newrecovery@example.com"}},
	}}
	resp, _, err := c.updateEmergencyEmail(context.Background(), args)
	if err != nil {
		t.Fatalf("updateEmergencyEmail: %v", err)
	}
	if state.users["alice"].RecoveryEmail != "newrecovery@example.com" {
		t.Fatalf("expected recovery email updated to newrecovery@example.com, got %s", state.users["alice"].RecoveryEmail)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true in response")
	}
	if resp.GetFields()["previous_emergency_email"].GetStringValue() != "oldrecovery@example.com" {
		t.Fatalf("expected previous_emergency_email=oldrecovery@example.com in response")
	}
	if resp.GetFields()["new_emergency_email"].GetStringValue() != "newrecovery@example.com" {
		t.Fatalf("expected new_emergency_email=newrecovery@example.com in response")
	}
}

func TestUpdateEmergencyEmail_Idempotent(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"bob": {Suspended: false, PrimaryEmail: "bob@example.com", RecoveryEmail: "recovery@example.com"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":         {Kind: &structpb.Value_StringValue{StringValue: "bob"}},
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: "recovery@example.com"}},
	}}

	// User already has target email - should not PUT
	prevPut := state.putCount
	resp, _, err := c.updateEmergencyEmail(context.Background(), args)
	if err != nil {
		t.Fatalf("updateEmergencyEmail: %v", err)
	}
	if state.putCount != prevPut {
		t.Fatalf("expected no PUT on idempotent update, got %d vs %d", state.putCount, prevPut)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true in response")
	}
	if resp.GetFields()["previous_emergency_email"].GetStringValue() != "recovery@example.com" {
		t.Fatalf("expected previous_emergency_email=recovery@example.com in response")
	}
	if resp.GetFields()["new_emergency_email"].GetStringValue() != "recovery@example.com" {
		t.Fatalf("expected new_emergency_email=recovery@example.com in response")
	}
}

func TestUpdateEmergencyEmail_ValidationErrors(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"alice": {Suspended: false, PrimaryEmail: "alice@example.com"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// Missing user_id
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: "recovery@example.com"}},
	}}
	_, _, err := c.updateEmergencyEmail(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing user_id")
	}
	if !strings.Contains(err.Error(), "missing user_id") {
		t.Fatalf("expected error message to contain 'missing user_id', got: %v", err)
	}

	// Missing emergency_email
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id": {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
	}}
	_, _, err = c.updateEmergencyEmail(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing emergency_email")
	}
	if !strings.Contains(err.Error(), "missing emergency_email") {
		t.Fatalf("expected error message to contain 'missing emergency_email', got: %v", err)
	}

	// Empty user_id after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":         {Kind: &structpb.Value_StringValue{StringValue: "   "}},
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: "recovery@example.com"}},
	}}
	_, _, err = c.updateEmergencyEmail(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty user_id")
	}
	if !strings.Contains(err.Error(), "user_id must be non-empty") {
		t.Fatalf("expected error message to contain 'user_id must be non-empty', got: %v", err)
	}

	// Invalid email format
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":         {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: "not-an-email"}},
	}}
	_, _, err = c.updateEmergencyEmail(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid email format")
	}
	if !strings.Contains(err.Error(), "valid email address") {
		t.Fatalf("expected error message to contain 'valid email address', got: %v", err)
	}
}

func TestUpdateEmergencyEmail_UserNotFound(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":         {Kind: &structpb.Value_StringValue{StringValue: "nonexistent"}},
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: "recovery@example.com"}},
	}}
	_, _, err := c.updateEmergencyEmail(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for user not found")
	}
}

func TestUpdateEmergencyEmail_ClearEmail(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{
		"charlie": {Suspended: false, PrimaryEmail: "charlie@example.com", RecoveryEmail: "recovery@example.com"},
	}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// Clear the email by passing empty string
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":         {Kind: &structpb.Value_StringValue{StringValue: "charlie"}},
		"emergency_email": {Kind: &structpb.Value_StringValue{StringValue: ""}},
	}}
	prevPut := state.putCount
	resp, _, err := c.updateEmergencyEmail(context.Background(), args)
	if err != nil {
		t.Fatalf("updateEmergencyEmail: %v", err)
	}
	if state.putCount == prevPut {
		t.Fatalf("expected PUT call to clear email, but no PUT was made")
	}
	if state.users["charlie"].RecoveryEmail != "" {
		t.Fatalf("expected recovery email cleared, got %s", state.users["charlie"].RecoveryEmail)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true in response")
	}
	if resp.GetFields()["previous_emergency_email"].GetStringValue() != "recovery@example.com" {
		t.Fatalf("expected previous_emergency_email=recovery@example.com in response")
	}
	if resp.GetFields()["new_emergency_email"].GetStringValue() != "" {
		t.Fatalf("expected new_emergency_email='' (empty) in response")
	}
}
