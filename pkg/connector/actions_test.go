package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

type testUser struct {
	Suspended    bool
	PrimaryEmail string
}

type testGroup struct {
	Id                     string
	Email                  string
	Name                   string
	Description            string
	AllowExternalMembers   string // "true" or "false"
	WhoCanPostMessage      string
	MessageModerationLevel string
	Members                map[string]string // email -> role
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
	mtx         sync.Mutex
	users       map[string]*testUser
	groups      map[string]*testGroup
	transfers   []*transferRecord
	putCount    int
	postCount   int
	insertCount int
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
				Suspended:    u.Suspended,
				PrimaryEmail: u.PrimaryEmail,
			}
			_ = json.NewEncoder(w).Encode(resp)
		case http.MethodPut:
			state.putCount++
			var body directoryAdmin.User
			_ = json.NewDecoder(r.Body).Decode(&body)
			u := state.users[userKey]
			if u == nil {
				u = &testUser{}
				state.users[userKey] = u
			}
			// Apply incoming fields (ForceSendFields is not needed in test server)
			if body.PrimaryEmail != "" {
				u.PrimaryEmail = body.PrimaryEmail
			}
			// Suspended is bool; accept as-is
			u.Suspended = body.Suspended
			resp := &directoryAdmin.User{
				Suspended:    u.Suspended,
				PrimaryEmail: u.PrimaryEmail,
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/admin/directory/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		state.mtx.Lock()
		defer state.mtx.Unlock()
		switch r.Method {
		case http.MethodPost:
			state.postCount++
			state.insertCount++
			var body directoryAdmin.Group
			_ = json.NewDecoder(r.Body).Decode(&body)
			// Generate a unique ID for the group
			groupId := "group_" + strings.ReplaceAll(body.Email, "@", "_at_")
			g := &testGroup{
				Id:          groupId,
				Email:       body.Email,
				Name:        body.Name,
				Description: body.Description,
			}
			state.groups[body.Email] = g
			resp := &directoryAdmin.Group{
				Id:          g.Id,
				Email:       g.Email,
				Name:        g.Name,
				Description: g.Description,
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/admin/directory/v1/groups/", func(w http.ResponseWriter, r *http.Request) {
		// Handle both group operations and member operations
		// Group GET: /admin/directory/v1/groups/{groupKey}
		// Member GET: /admin/directory/v1/groups/{groupKey}/members/{userEmail}
		// Member POST: /admin/directory/v1/groups/{groupKey}/members
		// Member PATCH: /admin/directory/v1/groups/{groupKey}/members/{userEmail}
		path := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/groups/")
		parts := strings.Split(path, "/")
		
		state.mtx.Lock()
		defer state.mtx.Unlock()

		// Check if this is a member operation
		if len(parts) >= 2 && parts[1] == "members" {
			groupKey := parts[0]

			// Look up group by email or ID
			var g *testGroup
			for email, group := range state.groups {
				if email == groupKey || group.Id == groupKey {
					g = group
					break
				}
			}

			if g == nil {
				http.Error(w, "group not found", http.StatusNotFound)
				return
			}

			// Initialize Members map if needed
			if g.Members == nil {
				g.Members = make(map[string]string)
			}

			if len(parts) == 2 {
				// /admin/directory/v1/groups/{groupKey}/members - POST to add member
				if r.Method == http.MethodPost {
					state.postCount++
					var body directoryAdmin.Member
					_ = json.NewDecoder(r.Body).Decode(&body)
					
					memberEmail := strings.ToLower(body.Email)
					role := body.Role
					if role == "" {
						role = "MEMBER"
					}
					g.Members[memberEmail] = role

					resp := &directoryAdmin.Member{
						Email: body.Email,
						Role:  role,
						Id:    "member_" + strings.ReplaceAll(memberEmail, "@", "_at_"),
					}
					_ = json.NewEncoder(w).Encode(resp)
					return
				}
			} else if len(parts) == 3 {
				// /admin/directory/v1/groups/{groupKey}/members/{userEmail}
				userEmail := strings.ToLower(parts[2])

				switch r.Method {
				case http.MethodGet:
					// Check if member exists
					role, exists := g.Members[userEmail]
					if !exists {
						http.Error(w, "member not found", http.StatusNotFound)
						return
					}

					resp := &directoryAdmin.Member{
						Email: userEmail,
						Role:  role,
						Id:    "member_" + strings.ReplaceAll(userEmail, "@", "_at_"),
					}
					_ = json.NewEncoder(w).Encode(resp)
					return

				case http.MethodPatch:
					// Update member role
					state.putCount++
					var body directoryAdmin.Member
					_ = json.NewDecoder(r.Body).Decode(&body)

					if _, exists := g.Members[userEmail]; !exists {
						http.Error(w, "member not found", http.StatusNotFound)
						return
					}

					g.Members[userEmail] = body.Role

					resp := &directoryAdmin.Member{
						Email: userEmail,
						Role:  body.Role,
						Id:    "member_" + strings.ReplaceAll(userEmail, "@", "_at_"),
					}
					_ = json.NewEncoder(w).Encode(resp)
					return
				}
			}

			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		// Handle group GET operation (not a member operation)
		groupKey := parts[0]
		
		if r.Method == http.MethodGet {
			// Look up by email first
			g := state.groups[groupKey]
			if g == nil {
				// Try looking up by ID
				for _, group := range state.groups {
					if group.Id == groupKey {
						g = group
						break
					}
				}
			}
			if g == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			resp := &directoryAdmin.Group{
				Id:          g.Id,
				Email:       g.Email,
				Name:        g.Name,
				Description: g.Description,
			}
			_ = json.NewEncoder(w).Encode(resp)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Groups Settings API handlers
	mux.HandleFunc("/groups/v1/groups/", func(w http.ResponseWriter, r *http.Request) {
		// path suffix is groupKey (email)
		groupKey := strings.TrimPrefix(r.URL.Path, "/groups/v1/groups/")
		state.mtx.Lock()
		defer state.mtx.Unlock()

		g := state.groups[groupKey]
		if g == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		switch r.Method {
		case http.MethodGet:
			// Return current settings
			// Initialize with defaults if not set
			if g.AllowExternalMembers == "" {
				g.AllowExternalMembers = "false"
			}
			if g.WhoCanPostMessage == "" {
				g.WhoCanPostMessage = "ALL_MEMBERS_CAN_POST"
			}
			if g.MessageModerationLevel == "" {
				g.MessageModerationLevel = "MODERATE_NONE"
			}

			resp := map[string]interface{}{
				"allowExternalMembers":   g.AllowExternalMembers,
				"whoCanPostMessage":      g.WhoCanPostMessage,
				"messageModerationLevel": g.MessageModerationLevel,
			}
			_ = json.NewEncoder(w).Encode(resp)

		case http.MethodPatch, http.MethodPut:
			state.putCount++
			var body map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&body)

			// Update settings if provided
			if allowExt, ok := body["allowExternalMembers"].(string); ok {
				g.AllowExternalMembers = allowExt
			}
			if whoCanPost, ok := body["whoCanPostMessage"].(string); ok {
				g.WhoCanPostMessage = whoCanPost
			}
			if modLevel, ok := body["messageModerationLevel"].(string); ok {
				g.MessageModerationLevel = modLevel
			}

			resp := map[string]interface{}{
				"allowExternalMembers":   g.AllowExternalMembers,
				"whoCanPostMessage":      g.WhoCanPostMessage,
				"messageModerationLevel": g.MessageModerationLevel,
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

func newTestGroupsSettingsService(t *testing.T, baseURL string, hc *http.Client) *groupssettings.Service {
	t.Helper()
	// The groupssettings API base path is /groups/v1/groups/
	// We need to append this to our test server URL
	srv, err := groupssettings.NewService(context.Background(), option.WithEndpoint(baseURL+"/groups/v1/groups/"), option.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("newTestGroupsSettingsService: %v", err)
	}
	return srv
}

func primeServiceCache(c *GoogleWorkspace, dir *directoryAdmin.Service, dt *datatransferAdmin.Service, gs *groupssettings.Service) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.serviceCache == nil {
		c.serviceCache = map[string]any{}
	}
	if dir != nil {
		c.serviceCache[directoryAdmin.AdminDirectoryUserScope] = dir
		c.serviceCache[directoryAdmin.AdminDirectoryGroupScope] = dir
		c.serviceCache[directoryAdmin.AdminDirectoryGroupReadonlyScope] = dir
		c.serviceCache[directoryAdmin.AdminDirectoryGroupMemberScope] = dir
	}
	if dt != nil {
		c.serviceCache[datatransferAdmin.AdminDatatransferScope] = dt
	}
	if gs != nil {
		c.serviceCache["https://www.googleapis.com/auth/apps.groups.settings"] = gs
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
	primeServiceCache(c, dir, nil, nil)

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
	primeServiceCache(c, dir, nil, nil)

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
	primeServiceCache(c, dir, nil, nil)

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
	primeServiceCache(c, nil, dt, nil)

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
	primeServiceCache(c, nil, dt, nil)

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

// ========== create_group Action Tests ==========

// Test 1: Success Case - Create group with valid inputs
func TestCreateGroup_Success(t *testing.T) {
	state := &testServerState{
		users:  map[string]*testUser{},
		groups: map[string]*testGroup{},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "newgroup@example.com"}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: "New Test Group"}},
		"description": {Kind: &structpb.Value_StringValue{StringValue: "A test group for unit tests"}},
	}}

	state.mtx.Lock()
	initialInsertCount := state.insertCount
	state.mtx.Unlock()

	resp, _, err := c.createGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify response values
	groupId := resp.Fields["group_id"].GetStringValue()
	if groupId == "" {
		t.Fatalf("expected group_id to be non-empty")
	}

	groupEmail := resp.Fields["group_email"].GetStringValue()
	if groupEmail != "newgroup@example.com" {
		t.Fatalf("expected group_email to be 'newgroup@example.com', got: %v", groupEmail)
	}

	groupName := resp.Fields["group_name"].GetStringValue()
	if groupName != "New Test Group" {
		t.Fatalf("expected group_name to be 'New Test Group', got: %v", groupName)
	}

	// Verify API was called once
	state.mtx.Lock()
	if state.insertCount != initialInsertCount+1 {
		t.Fatalf("expected one insert call")
	}

	// Verify group was created in state
	group := state.groups["newgroup@example.com"]
	if group == nil {
		t.Fatalf("expected group to be created")
	}
	if group.Email != "newgroup@example.com" {
		t.Fatalf("expected group email to be 'newgroup@example.com', got: %v", group.Email)
	}
	if group.Name != "New Test Group" {
		t.Fatalf("expected group name to be 'New Test Group', got: %v", group.Name)
	}
	if group.Description != "A test group for unit tests" {
		t.Fatalf("expected group description to be 'A test group for unit tests', got: %v", group.Description)
	}
	state.mtx.Unlock()
}

// Test 2: Idempotent Case - No API call when group already exists
func TestCreateGroup_Idempotent(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"existing@example.com": {
				Id:          "group_existing_at_example.com",
				Email:       "existing@example.com",
				Name:        "Existing Group",
				Description: "Already exists",
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "existing@example.com"}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: "Different Name"}},
	}}

	state.mtx.Lock()
	initialInsertCount := state.insertCount
	state.mtx.Unlock()

	resp, _, err := c.createGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify returns existing group details
	groupId := resp.Fields["group_id"].GetStringValue()
	if groupId != "group_existing_at_example.com" {
		t.Fatalf("expected existing group_id, got: %v", groupId)
	}

	groupEmail := resp.Fields["group_email"].GetStringValue()
	if groupEmail != "existing@example.com" {
		t.Fatalf("expected existing group_email, got: %v", groupEmail)
	}

	groupName := resp.Fields["group_name"].GetStringValue()
	if groupName != "Existing Group" {
		t.Fatalf("expected existing group_name 'Existing Group', got: %v", groupName)
	}

	// Verify no API call was made (CRITICAL CHECK)
	state.mtx.Lock()
	if state.insertCount != initialInsertCount {
		t.Fatalf("expected no insert call for idempotent operation")
	}
	state.mtx.Unlock()
}

// Test 3: Validation Errors - Test input validation
func TestCreateGroup_ValidationErrors(t *testing.T) {
	state := &testServerState{
		users:  map[string]*testUser{},
		groups: map[string]*testGroup{},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	// Missing group_email
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_name": {Kind: &structpb.Value_StringValue{StringValue: "Test Group"}},
	}}
	_, _, err := c.createGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing group_email")
	}
	if !strings.Contains(err.Error(), "missing group_email") {
		t.Fatalf("expected error message to contain 'missing group_email', got: %v", err)
	}

	// Missing group_name
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
	}}
	_, _, err = c.createGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing group_name")
	}
	if !strings.Contains(err.Error(), "missing group_name") {
		t.Fatalf("expected error message to contain 'missing group_name', got: %v", err)
	}

	// Empty group_email after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "   "}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: "Test Group"}},
	}}
	_, _, err = c.createGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty group_email")
	}
	if !strings.Contains(err.Error(), "group_email must be non-empty") {
		t.Fatalf("expected error message to contain 'group_email must be non-empty', got: %v", err)
	}

	// Empty group_name after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: "   "}},
	}}
	_, _, err = c.createGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty group_name")
	}
	if !strings.Contains(err.Error(), "group_name must be non-empty") {
		t.Fatalf("expected error message to contain 'group_name must be non-empty', got: %v", err)
	}

	// Invalid email format
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "invalid-email"}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: "Test Group"}},
	}}
	_, _, err = c.createGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid email format")
	}
	if !strings.Contains(err.Error(), "invalid email address") {
		t.Fatalf("expected error message to contain 'invalid email address', got: %v", err)
	}
}

// Test 4: Success Without Optional Description
func TestCreateGroup_SuccessWithoutDescription(t *testing.T) {
	state := &testServerState{
		users:  map[string]*testUser{},
		groups: map[string]*testGroup{},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: "nodesc@example.com"}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: "No Description Group"}},
	}}

	resp, _, err := c.createGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify group was created without description
	state.mtx.Lock()
	group := state.groups["nodesc@example.com"]
	if group == nil {
		t.Fatalf("expected group to be created")
	}
	if group.Description != "" {
		t.Fatalf("expected empty description, got: %v", group.Description)
	}
	state.mtx.Unlock()
}

// ========== modify_group_settings Action Tests ==========

// Test 1: Success Case - Modify group settings with all parameters
func TestModifyGroupSettings_Success(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"modify@example.com": {
				Id:                     "group_modify_at_example.com",
				Email:                  "modify@example.com",
				Name:                   "Modify Test Group",
				AllowExternalMembers:   "false",
				WhoCanPostMessage:      "ALL_MEMBERS_CAN_POST",
				MessageModerationLevel: "MODERATE_NONE",
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	gs := newTestGroupsSettingsService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, gs)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":                {Kind: &structpb.Value_StringValue{StringValue: "modify@example.com"}},
		"allow_external_members":   {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"who_can_post_message":     {Kind: &structpb.Value_StringValue{StringValue: "ANYONE_CAN_POST"}},
		"message_moderation_level": {Kind: &structpb.Value_StringValue{StringValue: "MODERATE_NON_MEMBERS"}},
	}}

	state.mtx.Lock()
	initialPutCount := state.putCount
	state.mtx.Unlock()

	resp, _, err := c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	if !resp.Fields["settings_updated"].GetBoolValue() {
		t.Fatalf("expected settings_updated to be true")
	}

	// Verify previous and new values
	if resp.Fields["previous_allow_external_members"].GetStringValue() != "false" {
		t.Fatalf("expected previous_allow_external_members to be 'false'")
	}
	if resp.Fields["new_allow_external_members"].GetStringValue() != "true" {
		t.Fatalf("expected new_allow_external_members to be 'true'")
	}

	if resp.Fields["previous_who_can_post_message"].GetStringValue() != "ALL_MEMBERS_CAN_POST" {
		t.Fatalf("expected previous_who_can_post_message to be 'ALL_MEMBERS_CAN_POST'")
	}
	if resp.Fields["new_who_can_post_message"].GetStringValue() != "ANYONE_CAN_POST" {
		t.Fatalf("expected new_who_can_post_message to be 'ANYONE_CAN_POST'")
	}

	// Verify API was called once
	state.mtx.Lock()
	if state.putCount != initialPutCount+1 {
		t.Fatalf("expected one PUT call, got %d", state.putCount-initialPutCount)
	}

	// Verify state changes
	group := state.groups["modify@example.com"]
	if group.AllowExternalMembers != "true" {
		t.Fatalf("expected AllowExternalMembers to be 'true', got: %v", group.AllowExternalMembers)
	}
	if group.WhoCanPostMessage != "ANYONE_CAN_POST" {
		t.Fatalf("expected WhoCanPostMessage to be 'ANYONE_CAN_POST', got: %v", group.WhoCanPostMessage)
	}
	if group.MessageModerationLevel != "MODERATE_NON_MEMBERS" {
		t.Fatalf("expected MessageModerationLevel to be 'MODERATE_NON_MEMBERS', got: %v", group.MessageModerationLevel)
	}
	state.mtx.Unlock()
}

// Test 2: Idempotent Case - No update when already at target
func TestModifyGroupSettings_Idempotent(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"idempotent@example.com": {
				Id:                     "group_idempotent_at_example.com",
				Email:                  "idempotent@example.com",
				Name:                   "Idempotent Test Group",
				AllowExternalMembers:   "true",
				WhoCanPostMessage:      "ANYONE_CAN_POST",
				MessageModerationLevel: "MODERATE_NON_MEMBERS",
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	gs := newTestGroupsSettingsService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, gs)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":                {Kind: &structpb.Value_StringValue{StringValue: "idempotent@example.com"}},
		"allow_external_members":   {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"who_can_post_message":     {Kind: &structpb.Value_StringValue{StringValue: "ANYONE_CAN_POST"}},
		"message_moderation_level": {Kind: &structpb.Value_StringValue{StringValue: "MODERATE_NON_MEMBERS"}},
	}}

	state.mtx.Lock()
	initialPutCount := state.putCount
	state.mtx.Unlock()

	resp, _, err := c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify no settings were updated (idempotent)
	if resp.Fields["settings_updated"].GetBoolValue() {
		t.Fatalf("expected settings_updated to be false for idempotent operation")
	}

	// Verify no API call was made (CRITICAL CHECK)
	state.mtx.Lock()
	if state.putCount != initialPutCount {
		t.Fatalf("expected no PUT call for idempotent operation")
	}
	state.mtx.Unlock()
}

// Test 3: Validation Errors
func TestModifyGroupSettings_ValidationErrors(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"test@example.com": {
				Id:    "group_test_at_example.com",
				Email: "test@example.com",
				Name:  "Test Group",
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	gs := newTestGroupsSettingsService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, gs)

	// Missing group_key
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"who_can_post_message": {Kind: &structpb.Value_StringValue{StringValue: "ANYONE_CAN_POST"}},
	}}
	_, _, err := c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing group_key")
	}
	if !strings.Contains(err.Error(), "missing group_key") {
		t.Fatalf("expected error message to contain 'missing group_key', got: %v", err)
	}

	// Empty group_key after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":            {Kind: &structpb.Value_StringValue{StringValue: "   "}},
		"who_can_post_message": {Kind: &structpb.Value_StringValue{StringValue: "ANYONE_CAN_POST"}},
	}}
	_, _, err = c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty group_key")
	}
	if !strings.Contains(err.Error(), "group_key must be non-empty") {
		t.Fatalf("expected error message to contain 'group_key must be non-empty', got: %v", err)
	}

	// No settings parameters provided
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
	}}
	_, _, err = c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error when no settings parameters provided")
	}
	if !strings.Contains(err.Error(), "at least one settings parameter must be provided") {
		t.Fatalf("expected error message to contain 'at least one settings parameter must be provided', got: %v", err)
	}

	// Invalid who_can_post_message
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":            {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
		"who_can_post_message": {Kind: &structpb.Value_StringValue{StringValue: "INVALID_VALUE"}},
	}}
	_, _, err = c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid who_can_post_message")
	}
	if !strings.Contains(err.Error(), "invalid who_can_post_message value") {
		t.Fatalf("expected error message to contain 'invalid who_can_post_message value', got: %v", err)
	}

	// Invalid message_moderation_level
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":                {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
		"message_moderation_level": {Kind: &structpb.Value_StringValue{StringValue: "INVALID_LEVEL"}},
	}}
	_, _, err = c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid message_moderation_level")
	}
	if !strings.Contains(err.Error(), "invalid message_moderation_level value") {
		t.Fatalf("expected error message to contain 'invalid message_moderation_level value', got: %v", err)
	}
}

// Test 4: Group Not Found
func TestModifyGroupSettings_GroupNotFound(t *testing.T) {
	state := &testServerState{
		users:  map[string]*testUser{},
		groups: map[string]*testGroup{},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	gs := newTestGroupsSettingsService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, gs)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":            {Kind: &structpb.Value_StringValue{StringValue: "nonexistent@example.com"}},
		"who_can_post_message": {Kind: &structpb.Value_StringValue{StringValue: "ANYONE_CAN_POST"}},
	}}

	_, _, err := c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for group not found")
	}
	if !strings.Contains(err.Error(), "group not found") {
		t.Fatalf("expected error message to contain 'group not found', got: %v", err)
	}
}

// Test 5: Partial Settings Update - Only one setting provided
func TestModifyGroupSettings_PartialUpdate(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"partial@example.com": {
				Id:                     "group_partial_at_example.com",
				Email:                  "partial@example.com",
				Name:                   "Partial Update Group",
				AllowExternalMembers:   "false",
				WhoCanPostMessage:      "ALL_MEMBERS_CAN_POST",
				MessageModerationLevel: "MODERATE_NONE",
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	gs := newTestGroupsSettingsService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, gs)

	// Only update who_can_post_message
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":            {Kind: &structpb.Value_StringValue{StringValue: "partial@example.com"}},
		"who_can_post_message": {Kind: &structpb.Value_StringValue{StringValue: "ALL_OWNERS_CAN_POST"}},
	}}

	state.mtx.Lock()
	initialPutCount := state.putCount
	state.mtx.Unlock()

	resp, _, err := c.modifyGroupSettingsActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	if !resp.Fields["settings_updated"].GetBoolValue() {
		t.Fatalf("expected settings_updated to be true")
	}

	// Verify only who_can_post_message was updated
	if resp.Fields["previous_who_can_post_message"].GetStringValue() != "ALL_MEMBERS_CAN_POST" {
		t.Fatalf("expected previous_who_can_post_message to be 'ALL_MEMBERS_CAN_POST'")
	}
	if resp.Fields["new_who_can_post_message"].GetStringValue() != "ALL_OWNERS_CAN_POST" {
		t.Fatalf("expected new_who_can_post_message to be 'ALL_OWNERS_CAN_POST'")
	}

	// Verify other settings fields are not in response
	if _, ok := resp.Fields["previous_allow_external_members"]; ok {
		t.Fatalf("expected previous_allow_external_members to not be in response")
	}
	if _, ok := resp.Fields["previous_message_moderation_level"]; ok {
		t.Fatalf("expected previous_message_moderation_level to not be in response")
	}

	// Verify API was called once
	state.mtx.Lock()
	if state.putCount != initialPutCount+1 {
		t.Fatalf("expected one PUT call")
	}

	// Verify only who_can_post_message changed
	group := state.groups["partial@example.com"]
	if group.WhoCanPostMessage != "ALL_OWNERS_CAN_POST" {
		t.Fatalf("expected WhoCanPostMessage to be 'ALL_OWNERS_CAN_POST', got: %v", group.WhoCanPostMessage)
	}
	// Other settings should remain unchanged
	if group.AllowExternalMembers != "false" {
		t.Fatalf("expected AllowExternalMembers to remain 'false', got: %v", group.AllowExternalMembers)
	}
	if group.MessageModerationLevel != "MODERATE_NONE" {
		t.Fatalf("expected MessageModerationLevel to remain 'MODERATE_NONE', got: %v", group.MessageModerationLevel)
	}
	state.mtx.Unlock()
}

// ========== add_user_to_group Action Tests ==========

// Test 1: Success Case - Add new user to group successfully
func TestAddUserToGroup_Success(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"team@example.com": {
				Id:      "group_team_at_example.com",
				Email:   "team@example.com",
				Name:    "Team Group",
				Members: make(map[string]string),
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "alice@example.com"}},
		"role":       {Kind: &structpb.Value_StringValue{StringValue: "MEMBER"}},
	}}

	state.mtx.Lock()
	initialPostCount := state.postCount
	state.mtx.Unlock()

	resp, _, err := c.addUserToGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify response values
	if resp.Fields["group_email"].GetStringValue() != "team@example.com" {
		t.Fatalf("expected group_email to be 'team@example.com'")
	}
	if resp.Fields["user_email"].GetStringValue() != "alice@example.com" {
		t.Fatalf("expected user_email to be 'alice@example.com'")
	}
	if resp.Fields["role"].GetStringValue() != "MEMBER" {
		t.Fatalf("expected role to be 'MEMBER'")
	}
	if resp.Fields["already_member"].GetBoolValue() {
		t.Fatalf("expected already_member to be false")
	}

	// Verify API was called once
	state.mtx.Lock()
	if state.postCount != initialPostCount+1 {
		t.Fatalf("expected one POST call")
	}

	// Verify member was added
	group := state.groups["team@example.com"]
	if role, exists := group.Members["alice@example.com"]; !exists {
		t.Fatalf("expected user to be added to group")
	} else if role != "MEMBER" {
		t.Fatalf("expected role to be 'MEMBER', got: %v", role)
	}
	state.mtx.Unlock()
}

// Test 2: Idempotent Case - User already in group with same role (no API call)
func TestAddUserToGroup_Idempotent(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"team@example.com": {
				Id:    "group_team_at_example.com",
				Email: "team@example.com",
				Name:  "Team Group",
				Members: map[string]string{
					"bob@example.com": "MEMBER",
				},
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "bob@example.com"}},
		"role":       {Kind: &structpb.Value_StringValue{StringValue: "MEMBER"}},
	}}

	state.mtx.Lock()
	initialPostCount := state.postCount
	initialPutCount := state.putCount
	state.mtx.Unlock()

	resp, _, err := c.addUserToGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify already_member is true
	if !resp.Fields["already_member"].GetBoolValue() {
		t.Fatalf("expected already_member to be true")
	}

	// Verify no API call was made (CRITICAL CHECK)
	state.mtx.Lock()
	if state.postCount != initialPostCount {
		t.Fatalf("expected no POST call for idempotent operation")
	}
	if state.putCount != initialPutCount {
		t.Fatalf("expected no PUT call for idempotent operation")
	}
	state.mtx.Unlock()
}

// Test 3: Update Role - User already in group but different role (update it)
func TestAddUserToGroup_UpdateRole(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"team@example.com": {
				Id:    "group_team_at_example.com",
				Email: "team@example.com",
				Name:  "Team Group",
				Members: map[string]string{
					"charlie@example.com": "MEMBER",
				},
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "charlie@example.com"}},
		"role":       {Kind: &structpb.Value_StringValue{StringValue: "MANAGER"}},
	}}

	state.mtx.Lock()
	initialPostCount := state.postCount
	initialPutCount := state.putCount
	state.mtx.Unlock()

	resp, _, err := c.addUserToGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify already_member is true
	if !resp.Fields["already_member"].GetBoolValue() {
		t.Fatalf("expected already_member to be true")
	}

	// Verify role was updated
	if resp.Fields["role"].GetStringValue() != "MANAGER" {
		t.Fatalf("expected role to be 'MANAGER'")
	}

	// Verify PUT was called but not POST
	state.mtx.Lock()
	if state.postCount != initialPostCount {
		t.Fatalf("expected no POST call when updating role")
	}
	if state.putCount != initialPutCount+1 {
		t.Fatalf("expected one PUT call to update role")
	}

	// Verify role was updated in state
	group := state.groups["team@example.com"]
	if role, exists := group.Members["charlie@example.com"]; !exists {
		t.Fatalf("expected user to still be in group")
	} else if role != "MANAGER" {
		t.Fatalf("expected role to be updated to 'MANAGER', got: %v", role)
	}
	state.mtx.Unlock()
}

// Test 4: Validation Errors
func TestAddUserToGroup_ValidationErrors(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"team@example.com": {
				Id:      "group_team_at_example.com",
				Email:   "team@example.com",
				Name:    "Team Group",
				Members: make(map[string]string),
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	// Missing group_key
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
	}}
	_, _, err := c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing group_key")
	}
	if !strings.Contains(err.Error(), "missing group_key") {
		t.Fatalf("expected error message to contain 'missing group_key', got: %v", err)
	}

	// Missing user_email
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key": {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
	}}
	_, _, err = c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for missing user_email")
	}
	if !strings.Contains(err.Error(), "missing user_email") {
		t.Fatalf("expected error message to contain 'missing user_email', got: %v", err)
	}

	// Empty group_key after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "   "}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
	}}
	_, _, err = c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty group_key")
	}
	if !strings.Contains(err.Error(), "group_key must be non-empty") {
		t.Fatalf("expected error message to contain 'group_key must be non-empty', got: %v", err)
	}

	// Empty user_email after trimming
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "   "}},
	}}
	_, _, err = c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for empty user_email")
	}
	if !strings.Contains(err.Error(), "user_email must be non-empty") {
		t.Fatalf("expected error message to contain 'user_email must be non-empty', got: %v", err)
	}

	// Invalid email format
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "invalid-email"}},
	}}
	_, _, err = c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid email format")
	}
	if !strings.Contains(err.Error(), "invalid email address") {
		t.Fatalf("expected error message to contain 'invalid email address', got: %v", err)
	}

	// Invalid role
	args = &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
		"role":       {Kind: &structpb.Value_StringValue{StringValue: "INVALID_ROLE"}},
	}}
	_, _, err = c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for invalid role")
	}
	if !strings.Contains(err.Error(), "invalid role") {
		t.Fatalf("expected error message to contain 'invalid role', got: %v", err)
	}
}

// Test 5: Group Not Found
func TestAddUserToGroup_GroupNotFound(t *testing.T) {
	state := &testServerState{
		users:  map[string]*testUser{},
		groups: map[string]*testGroup{},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "nonexistent@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "test@example.com"}},
	}}

	_, _, err := c.addUserToGroupActionHandler(context.Background(), args)
	if err == nil {
		t.Fatalf("expected error for group not found")
	}
	if !strings.Contains(err.Error(), "group not found") {
		t.Fatalf("expected error message to contain 'group not found', got: %v", err)
	}
}

// Test 6: Default Role - Role defaults to MEMBER when not provided
func TestAddUserToGroup_DefaultRole(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{},
		groups: map[string]*testGroup{
			"team@example.com": {
				Id:      "group_team_at_example.com",
				Email:   "team@example.com",
				Name:    "Team Group",
				Members: make(map[string]string),
			},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil, nil)

	// Don't provide role parameter
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"group_key":  {Kind: &structpb.Value_StringValue{StringValue: "team@example.com"}},
		"user_email": {Kind: &structpb.Value_StringValue{StringValue: "david@example.com"}},
	}}

	resp, _, err := c.addUserToGroupActionHandler(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !resp.Fields["success"].GetBoolValue() {
		t.Fatalf("expected success to be true")
	}

	// Verify role defaults to MEMBER
	if resp.Fields["role"].GetStringValue() != "MEMBER" {
		t.Fatalf("expected role to default to 'MEMBER', got: %v", resp.Fields["role"].GetStringValue())
	}

	// Verify member was added with MEMBER role
	state.mtx.Lock()
	group := state.groups["team@example.com"]
	if role, exists := group.Members["david@example.com"]; !exists {
		t.Fatalf("expected user to be added to group")
	} else if role != "MEMBER" {
		t.Fatalf("expected default role to be 'MEMBER', got: %v", role)
	}
	state.mtx.Unlock()
}
