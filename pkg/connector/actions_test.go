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
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

type testUser struct {
	Suspended    bool
	PrimaryEmail string
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

func TestLockUnlock_IdempotentAndPayload(t *testing.T) {
	state := &testServerState{users: map[string]*testUser{"alice": {Suspended: false, PrimaryEmail: "alice@example.com"}}}
	server := newTestServer(state)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, dir, nil)

	// lock user
	args := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id": {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
	}}
	if _, _, err := c.lockUser(context.Background(), args); err != nil {
		t.Fatalf("lockUser: %v", err)
	}
	if !state.users["alice"].Suspended {
		t.Fatalf("expected alice to be suspended")
	}
	// call again should not PUT again
	prevPut := state.putCount
	if _, _, err := c.lockUser(context.Background(), args); err != nil {
		t.Fatalf("lockUser second: %v", err)
	}
	if state.putCount != prevPut {
		t.Fatalf("expected no additional PUT on idempotent lock, got %d vs %d", state.putCount, prevPut)
	}

	// unlock user
	argsUnlock := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id": {Kind: &structpb.Value_StringValue{StringValue: "alice"}},
	}}
	if _, _, err := c.unlockUser(context.Background(), argsUnlock); err != nil {
		t.Fatalf("unlockUser: %v", err)
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
