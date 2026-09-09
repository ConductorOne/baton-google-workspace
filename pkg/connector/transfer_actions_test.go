package connector

// Transfer correlation and saved-operation observation fixtures.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func driveTransferArgs() *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":        {Kind: &structpb.Value_StringValue{StringValue: "src"}},
		"target_resource_id": {Kind: &structpb.Value_StringValue{StringValue: "dst"}},
	}}
}

func TestTransferDiscoveryCannotInsertAfterTruncation(t *testing.T) {
	var reads, writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			writes.Add(1)
			http.Error(w, "unexpected mutation", http.StatusBadRequest)
			return
		}
		page := reads.Add(1)
		if page > 10 {
			http.Error(w, "discovery exceeded its limit", http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"nextPageToken": strconv.Itoa(int(page))}); err != nil {
			t.Errorf("encoding transfer page: %v", err)
		}
	}))
	defer server.Close()
	connector := newTestConnector()
	primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
	result, _, err := connector.transferUserDriveFiles(t.Context(), driveTransferArgs())
	if status.Code(err) != codes.FailedPrecondition || result != nil {
		t.Fatalf("incomplete discovery must fail without a result: result=%v error=%v", result, err)
	}
	if writes.Load() != 0 || reads.Load() == 0 || reads.Load() > 10 {
		t.Fatalf("incomplete discovery must stay bounded and never mutate: reads=%d writes=%d", reads.Load(), writes.Load())
	}
}

// TestTransferDrive_DifferentParams_NoSecondPost seeds an ongoing transfer
// with PRIVACY_LEVEL=[SHARED] only. A request for private+shared (the default)
// must NOT adopt it and must NOT start a duplicate transfer either: it fails
// with FailedPrecondition naming the conflicting transfer.
func TestTransferDrive_DifferentParams_NoSecondPost(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_shared", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "inProgress",
			Params: map[string][]string{"PRIVACY_LEVEL": {"SHARED"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	_, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err == nil {
		t.Fatalf("conflicting ongoing transfer must fail, not adopt and not POST")
	}
	if st := status.Code(err); st != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", st, err)
	}
	if !strings.Contains(err.Error(), "tr_shared") {
		t.Fatalf("error must name the conflicting transfer id, got: %v", err)
	}
	if state.postCount != 0 {
		t.Fatalf("expected no new POST on conflict, got %d", state.postCount)
	}
}

// TestTransferDrive_DuplicateMatches_NoSecondPost seeds TWO identical
// parameter-matched ongoing transfers. Ambiguity must fail explicitly with
// both IDs; no adoption and no POST.
func TestTransferDrive_DuplicateMatches_NoSecondPost(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{
			{Id: "tr_a", OldOwner: "src", NewOwner: "dst", AppID: appIdGoogleDocsAndGoogleDrive, Status: "inProgress", Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}}},
			{Id: "tr_b", OldOwner: "src", NewOwner: "dst", AppID: appIdGoogleDocsAndGoogleDrive, Status: "new", Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}}},
		},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	_, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err == nil {
		t.Fatalf("ambiguous duplicate matches must fail")
	}
	if st := status.Code(err); st != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", st, err)
	}
	if !strings.Contains(err.Error(), "tr_a") || !strings.Contains(err.Error(), "tr_b") {
		t.Fatalf("error must name both candidate ids, got: %v", err)
	}
	if state.postCount != 0 {
		t.Fatalf("expected no POST on ambiguity, got %d", state.postCount)
	}
}

// TestTransferDrive_SingleMatchAdopted_NoPost seeds exactly one
// parameter-matched ongoing transfer; it is adopted with no new POST.
func TestTransferDrive_SingleMatchAdopted_NoPost(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_match", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "inProgress",
			Params: map[string][]string{"PRIVACY_LEVEL": {"SHARED", "PRIVATE"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	resp, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err != nil {
		t.Fatalf("single exact match must be adopted: %v", err)
	}
	if id := resp.GetFields()["transfer_id"].GetStringValue(); id != "tr_match" {
		t.Fatalf("expected adopted id tr_match, got %q", id)
	}
	if state.postCount != 0 {
		t.Fatalf("expected no POST on adoption, got %d", state.postCount)
	}
}

// TestTransferDrive_CompletedNotAdopted seeds a COMPLETED transfer with
// matching params; it must not be adopted, and since no ongoing transfer
// exists a new one is created.
func TestTransferDrive_CompletedNotAdopted(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_done", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "completed",
			Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	resp, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err != nil {
		t.Fatalf("completed history must not block a fresh request: %v", err)
	}
	if id := resp.GetFields()["transfer_id"].GetStringValue(); id == "tr_done" {
		t.Fatalf("completed transfer must not be adopted as this operation")
	}
	if state.postCount != 1 {
		t.Fatalf("expected fresh POST, got %d", state.postCount)
	}
}

// An unidentified ongoing operation is not absence and must not trigger an insert.
func TestTransferDrive_EmptyIdentityRecordNotAdopted(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "inProgress",
			Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	_, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err == nil {
		t.Fatal("incomplete transfer identity must remain an unknown outcome")
	}
	if state.postCount != 0 {
		t.Fatalf("expected no POST after incomplete discovery, got %d", state.postCount)
	}
}

// TestTransferCalendar_ReleaseResourcesParamMatch seeds an ongoing Calendar
// transfer WITH RELEASE_RESOURCES=TRUE; a request with release_resources=true
// must adopt it (no POST), while release_resources=false must conflict.
func TestTransferCalendar_ReleaseResourcesParamMatch(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_cal", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleCalendar,
			Status: "inProgress",
			Params: map[string][]string{"RELEASE_RESOURCES": {"TRUE"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	matchArgs := &structpb.Struct{Fields: map[string]*structpb.Value{
		"resource_id":        {Kind: &structpb.Value_StringValue{StringValue: "src"}},
		"target_resource_id": {Kind: &structpb.Value_StringValue{StringValue: "dst"}},
		"release_resources":  {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	resp, _, err := c.transferUserCalendar(context.Background(), matchArgs)
	if err != nil {
		t.Fatalf("matching release_resources must adopt: %v", err)
	}
	if id := resp.GetFields()["transfer_id"].GetStringValue(); id != "tr_cal" {
		t.Fatalf("expected adopted id tr_cal, got %q", id)
	}
	if state.postCount != 0 {
		t.Fatalf("expected no POST on matched adoption, got %d", state.postCount)
	}
}

func getTransferArgs() *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"transfer_id": structpb.NewStringValue("tr_x"),
	}}
}

func TestGetUserDataTransfer_ReadsProviderState(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_x", OldOwner: "src", NewOwner: "dst",
			AppID: appIdGoogleDocsAndGoogleDrive, Status: "inProgress",
		}},
	}
	server := newTransferGetServer(state)
	defer server.Close()
	c := newTestConnector()
	primeServiceCache(c, nil, newTestDataTransferService(t, server.URL, server.Client()))

	result, _, err := c.getUserDataTransfer(context.Background(), getTransferArgs())
	if err != nil {
		t.Fatalf("read transfer: %v", err)
	}
	if !result.GetFields()[fieldSuccess].GetBoolValue() {
		t.Fatal("expected successful provider read")
	}
	if result.GetFields()[argResourceID].GetStringValue() != "src" || result.GetFields()[argTargetResourceID].GetStringValue() != "dst" {
		t.Fatalf("expected provider owner IDs, got %v", result.AsMap())
	}
	if result.GetFields()[fieldStatus].GetStringValue() != "inProgress" {
		t.Fatalf("expected actual provider status, got %q", result.GetFields()[fieldStatus].GetStringValue())
	}
	if result.GetFields()[fieldPerAppStatus].GetStructValue().GetFields()["55656082996"] == nil {
		t.Fatal("expected provider application status")
	}
}

func TestGetUserDataTransferDoesNotHideReadFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		code   codes.Code
	}{
		{"permission denied", http.StatusForbidden, `{"error":{"code":403,"message":"denied"}}`, codes.PermissionDenied},
		{"different transfer", http.StatusOK, `{"id":"other-transfer","oldOwnerUserId":"other-user"}`, codes.FailedPrecondition},
		{"invalid application data", http.StatusOK, `{"id":"tr_x","applicationDataTransfers":[null]}`, codes.DataLoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("transfer lookup attempted mutation: %s", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			connector := newTestConnector()
			primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
			result, _, err := connector.getUserDataTransfer(t.Context(), getTransferArgs())
			if status.Code(err) != tc.code || result != nil {
				t.Fatalf("expected failed lookup with %v and no result, got result=%v error=%v", tc.code, result, err)
			}
		})
	}
}

// jsonEncodeTransfer writes a DataTransfer JSON body for the get-by-id fake.
func jsonEncodeTransfer(w http.ResponseWriter, tr *transferRecord, params []*datatransferAdmin.ApplicationTransferParam) error {
	resp := &datatransferAdmin.DataTransfer{
		Id:                        tr.Id,
		OldOwnerUserId:            tr.OldOwner,
		NewOwnerUserId:            tr.NewOwner,
		OverallTransferStatusCode: tr.Status,
		ApplicationDataTransfers: []*datatransferAdmin.ApplicationDataTransfer{{
			ApplicationId:             tr.AppID,
			ApplicationTransferStatus: tr.Status,
			ApplicationTransferParams: params,
		}},
	}
	return json.NewEncoder(w).Encode(resp)
}

// newTransferGetServer serves only GET /transfers/{id} from the shared state.
func newTransferGetServer(state *testServerState) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "read only", http.StatusMethodNotAllowed)
			return
		}
		state.mtx.Lock()
		defer state.mtx.Unlock()
		for _, tr := range state.transfers {
			if tr.Id == "tr_x" {
				params := make([]*datatransferAdmin.ApplicationTransferParam, 0, len(tr.Params))
				for k, vs := range tr.Params {
					params = append(params, &datatransferAdmin.ApplicationTransferParam{Key: k, Value: vs})
				}
				_ = jsonEncodeTransfer(w, tr, params)
				return
			}
		}
		http.Error(w, `{"error":{"code":404,"message":"not found"}}`, http.StatusNotFound)
	}))
}

// TestTransferInsert_UnrelatedAppUnknownStatusDoesNotBlock seeds an ongoing
// transfer for a DIFFERENT application whose overall status is unrecognized.
// The requested application has no transfer, so the unrelated record must not
// block: a fresh insert proceeds.
func TestTransferInsert_UnrelatedAppUnknownStatusDoesNotBlock(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_other", OldOwner: "src", NewOwner: "dst",
			AppID:  999999999, // unrelated application
			Status: "exoticUnknownStatus",
			Params: map[string][]string{"SOMETHING": {"ELSE"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	resp, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err != nil {
		t.Fatalf("unrelated application's unknown status must not block the insert: %v", err)
	}
	if id := resp.GetFields()["transfer_id"].GetStringValue(); id == "tr_other" {
		t.Fatalf("the unrelated transfer must not be adopted")
	}
	state.mtx.Lock()
	postCount := state.postCount
	state.mtx.Unlock()
	if postCount != 1 {
		t.Fatalf("expected a fresh insert, got %d POSTs", postCount)
	}
}

// TestTransferInsert_RelevantAppUnknownStatusStillRefuses seeds an ongoing
// transfer FOR THE REQUESTED APPLICATION with an unrecognized overall status:
// the insert must refuse rather than adopt or blindly continue.
func TestTransferInsert_RelevantAppUnknownStatusStillRefuses(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_ours", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "exoticUnknownStatus",
			Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	_, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err == nil {
		t.Fatalf("a covering transfer with unknown status must refuse the insert")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", status.Code(err), err)
	}
	state.mtx.Lock()
	postCount := state.postCount
	state.mtx.Unlock()
	if postCount != 0 {
		t.Fatalf("refusal must not insert, got %d POSTs", postCount)
	}
}

// TestTransferInsert_PendingIsOngoingNotCompleted seeds a covering ongoing
// transfer with status "pending": it is adoptable (no second POST) and is
// never treated as completed by the read path.
func TestTransferInsert_PendingIsOngoingNotCompleted(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_pending", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "pending",
			Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}},
		}},
	}
	server := newTestServer(state)
	defer server.Close()

	dt := newTestDataTransferService(t, server.URL, server.Client())
	c := newTestConnector()
	primeServiceCache(c, nil, dt)

	resp, _, err := c.transferUserDriveFiles(context.Background(), driveTransferArgs())
	if err != nil {
		t.Fatalf("pending is an ongoing status and must adopt: %v", err)
	}
	if id := resp.GetFields()["transfer_id"].GetStringValue(); id != "tr_pending" {
		t.Fatalf("expected adoption of tr_pending, got %q", id)
	}
	state.mtx.Lock()
	postCount := state.postCount
	state.mtx.Unlock()
	if postCount != 0 {
		t.Fatalf("adoption must not insert, got %d POSTs", postCount)
	}
	if resp.GetFields()["status"].GetStringValue() != "pending" {
		t.Fatalf("expected pending status passthrough")
	}
}
