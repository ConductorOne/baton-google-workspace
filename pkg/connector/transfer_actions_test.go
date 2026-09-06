package connector

// Transfer correlation and saved-operation observation fixtures.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func getTransferArgs(appID string, levels []string, release *structpb.Value) *structpb.Struct {
	fields := map[string]*structpb.Value{
		"transfer_id":        {Kind: &structpb.Value_StringValue{StringValue: "tr_x"}},
		"resource_id":        {Kind: &structpb.Value_StringValue{StringValue: "src"}},
		"target_resource_id": {Kind: &structpb.Value_StringValue{StringValue: "dst"}},
		"application_id":     {Kind: &structpb.Value_StringValue{StringValue: appID}},
	}
	if levels != nil {
		lv := &structpb.ListValue{}
		for _, l := range levels {
			lv.Values = append(lv.Values, &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: l}})
		}
		fields["privacy_levels"] = &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: lv}}
	}
	if release != nil {
		fields["release_resources"] = release
	}
	return &structpb.Struct{Fields: fields}
}

// TestGetUserDataTransfer_ReadsAndVerifies covers the saved-operation read against
// a fake provider: matched Drive transfer returns success with completed
// derived from the provider status; mismatched identity/params return
// FailedPrecondition WITH the observed record; wrong-application arguments
// are rejected at the schema level.
func TestGetUserDataTransfer_ReadsAndVerifies(t *testing.T) {
	state := &testServerState{
		users: map[string]*testUser{"src": {}, "dst": {}},
		transfers: []*transferRecord{{
			Id: "tr_x", OldOwner: "src", NewOwner: "dst",
			AppID:  appIdGoogleDocsAndGoogleDrive,
			Status: "inProgress",
			Params: map[string][]string{"PRIVACY_LEVEL": {"PRIVATE", "SHARED"}},
		}},
	}
	getServer := newTransferGetServer(state)
	defer getServer.Close()
	dt2 := newTestDataTransferService(t, getServer.URL, getServer.Client())
	c2 := newTestConnector()
	primeServiceCache(c2, nil, dt2)

	// Matched: success, not completed, per-app status present.
	resp, _, err := c2.getUserDataTransfer(context.Background(), getTransferArgs("55656082996", []string{"private", "shared"}, nil))
	if err != nil {
		t.Fatalf("matched read must succeed: %v", err)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true")
	}
	if resp.GetFields()["completed"].GetBoolValue() {
		t.Fatalf("inProgress is not completed")
	}
	if got := resp.GetFields()["status"].GetStringValue(); got != "inProgress" {
		t.Fatalf("expected provider status inProgress, got %q", got)
	}
	perApp := resp.GetFields()["per_application_status"].GetStructValue()
	if perApp.GetFields()["55656082996"] == nil {
		t.Fatalf("expected per-application status entry")
	}

	// Param mismatch: FailedPrecondition with observed data retained.
	_, _, err = c2.getUserDataTransfer(context.Background(), getTransferArgs("55656082996", []string{"shared"}, nil))
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("param mismatch must be FailedPrecondition, got %v", err)
	}

	// Wrong application for the parameter set: release_resources on Drive is rejected.
	_, _, err = c2.getUserDataTransfer(context.Background(), getTransferArgs("55656082996", []string{"private"}, &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: true}}))
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("release_resources on Drive must be InvalidArgument, got %v", err)
	}

	// Calendar without release_resources is rejected.
	_, _, err = c2.getUserDataTransfer(context.Background(), getTransferArgs("435070579839", nil, nil))
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Calendar without release_resources must be InvalidArgument, got %v", err)
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
