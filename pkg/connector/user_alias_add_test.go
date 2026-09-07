package connector

// Focused fixtures for the add_user_alias resource action (frozen contract:
// registration, successful insert/readback, same-account replay, foreign-user
// and group conflict, primary/precondition/malformed rejection, permission
// and unknown post-state).
//
// Test-server rules follow user_alias_actions_test.go: handlers never call
// require/FailNow; all shared state is guarded by state.mtx and mutated
// atomically, so concurrent reads under the handler's read-read-insert-read
// flow observe consistent snapshots.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
	"github.com/conductorone/baton-sdk/pkg/actions"
)

type addAliasTestUser struct {
	Id                 string
	PrimaryEmail       string
	CustomerId         string
	Aliases            []string
	NonEditableAliases []string
}

type addAliasTestState struct {
	mtx        sync.Mutex
	users      map[string]*addAliasTestUser
	groups     map[string]bool // group primary/alias emails that own a namespace
	inserts    int
	mutations  int // counts ALL provider mutations: POST/PUT/PATCH/DELETE
	deleteHits int
	patchHits  int
	putHits    int
	// insertFailStatus, when set, makes the alias INSERT return that HTTP
	// status instead of succeeding (409 also has dedicated group/foreign
	// handling via aliasOwners).
	insertFailStatus int
	// readbackFailAfterInsert, when true, makes GET/aliases reads fail
	// with 403 once at least one insert has been attempted.
	readbackFailAfterInsert bool
}

func newAddAliasTestServer(state *addAliasTestState) *httptest.Server {
	mux := http.NewServeMux()

	writeErr := func(w http.ResponseWriter, code int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, fmt.Sprintf(`{"error":{"code":%d,"message":"%s"}}`, code, msg), code)
	}
	serveUser := func(w http.ResponseWriter, u *addAliasTestUser) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":                 u.Id,
			"primaryEmail":       u.PrimaryEmail,
			"customerId":         u.CustomerId,
			"aliases":            u.Aliases,
			"nonEditableAliases": u.NonEditableAliases,
		})
	}

	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		state.mtx.Lock()
		defer state.mtx.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		parts := strings.Split(path, "/")
		// Count every provider mutation attempt regardless of outcome, so no
		// moving/removing fallback can hide behind an insert-only counter.
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
			state.mutations++
			if r.Method == http.MethodPut {
				state.putHits++
			}
			if r.Method == http.MethodPatch {
				state.patchHits++
			}
			if r.Method == http.MethodDelete {
				state.deleteHits++
			}
		}

		if state.readbackFailAfterInsert && state.inserts > 0 && r.Method == http.MethodGet {
			writeErr(w, http.StatusForbidden, "readback denied")
			return
		}

		// GET /users/{userKey} — stable ID or email/alias lookup.
		if len(parts) == 1 && r.Method == http.MethodGet {
			u := state.users[parts[0]]
			if u == nil {
				for _, candidate := range state.users {
					if strings.EqualFold(candidate.PrimaryEmail, parts[0]) || aliasListHas(candidate.Aliases, parts[0]) {
						u = candidate
						break
					}
				}
			}
			if u == nil {
				writeErr(w, http.StatusNotFound, "Resource Not Found: userKey")
				return
			}
			serveUser(w, u)
			return
		}

		// GET /users/{userKey}/aliases — dedicated alias collection.
		if len(parts) == 2 && parts[1] == "aliases" && r.Method == http.MethodGet {
			u := state.users[parts[0]]
			if u == nil {
				writeErr(w, http.StatusNotFound, "user not found")
				return
			}
			aliases := make([]map[string]string, 0, len(u.Aliases))
			for _, alias := range u.Aliases {
				aliases = append(aliases, map[string]string{"alias": alias})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"aliases": aliases})
			return
		}

		// POST /users/{userKey}/aliases — insert.
		if len(parts) == 2 && parts[1] == "aliases" && r.Method == http.MethodPost {
			state.inserts++
			if state.insertFailStatus != 0 {
				var forcedBody struct {
					Alias string `json:"alias"`
				}
				if err := json.NewDecoder(r.Body).Decode(&forcedBody); err != nil {
					writeErr(w, http.StatusBadRequest, "invalid alias request")
					return
				}
				if state.insertFailStatus == http.StatusConflict {
					// A concurrent duplicate landed on the same target just
					// before the conflict was returned.
					if u := state.users[parts[0]]; u != nil && !aliasListHas(u.Aliases, forcedBody.Alias) {
						u.Aliases = append(u.Aliases, forcedBody.Alias)
					}
				}
				writeErr(w, state.insertFailStatus, "forced insert failure")
				return
			}
			u := state.users[parts[0]]
			if u == nil {
				writeErr(w, http.StatusNotFound, "Resource Not Found: userKey")
				return
			}
			var body struct {
				Alias string `json:"alias"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid alias request")
				return
			}
			// Authoritative provider conflict: another user owns the alias,
			// or a group/namespace owns the address.
			for _, candidate := range state.users {
				if candidate != u && (strings.EqualFold(candidate.PrimaryEmail, body.Alias) || aliasListHas(candidate.Aliases, body.Alias)) {
					writeErr(w, http.StatusConflict, "Duplicate Alias")
					return
				}
			}
			if state.groups[strings.ToLower(body.Alias)] {
				writeErr(w, http.StatusConflict, "Duplicate Alias: group or namespace")
				return
			}
			u.Aliases = append(u.Aliases, body.Alias)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "admin#directory#alias", "alias": body.Alias, "primaryEmail": u.PrimaryEmail})
			return
		}

		writeErr(w, http.StatusNotFound, "not found")
	})

	return httptest.NewServer(mux)
}

func newAddAliasTestResourceType(t *testing.T, server *httptest.Server) *userResourceType {
	t.Helper()
	dir := newTestDirectoryService(t, server.URL, server.Client())
	return &userResourceType{
		resourceType: resourceTypeUser,
		client: &gwclient.GoogleWorkspaceClient{
			UserProvisioningService: dir,
		},
		customerId: "C01",
		domain:     "",
	}
}

// TestAddUserAlias_RegisteredAsResourceAction verifies the action registers
// under the user resource type alongside remove_user_alias.
func TestAddUserAlias_RegisteredAsResourceAction(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	manager := actions.NewActionManager(context.Background())
	registry, err := manager.GetTypeRegistry(context.Background(), resourceTypeUser.Id)
	if err != nil {
		t.Fatalf("GetTypeRegistry: %v", err)
	}
	if err := o.ResourceActions(context.Background(), registry); err != nil {
		t.Fatalf("ResourceActions: %v", err)
	}
	schemas, _, err := manager.ListActionSchemas(context.Background(), resourceTypeUser.Id)
	if err != nil {
		t.Fatalf("ListActionSchemas: %v", err)
	}
	names := map[string]bool{}
	for _, schema := range schemas {
		names[schema.GetName()] = true
	}
	expected := []string{
		"change_user_org_unit", "offboarding_profile_update", "sign_out_user",
		"delete_all_oauth_tokens", "delete_all_application_passwords",
		"update_user_manager", "update_user_profile", "make_admin",
		"remove_user_alias", "add_user_alias",
	}
	if len(names) != len(expected) {
		t.Fatalf("user action inventory changed beyond add_user_alias: got %v", names)
	}
	for _, name := range expected {
		if !names[name] {
			t.Fatalf("required user action %s is missing from %v", name, names)
		}
	}
}

// TestAddUserAlias_AddsAndReadsBack covers the successful insert path: one
// insert, observed post-state presence on the pinned target, added outcome.
func TestAddUserAlias_AddsAndReadsBack(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01", Aliases: []string{}},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("addUserAliasActionHandler: %v", err)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true, got %v", resp)
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "added" {
		t.Fatalf("expected outcome=added, got %q", got)
	}
	if resp.GetFields()["alias_present_before"].GetBoolValue() {
		t.Fatalf("pre-read must observe absence")
	}
	if !resp.GetFields()["alias_present_after"].GetBoolValue() {
		t.Fatalf("expected observed alias_present_after=true")
	}
	if !resp.GetFields()["observation_complete"].GetBoolValue() {
		t.Fatalf("expected observation_complete=true")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("expected exactly 1 insert and no other mutations, got inserts=%d mutations=%d", state.inserts, state.mutations)
	}
	if !resp.GetFields()["insert_attempted"].GetBoolValue() || !resp.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatalf("added outcome requires insert_attempted=true and insert_acknowledged=true")
	}
}

// TestAddUserAlias_AlreadyPresentSameAccount covers idempotent replay when
// the alias is already attached to the SAME pinned target: zero inserts.
func TestAddUserAlias_AlreadyPresentSameAccount(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01", Aliases: []string{"new@example.com"}},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("already-present must not be an error: %v", err)
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "already_present" {
		t.Fatalf("expected outcome=already_present, got %q", got)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("verified same-account presence is success=true")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("expected zero mutations on already-present, got %d", state.mutations)
	}
	if resp.GetFields()["insert_attempted"].GetBoolValue() || resp.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatalf("pre-read replay carries no insert attempt or acknowledgement")
	}
}

// TestAddUserAlias_ForeignUserConflict covers a second user owning the
// address: owner_mismatch failure BEFORE any insert (read-by-alias), zero
// inserts.
func TestAddUserAlias_ForeignUserConflict(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
		"u2": {Id: "u2", PrimaryEmail: "other@example.com", CustomerId: "C01", Aliases: []string{"new@example.com"}},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	_, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err == nil {
		t.Fatalf("foreign-user ownership must fail")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", status.Code(err), err)
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("owner_mismatch must be detected before any mutation, got %d", state.mutations)
	}
}

// TestAddUserAlias_GroupConflictOnProvider409 covers group/namespace
// ownership: the user lookup 404s, the INSERT 409s (authoritative provider
// conflict), and no same-account replay exists -> alias_conflict.
func TestAddUserAlias_GroupConflictOnProvider409(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{"new@example.com": true}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err == nil {
		t.Fatalf("group-owned address must fail on the provider conflict")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", status.Code(err), err)
	}
	if resp != nil && resp.GetFields()["outcome"].GetStringValue() != "alias_conflict" {
		t.Fatalf("expected outcome=alias_conflict, got %q", resp.GetFields()["outcome"].GetStringValue())
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("exactly one insert attempt expected before the 409 (no other mutations), got inserts=%d mutations=%d", state.inserts, state.mutations)
	}
}

// TestAddUserAlias_ReplayAfter409Qualified covers the bounded same-account
// replay qualification: the insert 409s (concurrent duplicate), the qualified
// readback shows the alias on the SAME pinned target -> already_present.
func TestAddUserAlias_ReplayAfter409Qualified(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	// Emulate a concurrent duplicate insert: the pre-read observes absence,
	// then the provider answers 409 while the alias has concurrently landed
	// on the SAME pinned target. The server attaches it when it writes the
	// forced 409 (see the POST handler below). The handler's qualified
	// readback must recognize the same-account replay as already_present.
	state.mtx.Lock()
	state.insertFailStatus = http.StatusConflict
	state.mtx.Unlock()

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("qualified same-account 409 replay must resolve to already_present, got: %v", err)
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "already_present" {
		t.Fatalf("expected outcome=already_present, got %q", got)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("qualified replay success=true")
	}
	if !resp.GetFields()["alias_present_after"].GetBoolValue() {
		t.Fatalf("qualified replay must observe post-state presence")
	}
	if !resp.GetFields()["insert_attempted"].GetBoolValue() {
		t.Fatalf("qualified replay followed an insert attempt")
	}
	if resp.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatalf("the provider never acknowledged THIS insert; insert_acknowledged must be false")
	}
}

// TestAddUserAlias_Rejections covers primary-address, stable-ID, malformed
// email, and precondition mismatches — all before any provider mutation.
func TestAddUserAlias_Rejections(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)
	ctx := context.Background()

	// alias == primary
	_, _, err := o.addUserAliasActionHandler(ctx, aliasArgs("u1", "target@example.com", "target@example.com", "C01"))
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("primary misuse must be InvalidArgument, got %v", err)
	}
	// email as user_id
	_, _, err = o.addUserAliasActionHandler(ctx, aliasArgs("target@example.com", "new@example.com", "target@example.com", "C01"))
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("email-as-user_id must be InvalidArgument, got %v", err)
	}
	// malformed alias
	_, _, err = o.addUserAliasActionHandler(ctx, aliasArgs("u1", "not-an-email", "target@example.com", "C01"))
	if err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed alias must be InvalidArgument, got %v", err)
	}
	// primary precondition mismatch
	_, _, err = o.addUserAliasActionHandler(ctx, aliasArgs("u1", "new@example.com", "WRONG@example.com", "C01"))
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("primary precondition mismatch must be FailedPrecondition, got %v", err)
	}
	// customer precondition mismatch
	_, _, err = o.addUserAliasActionHandler(ctx, aliasArgs("u1", "new@example.com", "target@example.com", "C99"))
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("customer precondition mismatch must be FailedPrecondition, got %v", err)
	}
	// missing user
	_, _, err = o.addUserAliasActionHandler(ctx, aliasArgs("ghost", "new@example.com", "target@example.com", "C01"))
	if err == nil || status.Code(err) != codes.NotFound {
		t.Fatalf("missing user must be NotFound, got %v", err)
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("rejections must perform zero provider mutations, got %d", state.mutations)
	}
}

// TestAddUserAlias_PermissionErrorPreserved covers a 403 on insert: the
// error and uncertainty are preserved, success is never claimed.
func TestAddUserAlias_PermissionErrorPreserved(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	state.mtx.Lock()
	state.insertFailStatus = http.StatusForbidden
	state.mtx.Unlock()
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err == nil {
		t.Fatalf("permission error must surface")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v (%v)", status.Code(err), err)
	}
	if resp != nil {
		if got := resp.GetFields()["outcome"].GetStringValue(); got != "readback_unknown" {
			t.Fatalf("expected outcome=readback_unknown, got %q", got)
		}
		if resp.GetFields()["success"].GetBoolValue() {
			t.Fatalf("permission error must never report success")
		}
		if _, present := resp.GetFields()["alias_present_after"]; present {
			t.Fatalf("unknown post-state must omit alias_present_after")
		}
	}
}

// TestAddUserAlias_UnknownPostStateOmitted covers a readback failure after
// an acknowledged insert: observation incomplete, presence omitted (never
// false), explicit error.
func TestAddUserAlias_UnknownPostStateOmitted(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	state.mtx.Lock()
	state.readbackFailAfterInsert = true
	state.mtx.Unlock()
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err == nil {
		t.Fatalf("failed post-insert readback must surface an error")
	}
	if resp == nil {
		t.Fatalf("qualified partial result must be returned alongside the error")
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "readback_unknown" {
		t.Fatalf("expected outcome=readback_unknown, got %q", got)
	}
	if resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("unknown post-state must not report success")
	}
	if resp.GetFields()["alias_present_before"].GetBoolValue() {
		t.Fatalf("alias_present_before must be the observed false pre-insert state")
	}
	if _, present := resp.GetFields()["alias_present_after"]; present {
		t.Fatalf("unknown post-state must omit alias_present_after, never default it to false")
	}
	if resp.GetFields()["observation_complete"].GetBoolValue() {
		t.Fatalf("observation_complete must be false when readback failed")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("expected exactly 1 insert and no other mutations before the failed readback, got inserts=%d mutations=%d", state.inserts, state.mutations)
	}
}

// invokeAddUserAlias drives add_user_alias through the REAL public SDK
// invocation boundary: actions.NewActionManager -> GetTypeRegistry ->
// ResourceActions registration -> InvokeAction (which runs the handler in the
// manager's goroutine, applies setOutcome, and returns actionID/status/rv/err).
// This exercises the exact path a hosted caller would, including the FAILED
// status retaining rv alongside err.
func invokeAddUserAlias(t *testing.T, o *userResourceType, args *structpb.Struct) (v2.BatonActionStatus, *structpb.Struct, error) {
	t.Helper()
	manager := actions.NewActionManager(context.Background())
	registry, err := manager.GetTypeRegistry(context.Background(), resourceTypeUser.Id)
	if err != nil {
		t.Fatalf("GetTypeRegistry: %v", err)
	}
	if err := o.ResourceActions(context.Background(), registry); err != nil {
		t.Fatalf("ResourceActions: %v", err)
	}
	actionID, actionStatus, rv, _, invokeErr := manager.InvokeAction(context.Background(), "add_user_alias", resourceTypeUser.Id, args)
	if actionID == "" {
		t.Fatalf("InvokeAction returned no action ID")
	}
	return actionStatus, rv, invokeErr
}

// sdkAddArgs builds the add_user_alias argument struct.
func sdkAddArgs(userID, alias, primary, customer string) *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":                structpb.NewStringValue(userID),
		"alias":                  structpb.NewStringValue(alias),
		"expected_primary_email": structpb.NewStringValue(primary),
		"expected_customer_id":   structpb.NewStringValue(customer),
	}}
}

// TestAddUserAlias_SDKInvocation_Success exercises the public invocation
// path end-to-end for the success case: COMPLETE status, added outcome,
// evidence fields present.
func TestAddUserAlias_SDKInvocation_Success(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	actionStatus, rv, err := invokeAddUserAlias(t, o, sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("InvokeAction: %v", err)
	}
	if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE {
		t.Fatalf("expected COMPLETE, got %v", actionStatus)
	}
	if got := rv.GetFields()["outcome"].GetStringValue(); got != "added" {
		t.Fatalf("expected outcome=added, got %q", got)
	}
	if !rv.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true")
	}
	if !rv.GetFields()["insert_attempted"].GetBoolValue() || !rv.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatalf("added outcome requires insert_attempted=true and insert_acknowledged=true")
	}
	if !rv.GetFields()["alias_present_after"].GetBoolValue() {
		t.Fatalf("expected observed alias_present_after=true")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("expected exactly 1 insert / 1 total mutation, got inserts=%d mutations=%d", state.inserts, state.mutations)
	}
}

// TestAddUserAlias_SDKInvocation_SameAccountReplay exercises the public
// invocation path for the same-account replay: COMPLETE, already_present,
// zero mutations, and NO insert attempt/acknowledgement evidence.
func TestAddUserAlias_SDKInvocation_SameAccountReplay(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01", Aliases: []string{"new@example.com"}},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	actionStatus, rv, err := invokeAddUserAlias(t, o, sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("InvokeAction: %v", err)
	}
	if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE {
		t.Fatalf("expected COMPLETE, got %v", actionStatus)
	}
	if got := rv.GetFields()["outcome"].GetStringValue(); got != "already_present" {
		t.Fatalf("expected outcome=already_present, got %q", got)
	}
	if rv.GetFields()["insert_attempted"].GetBoolValue() || rv.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatalf("pre-read replay must carry no insert attempt or acknowledgement")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("replay must perform zero mutations, got %d", state.mutations)
	}
}

// TestAddUserAlias_SDKInvocation_ForeignAndGroupCollision exercises the
// public invocation path for foreign-user and group collisions: FAILED
// status WITH rv retained (SDK setOutcome keeps return values on failure),
// outcome owner_mismatch / alias_conflict, zero-or-bounded mutations.
func TestAddUserAlias_SDKInvocation_ForeignAndGroupCollision(t *testing.T) {
	// Foreign user ownership is detected before any insert.
	foreign := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}}
	foreignServer := newAddAliasTestServer(foreign)
	defer foreignServer.Close()
	o := newAddAliasTestResourceType(t, foreignServer)
	// Pre-attach the alias to another user, outside the target's collection.
	foreign.mtx.Lock()
	foreign.users["u2"] = &addAliasTestUser{Id: "u2", PrimaryEmail: "other@example.com", CustomerId: "C01", Aliases: []string{"new@example.com"}}
	foreign.mtx.Unlock()

	actionStatus, rv, err := invokeAddUserAlias(t, o, sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("SDK invocation failed: %v", err)
	}
	if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		t.Fatalf("expected FAILED, got %v", actionStatus)
	}
	if rv == nil {
		t.Fatalf("SDK must retain the observed result alongside the error")
	}
	if got := rv.GetFields()["outcome"].GetStringValue(); got != "owner_mismatch" {
		t.Fatalf("expected outcome=owner_mismatch, got %q", got)
	}
	if rv.GetFields()["insert_attempted"].GetBoolValue() || rv.GetFields()["error"].GetStringValue() == "" {
		t.Fatalf("foreign ownership must fail with error evidence before an insert")
	}

	// Group collision: user lookup 404s, insert 409s, target collection
	// lacks the alias, by-alias read 404s -> alias_conflict.
	groupState := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{"new@example.com": true}}
	groupServer := newAddAliasTestServer(groupState)
	defer groupServer.Close()
	o2 := newAddAliasTestResourceType(t, groupServer)

	actionStatus2, rv2, err2 := invokeAddUserAlias(t, o2, sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err2 != nil {
		t.Fatalf("SDK invocation failed: %v", err2)
	}
	if actionStatus2 != v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		t.Fatalf("expected FAILED, got %v", actionStatus2)
	}
	if got := rv2.GetFields()["outcome"].GetStringValue(); got != "alias_conflict" {
		t.Fatalf("expected outcome=alias_conflict, got %q", got)
	}
	if !rv2.GetFields()["insert_attempted"].GetBoolValue() {
		t.Fatalf("conflict requires insert_attempted=true")
	}
	if rv2.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatalf("conflict must not carry insert_acknowledged=true")
	}
	if rv2.GetFields()["alias_present_after"].GetBoolValue() || !rv2.GetFields()["observation_complete"].GetBoolValue() {
		t.Fatalf("the target collection was observed without the conflicting alias")
	}
	groupState.mtx.Lock()
	defer groupState.mtx.Unlock()
	if groupState.inserts != 1 || groupState.mutations != 1 {
		t.Fatalf("exactly one insert attempt expected, got inserts=%d mutations=%d", groupState.inserts, groupState.mutations)
	}
}

// TestAddUserAlias_SDKInvocation_Rejections exercises the public invocation
// path for precondition and malformed rejections: FAILED status, rv retained,
// zero mutations.
func TestAddUserAlias_SDKInvocation_Rejections(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	for name, args := range map[string]*structpb.Struct{
		"primary misuse":   sdkAddArgs("u1", "target@example.com", "target@example.com", "C01"),
		"email as user_id": sdkAddArgs("target@example.com", "new@example.com", "target@example.com", "C01"),
		"malformed alias":  sdkAddArgs("u1", "not-an-email", "target@example.com", "C01"),
		"wrong primary":    sdkAddArgs("u1", "new@example.com", "WRONG@example.com", "C01"),
		"wrong customer":   sdkAddArgs("u1", "new@example.com", "target@example.com", "C99"),
		"missing user":     sdkAddArgs("ghost", "new@example.com", "target@example.com", "C01"),
	} {
		actionStatus, rv, err := invokeAddUserAlias(t, o, args)
		if err != nil {
			t.Fatalf("%s: SDK invocation failed: %v", name, err)
		}
		if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
			t.Fatalf("%s: expected FAILED, got %v", name, actionStatus)
		}
		if rv == nil {
			t.Fatalf("%s: SDK must retain rv alongside the error", name)
		}
		if rv.GetFields()["error"].GetStringValue() == "" {
			t.Fatalf("%s: failed action lost error evidence", name)
		}
		if rv.GetFields()["insert_attempted"].GetBoolValue() {
			t.Fatalf("%s: no insert may be attempted", name)
		}
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("rejections must perform zero provider mutations, got %d", state.mutations)
	}
}

// TestAddUserAlias_SDKInvocation_ReadbackUncertainty exercises the public
// invocation path when the post-write readback fails: FAILED status, rv
// retained with insert_attempted=true and insert_acknowledged=true while
// alias_present_after is omitted because the post-state could not be read;
// observed alias_present_before=false preserved.
func TestAddUserAlias_SDKInvocation_ReadbackUncertainty(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	state.mtx.Lock()
	state.readbackFailAfterInsert = true
	state.mtx.Unlock()
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	actionStatus, rv, err := invokeAddUserAlias(t, o, sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("SDK invocation failed: %v", err)
	}
	if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		t.Fatalf("expected FAILED, got %v", actionStatus)
	}
	if rv == nil {
		t.Fatalf("SDK must retain rv alongside the error")
	}
	if got := rv.GetFields()["outcome"].GetStringValue(); got != "readback_unknown" {
		t.Fatalf("expected outcome=readback_unknown, got %q", got)
	}
	if !rv.GetFields()["insert_attempted"].GetBoolValue() {
		t.Fatalf("an insert was attempted")
	}
	if !rv.GetFields()["insert_acknowledged"].GetBoolValue() || rv.GetFields()["error"].GetStringValue() == "" {
		t.Fatalf("acknowledged insert and subsequent read error must both be retained")
	}
	if rv.GetFields()["alias_present_before"].GetBoolValue() {
		t.Fatalf("observed before-state was absent")
	}
	if _, present := rv.GetFields()["alias_present_after"]; present {
		t.Fatalf("unknown post-state must omit alias_present_after")
	}
	if rv.GetFields()["observation_complete"].GetBoolValue() {
		t.Fatalf("observation_complete must be false")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("exactly one insert before the failed readback, got inserts=%d mutations=%d", state.inserts, state.mutations)
	}
}

// TestAddUserAlias_NonEditableZeroMutation covers the same-account
// NonEditableAliases pre-check: precondition_failed before any mutation.
func TestAddUserAlias_NonEditableZeroMutation(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	state.mtx.Lock()
	state.users["u1"] = &addAliasTestUser{Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01", NonEditableAliases: []string{"locked@example.com"}}
	state.mtx.Unlock()
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)

	// Note: NonEditableAliases is served only via the full user GET; the
	// dedicated aliases.list collection intentionally omits non-editable
	// aliases, so present=false in the collection and the pre-check gates.
	_, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "locked@example.com", "target@example.com", "C01"))
	if err == nil {
		t.Fatalf("non-editable alias must be rejected")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v (%v)", status.Code(err), err)
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("non-editable rejection must perform zero mutations, got %d", state.mutations)
	}
}

// TestAddUserAlias_SecondaryDomainDifferentSyncDomain covers the finalized
// namespace decision: the alias lives on a provider-verified secondary domain
// that differs from the connector's configured sync domain (o.domain). The
// action must succeed — the sync domain is not a write allowlist — as long as
// the pinned target/customer preconditions hold.
func TestAddUserAlias_SecondaryDomainDifferentSyncDomain(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)
	// Simulate a connector configured with a DIFFERENT sync domain than the
	// alias's secondary domain.
	o.domain = "primary.example.com"

	resp, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "alias@secondary.example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("secondary-domain alias in the same customer must succeed: %v", err)
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "added" {
		t.Fatalf("expected outcome=added, got %q", got)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("expected exactly 1 insert / 1 total mutation, got inserts=%d mutations=%d", state.inserts, state.mutations)
	}
}

// TestAddUserAlias_WrongConfiguredCustomer covers the authoritative
// configured-customer boundary: a provider target in a different customer
// than the connector's configuration is rejected with PermissionDenied even
// when the caller-submitted expected_customer_id MATCHES the provider record
// (the submitted pin alone is never trusted), and no mutation occurs.
func TestAddUserAlias_WrongConfiguredCustomer(t *testing.T) {
	state := &addAliasTestState{users: map[string]*addAliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "OTHER-CUSTOMER"},
	}, groups: map[string]bool{}}
	server := newAddAliasTestServer(state)
	defer server.Close()
	o := newAddAliasTestResourceType(t, server)
	// o.customerId is "C01" via newAddAliasTestResourceType; the
	// provider record carries OTHER-CUSTOMER. The submitted expected_customer_id
	// deliberately MATCHES the provider record to prove the gate is not the
	// submitted pin.
	_, _, err := o.addUserAliasActionHandler(context.Background(), aliasArgs("u1", "new@example.com", "target@example.com", "OTHER-CUSTOMER"))
	if err == nil {
		t.Fatalf("wrong configured customer must be rejected")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v (%v)", status.Code(err), err)
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.mutations != 0 {
		t.Fatalf("configured-customer rejection must perform zero mutations, got %d", state.mutations)
	}
}

func TestAddUserAlias_SDKRequiresTrustedCompleteAccount(t *testing.T) {
	for _, test := range []struct {
		name, configuredCustomer, primary, outcome string
	}{
		{"missing configured customer", "", "target@example.com", aliasOutcomePreconditionFailed},
		{"incomplete provider target", "C01", "", aliasOutcomeReadbackUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &addAliasTestState{users: map[string]*addAliasTestUser{
				"u1": {Id: "u1", PrimaryEmail: test.primary, CustomerId: "C01"},
			}}
			server := newAddAliasTestServer(state)
			defer server.Close()
			resourceType := newAddAliasTestResourceType(t, server)
			resourceType.customerId = test.configuredCustomer
			actionStatus, result, err := invokeAddUserAlias(t, resourceType, sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
			if err != nil {
				t.Fatalf("SDK invocation failed: %v", err)
			}
			if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED || result.GetFields()["error"].GetStringValue() == "" {
				t.Fatalf("untrusted/incomplete target must remain a failed action with error evidence")
			}
			if got := result.GetFields()["outcome"].GetStringValue(); got != test.outcome {
				t.Fatalf("outcome = %s, want %s", got, test.outcome)
			}
			if _, observed := result.GetFields()["alias_present_after"]; observed {
				t.Fatal("unread post-state must remain unknown")
			}
			state.mtx.Lock()
			defer state.mtx.Unlock()
			if state.mutations != 0 {
				t.Fatalf("invalid target caused %d mutations", state.mutations)
			}
		})
	}
}

func TestAddUserAlias_SDKConflictReadbackFailureIsUnknown(t *testing.T) {
	state := &addAliasTestState{
		users: map[string]*addAliasTestUser{
			"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
		},
		insertFailStatus:        http.StatusConflict,
		readbackFailAfterInsert: true,
	}
	server := newAddAliasTestServer(state)
	defer server.Close()
	actionStatus, result, err := invokeAddUserAlias(t, newAddAliasTestResourceType(t, server), sdkAddArgs("u1", "new@example.com", "target@example.com", "C01"))
	if err != nil {
		t.Fatalf("SDK invocation failed: %v", err)
	}
	if actionStatus != v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED || result.GetFields()["error"].GetStringValue() == "" {
		t.Fatal("unreadable conflict reconciliation must fail with error evidence")
	}
	if result.GetFields()["outcome"].GetStringValue() != aliasOutcomeReadbackUnknown {
		t.Fatal("readback failure must not become an owner conflict or successful replay")
	}
	if !result.GetFields()["insert_attempted"].GetBoolValue() || result.GetFields()["insert_acknowledged"].GetBoolValue() {
		t.Fatal("conflicted insert attempt must not gain an acknowledgement")
	}
	if _, observed := result.GetFields()["alias_present_after"]; observed {
		t.Fatal("failed readback must omit alias post-state")
	}
	state.mtx.Lock()
	defer state.mtx.Unlock()
	if state.inserts != 1 || state.mutations != 1 {
		t.Fatalf("conflict reconciliation replayed mutation: inserts=%d mutations=%d", state.inserts, state.mutations)
	}
}
