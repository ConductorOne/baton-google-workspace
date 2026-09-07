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

	"google.golang.org/protobuf/types/known/structpb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type aliasTestUser struct {
	Id                 string
	PrimaryEmail       string
	CustomerId         string
	Aliases            []string
	NonEditableAliases []string
}

type aliasTestState struct {
	mtx        sync.Mutex
	users      map[string]*aliasTestUser
	deleteHits int
	getCount   int
	// when set, the alias DELETE returns this status instead of succeeding
	deleteFailStatus   int
	readbackFailStatus int
	afterDeleteUser    *aliasTestUser
}

func newAliasTestServer(state *aliasTestState) *httptest.Server {
	mux := http.NewServeMux()

	serveUser := func(w http.ResponseWriter, u *aliasTestUser) {
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
		w.Header().Set("Content-Type", "application/json")
		if state.deleteHits > 0 && r.Method == http.MethodGet && state.readbackFailStatus != 0 {
			http.Error(w, `{"error":{"code":403,"message":"readback denied"}}`, state.readbackFailStatus)
			return
		}

		// GET /users/{userKey}
		if len(parts) == 1 && r.Method == http.MethodGet {
			state.getCount++
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
				http.Error(w, `{"error":{"code":404,"message":"Resource Not Found: userKey"}}`, http.StatusNotFound)
				return
			}
			serveUser(w, u)
			return
		}
		if len(parts) == 2 && parts[1] == "aliases" && r.Method == http.MethodGet {
			u := state.users[parts[0]]
			if u == nil {
				http.Error(w, `{"error":{"code":404,"message":"user not found"}}`, http.StatusNotFound)
				return
			}
			aliases := make([]map[string]string, 0, len(u.Aliases))
			for _, alias := range u.Aliases {
				aliases = append(aliases, map[string]string{"alias": alias})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"aliases": aliases})
			return
		}

		// DELETE /users/{userKey}/aliases/{alias}
		if len(parts) == 3 && parts[1] == "aliases" && r.Method == http.MethodDelete {
			state.deleteHits++
			if state.deleteFailStatus != 0 {
				http.Error(w, fmt.Sprintf(`{"error":{"code":%d,"message":"forced failure"}}`, state.deleteFailStatus), state.deleteFailStatus)
				return
			}
			u := state.users[parts[0]]
			if u == nil {
				http.Error(w, `{"error":{"code":404,"message":"Resource Not Found: userKey"}}`, http.StatusNotFound)
				return
			}
			alias := parts[2]
			for i, a := range u.Aliases {
				if strings.EqualFold(a, alias) {
					u.Aliases = append(u.Aliases[:i], u.Aliases[i+1:]...)
					break
				}
			}
			if state.afterDeleteUser != nil {
				state.users[parts[0]] = state.afterDeleteUser
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	return httptest.NewServer(mux)
}

func newAliasTestResourceType(t *testing.T, server *httptest.Server) *userResourceType {
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

func aliasArgs(userId, alias, primary, customer string) *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"user_id":                structpb.NewStringValue(userId),
		"alias":                  structpb.NewStringValue(alias),
		"expected_primary_email": structpb.NewStringValue(primary),
		"expected_customer_id":   structpb.NewStringValue(customer),
	}}
}

func TestRemoveUserAlias_Deletes(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "new@example.com", CustomerId: "C01", Aliases: []string{"old@example.com"}},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	o := newAliasTestResourceType(t, server)

	resp, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("u1", "old@example.com", "new@example.com", "C01"))
	if err != nil {
		t.Fatalf("removeUserAliasActionHandler: %v", err)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("expected success=true, got %v", resp)
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "deleted" {
		t.Fatalf("expected outcome=deleted, got %q", got)
	}
	if !resp.GetFields()["alias_present_before"].GetBoolValue() {
		t.Fatalf("expected alias_present_before=true")
	}
	if resp.GetFields()["alias_present_after"].GetBoolValue() {
		t.Fatalf("expected alias_present_after=false")
	}
	if !resp.GetFields()["observation_complete"].GetBoolValue() {
		t.Fatalf("expected observation_complete=true")
	}
	if state.deleteHits != 1 {
		t.Fatalf("expected exactly 1 DELETE, got %d", state.deleteHits)
	}
}

func TestRemoveUserAlias_AlreadyAbsent(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "new@example.com", CustomerId: "C01", Aliases: nil},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	o := newAliasTestResourceType(t, server)

	resp, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("u1", "old@example.com", "new@example.com", "C01"))
	if err != nil {
		t.Fatalf("already-absent must not be an error: %v", err)
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "already_absent" {
		t.Fatalf("expected outcome=already_absent, got %q", got)
	}
	if !resp.GetFields()["success"].GetBoolValue() {
		t.Fatalf("already_absent is success=true (idempotent)")
	}
	if state.deleteHits != 0 {
		t.Fatalf("expected no DELETE for already-absent alias, got %d", state.deleteHits)
	}
}

func TestRemoveUserAlias_UserNotFoundDistinct(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{}}
	server := newAliasTestServer(state)
	defer server.Close()
	o := newAliasTestResourceType(t, server)

	_, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("ghost", "old@example.com", "new@example.com", "C01"))
	if err == nil {
		t.Fatalf("missing user must be an error")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected a qualified not-found error, got: %v", err)
	}
	if state.deleteHits != 0 {
		t.Fatalf("expected no DELETE for missing user")
	}
}

func TestRemoveUserAlias_RejectsPrimary(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "primary@example.com", CustomerId: "C01", Aliases: []string{"x@example.com"}},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	o := newAliasTestResourceType(t, server)

	_, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("u1", "primary@example.com", "primary@example.com", "C01"))
	if err == nil {
		t.Fatalf("primary address removal must be rejected")
	}
	if !strings.Contains(err.Error(), "primary") {
		t.Fatalf("expected primary-rejection error, got: %v", err)
	}
	if state.deleteHits != 0 {
		t.Fatalf("expected no DELETE when rejecting primary")
	}
}

func TestRemoveUserAlias_PreconditionPrimaryMismatch(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "actual@example.com", CustomerId: "C01", Aliases: []string{"old@example.com"}},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	o := newAliasTestResourceType(t, server)

	_, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("u1", "old@example.com", "WRONG@example.com", "C01"))
	if err == nil {
		t.Fatalf("primary precondition mismatch must abort")
	}
	if state.deleteHits != 0 {
		t.Fatalf("expected no DELETE on precondition failure, got %d", state.deleteHits)
	}
}

func TestRemoveUserAlias_PreconditionCustomerMismatch(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "new@example.com", CustomerId: "COTHER", Aliases: []string{"old@example.com"}},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	o := newAliasTestResourceType(t, server)

	_, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("u1", "old@example.com", "new@example.com", "C01"))
	if err == nil {
		t.Fatalf("customer precondition mismatch must abort")
	}
	if state.deleteHits != 0 {
		t.Fatalf("expected no DELETE on customer precondition failure")
	}
}

func TestRemoveUserAlias_DeleteStillPresentIsError(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "new@example.com", CustomerId: "C01", Aliases: []string{"old@example.com"}},
	}}
	// Server variant whose DELETE returns 200 but never removes the alias
	// (simulates a provider no-op). GETs go through the normal handler.
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		state.mtx.Lock()
		defer state.mtx.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		parts := strings.Split(path, "/")
		if len(parts) == 3 && parts[1] == "aliases" && r.Method == http.MethodDelete {
			state.deleteHits++
			w.WriteHeader(http.StatusOK)
			return
		}
		if len(parts) == 2 && parts[1] == "aliases" && r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"aliases": []map[string]string{{"alias": "old@example.com"}}})
			return
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			state.getCount++
			u := state.users[parts[0]]
			if u == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":           u.Id,
				"primaryEmail": u.PrimaryEmail,
				"customerId":   u.CustomerId,
				"aliases":      u.Aliases,
			})
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o := newAliasTestResourceType(t, srv)
	resp, _, err := o.removeUserAliasActionHandler(context.Background(), aliasArgs("u1", "old@example.com", "new@example.com", "C01"))
	if err == nil {
		t.Fatalf("alias still present after delete must be an explicit error")
	}
	if resp == nil {
		t.Fatalf("expected observed data returned alongside the error")
	}
	if got := resp.GetFields()["outcome"].GetStringValue(); got != "readback_unknown" {
		t.Fatalf("expected outcome=readback_unknown, got %q", got)
	}
	if !resp.GetFields()["alias_present_after"].GetBoolValue() {
		t.Fatalf("expected alias_present_after=true when provider kept the alias")
	}
	if !resp.GetFields()["observation_complete"].GetBoolValue() {
		t.Fatalf("expected observation_complete=true; the readback DID complete")
	}
}
