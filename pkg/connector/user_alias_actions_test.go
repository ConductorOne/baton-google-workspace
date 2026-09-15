package connector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestUserAliasActionsThroughSDK(t *testing.T) {
	for _, tc := range []struct {
		name      string
		action    string
		present   bool
		scenario  string
		outcome   string
		errorCode string
		writes    int32
	}{
		{"add", "add_user_alias", false, "", "added", "", 1},
		{"already present", "add_user_alias", true, "", "already_present", "", 0},
		{"noneditable overlap", "add_user_alias", true, "noneditable", "already_present", "", 0},
		{"noneditable only", "add_user_alias", false, "noneditable", "", "FailedPrecondition", 0},
		{"conflict", "add_user_alias", false, "conflict", "", "Aborted", 1},
		{"concurrent attachment", "add_user_alias", false, "concurrent", "already_present", "", 1},
		{"write failure", "add_user_alias", false, "write error", "", "Unavailable", 1},
		{"throttled insert", "add_user_alias", false, "throttled", "", "Unavailable", 1},
		{"read failure", "add_user_alias", false, "read error", "", "PermissionDenied", 0},
		{"readback failure", "add_user_alias", false, "readback error", "", "PermissionDenied", 1},
		{"foreign customer", "add_user_alias", false, "foreign customer", "", "PermissionDenied", 0},
		{"primary address", "add_user_alias", false, "primary", "", "FailedPrecondition", 0},
		{"remove", "remove_user_alias", true, "", "deleted", "", 1},
		{"already absent", "remove_user_alias", false, "", "already_absent", "", 0},
		{"remove noneditable", "remove_user_alias", true, "noneditable", "", "FailedPrecondition", 0},
		{"remove noneditable only", "remove_user_alias", false, "noneditable", "", "FailedPrecondition", 0},
		{"delete failure", "remove_user_alias", true, "write error", "", "Unavailable", 1},
		{"throttled delete", "remove_user_alias", true, "throttled", "", "Unavailable", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const alias = "alias@example.com"
			var present atomic.Bool
			present.Store(tc.present)
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet && r.URL.Path == "/admin/directory/v1/users/user-id" {
					customer, primary := "customer", "user@example.com"
					if tc.scenario == "foreign customer" {
						customer = "other-customer"
					}
					if tc.scenario == "primary" {
						primary = alias
					}
					user := map[string]any{"id": "user-id", "customerId": customer, "primaryEmail": primary}
					if tc.scenario == "noneditable" {
						user["nonEditableAliases"] = []string{alias}
					}
					if err := json.NewEncoder(w).Encode(user); err != nil {
						t.Errorf("encoding user response: %v", err)
					}
					return
				}
				if r.Method == http.MethodGet && r.URL.Path == "/admin/directory/v1/users/user-id/aliases" {
					if tc.scenario == "read error" || tc.scenario == "readback error" && writes.Load() > 0 {
						w.WriteHeader(http.StatusForbidden)
						_, _ = w.Write([]byte(`{"error":{"code":403,"message":"settings unavailable"}}`))
						return
					}
					aliases := []map[string]string{}
					if present.Load() {
						aliases = append(aliases, map[string]string{"alias": alias})
					}
					if err := json.NewEncoder(w).Encode(map[string]any{"aliases": aliases}); err != nil {
						t.Errorf("encoding alias response: %v", err)
					}
					return
				}
				isInsert := r.Method == http.MethodPost && r.URL.Path == "/admin/directory/v1/users/user-id/aliases"
				isDelete := r.Method == http.MethodDelete && r.URL.Path == "/admin/directory/v1/users/user-id/aliases/"+alias
				if !isInsert && !isDelete {
					t.Errorf("unexpected provider operation: %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected operation", http.StatusBadRequest)
					return
				}
				writes.Add(1)
				if isInsert {
					var body struct {
						Alias string `json:"alias"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Alias != alias {
						t.Errorf("unexpected alias request: body=%v error=%v", body, err)
						http.Error(w, "invalid alias", http.StatusBadRequest)
						return
					}
				}
				if tc.scenario == "throttled" {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`{"error":{"code":429,"message":"rate limit exceeded"}}`))
					return
				}
				if tc.scenario == "write error" {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`{"error":{"code":503,"message":"write unavailable"}}`))
					return
				}
				if tc.scenario == "conflict" || tc.scenario == "concurrent" {
					present.Store(tc.scenario == "concurrent")
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"error":{"code":409,"message":"alias already exists"}}`))
					return
				}
				present.Store(isInsert)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer server.Close()
			user := &userResourceType{
				customerId: "customer",
				client: &gwclient.GoogleWorkspaceClient{
					UserProvisioningService: newTestDirectoryService(t, server.URL, server.Client()),
				},
			}
			manager := actions.NewActionManager(t.Context())
			registry, err := manager.GetTypeRegistry(t.Context(), resourceTypeUser.Id)
			require.NoError(t, err)
			require.NoError(t, user.ResourceActions(t.Context(), registry))
			args, err := structpb.NewStruct(map[string]any{"user_id": "user-id", "alias": alias})
			require.NoError(t, err)
			_, outcome, result, _, err := manager.InvokeAction(t.Context(), tc.action, resourceTypeUser.Id, args)
			require.NoError(t, err)
			if tc.errorCode != "" {
				require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, outcome)
				require.Contains(t, result.GetFields()["error"].GetStringValue(), tc.errorCode)
			} else {
				require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, outcome)
				require.True(t, result.GetFields()[fieldSuccess].GetBoolValue())
				require.Equal(t, tc.outcome, result.GetFields()["outcome"].GetStringValue())
				require.Equal(t, tc.action == "add_user_alias", present.Load())
			}
			require.Equal(t, tc.writes, writes.Load(), "do not replay or mutate a different target")
		})
	}
}
