package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/pagination"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// safeUserResponse mirrors directoryAdmin.User for JSON without Password (avoids gosec G117).
type safeUserResponse struct {
	Id            string                          `json:"id,omitempty"`
	PrimaryEmail  string                          `json:"primaryEmail,omitempty"`
	Name          *directoryAdmin.UserName        `json:"name,omitempty"`
	RecoveryEmail string                          `json:"recoveryEmail,omitempty"`
	CustomSchemas map[string]googleapi.RawMessage `json:"customSchemas,omitempty"`
}

// Minimal fake for Reports Activities.List + Directory lookups used by admin_event_feed.
// activitiesByEvent maps eventName query parameter values to the Activities response
// to return. An unrecognised eventName returns an empty Activities response.
func newAdminFeedTestServer(users map[string]*directoryAdmin.User, groups map[string]*directoryAdmin.Group, activitiesByEvent map[string]*reportsAdmin.Activities) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/reports/v1/activity/users/all/applications/admin", func(w http.ResponseWriter, r *http.Request) {
		eventName := r.URL.Query().Get("eventName")
		resp, ok := activitiesByEvent[eventName]
		if !ok {
			resp = &reportsAdmin.Activities{}
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		u := users[key]
		if u == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(safeUserResponse{Id: u.Id, PrimaryEmail: u.PrimaryEmail, Name: u.Name})
	})

	mux.HandleFunc("/admin/directory/v1/groups/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/groups/")
		g := groups[key]
		if g == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(g)
	})

	return httptest.NewServer(mux)
}

func newReportsService(t *testing.T, baseURL string, hc *http.Client) *reportsAdmin.Service {
	t.Helper()
	srv, err := reportsAdmin.NewService(context.Background(), option.WithEndpoint(baseURL+"/"), option.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("newReportsService: %v", err)
	}
	return srv
}

func TestAdminEventFeed_GroupAndUserEvents(t *testing.T) {
	// Create one group and one user resolvable by email
	users := map[string]*directoryAdmin.User{
		"user@example.com": {Id: "user-1", Name: &directoryAdmin.UserName{FullName: "User One", GivenName: "User", FamilyName: "One", DisplayName: "User One"}, PrimaryEmail: "user@example.com"},
	}
	groups := map[string]*directoryAdmin.Group{
		"group@example.com": {Id: "group-1", Name: "Group One", Email: "group@example.com"},
	}

	// Build per-event-name Activities responses. The feed now issues one
	// ListActivities call per event name, so the mock routes by eventName.
	now := time.Now().UTC().Format(time.RFC3339)
	actsByEvent := map[string]*reportsAdmin.Activities{
		"CHANGE_GROUP_NAME": {
			Items: []*reportsAdmin.Activity{{
				Id: &reportsAdmin.ActivityId{Time: now, UniqueQualifier: 123},
				Events: []*reportsAdmin.ActivityEvents{{
					Type: "GROUP_SETTINGS",
					Name: "CHANGE_GROUP_NAME",
					Parameters: []*reportsAdmin.ActivityEventsParameters{
						{Name: "GROUP_EMAIL", Value: "group@example.com"},
					},
				}},
			}},
		},
		"ADD_GROUP_MEMBER": {
			Items: []*reportsAdmin.Activity{{
				Id: &reportsAdmin.ActivityId{Time: now, UniqueQualifier: 124},
				Events: []*reportsAdmin.ActivityEvents{{
					Type: "GROUP_SETTINGS",
					Name: "ADD_GROUP_MEMBER",
					Parameters: []*reportsAdmin.ActivityEventsParameters{
						{Name: "GROUP_EMAIL", Value: "group@example.com"},
						{Name: "USER_EMAIL", Value: "user@example.com"},
					},
				}},
			}},
		},
		"CHANGE_FIRST_NAME": {
			Items: []*reportsAdmin.Activity{{
				Id: &reportsAdmin.ActivityId{Time: now, UniqueQualifier: 456},
				Events: []*reportsAdmin.ActivityEvents{{
					Type: "USER_SETTINGS",
					Name: "CHANGE_FIRST_NAME",
					Parameters: []*reportsAdmin.ActivityEventsParameters{
						{Name: "USER_EMAIL", Value: "user@example.com"},
					},
				}},
			}},
		},
	}

	server := newAdminFeedTestServer(users, groups, actsByEvent)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsService(t, server.URL, server.Client())

	client := &gwclient.GoogleWorkspaceClient{
		UserService:   dir,
		GroupService:  dir,
		ReportService: rep,
	}

	feed := newAdminEventFeed(client)
	start := timestamppb.Now()
	st := &pagination.StreamToken{Size: 100}
	events, state, _, err := feed.ListEvents(context.Background(), start, st)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if state == nil || state.HasMore {
		t.Fatalf("expected single page, got more")
	}
	// We expect: one resource change for group name change, one create grant for add member, one user change event
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	// Basic sanity on event types
	var hasGroupChange, hasGrant, hasUserChange bool
	for _, e := range events {
		if rce := e.GetResourceChangeEvent(); rce != nil {
			if rce.GetResourceId().GetResourceType() == resourceTypeGroup.Id {
				hasGroupChange = true
			}
			if rce.GetResourceId().GetResourceType() == resourceTypeUser.Id {
				hasUserChange = true
			}
			continue
		}
		if cge := e.GetCreateGrantEvent(); cge != nil {
			hasGrant = true
			continue
		}
	}
	if !hasGroupChange || !hasGrant || !hasUserChange {
		t.Fatalf("expected group change, grant, and user change events")
	}
}
