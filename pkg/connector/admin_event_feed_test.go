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
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Minimal fake for Reports Activities.List + Directory lookups used by admin_event_feed.
func newAdminFeedTestServer(users map[string]*directoryAdmin.User, groups map[string]*directoryAdmin.Group, activities *reportsAdmin.Activities) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/reports/v1/activity/users/all/applications/admin", func(w http.ResponseWriter, r *http.Request) {
		// ignore query parsing beyond pageToken/startTime for now
		_ = json.NewEncoder(w).Encode(activities)
	})

	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/admin/directory/v1/users/")
		u := users[key]
		if u == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(u)
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

	// Build Activities with admin events we handle
	now := time.Now().UTC().Format(time.RFC3339)
	acts := &reportsAdmin.Activities{
		Items: []*reportsAdmin.Activity{
			{
				Id: &reportsAdmin.ActivityId{Time: now, UniqueQualifier: 123},
				Events: []*reportsAdmin.ActivityEvents{
					{
						Type: "GROUP_SETTINGS",
						Name: "CHANGE_GROUP_NAME",
						Parameters: []*reportsAdmin.ActivityEventsParameters{
							{Name: "GROUP_EMAIL", Value: "group@example.com"},
						},
					},
					{
						Type: "GROUP_SETTINGS", Name: "ADD_GROUP_MEMBER",
						Parameters: []*reportsAdmin.ActivityEventsParameters{
							{Name: "GROUP_EMAIL", Value: "group@example.com"},
							{Name: "USER_EMAIL", Value: "user@example.com"},
						},
					},
				},
			},
			{
				Id: &reportsAdmin.ActivityId{Time: now, UniqueQualifier: 456},
				Events: []*reportsAdmin.ActivityEvents{
					{Type: "USER_SETTINGS", Name: "CHANGE_FIRST_NAME", Parameters: []*reportsAdmin.ActivityEventsParameters{{Name: "USER_EMAIL", Value: "user@example.com"}}},
				},
			},
		},
		NextPageToken: "",
	}

	server := newAdminFeedTestServer(users, groups, acts)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsService(t, server.URL, server.Client())

	c := newTestConnector()
	// prime directory read-only scopes for admin_event_feed
	primeServiceCache(c, dir, nil)
	c.reportService = rep

	feed := newAdminEventFeed(c)
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
