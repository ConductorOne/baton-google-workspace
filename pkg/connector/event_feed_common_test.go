package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// newDirectoryUsersOnlyServer serves a paginated /admin/directory/v1/users listing (id +
// primaryEmail only, matching ListUserIDsPage's Fields projection) split into pages of
// pageSize, and a per-user activities.list response built by activityForUser.
func newDirectoryUsersOnlyServer(t *testing.T, users []*directoryAdmin.User, pageSize int, activityForUser func(email string) *reportsAdmin.Activities) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		start := 0
		if pt := r.URL.Query().Get("pageToken"); pt != "" {
			n, err := strconv.Atoi(pt)
			if err != nil {
				http.Error(w, "bad page token", http.StatusBadRequest)
				return
			}
			start = n
		}
		end := start + pageSize
		if end > len(users) {
			end = len(users)
		}
		resp := &directoryAdmin.Users{Users: users[start:end]}
		if end < len(users) {
			resp.NextPageToken = strconv.Itoa(end)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		// Path shape: /admin/reports/v1/activity/users/{userKey}/applications/{appName}
		// userKey is URL-escaped email; extract it without over-parsing.
		const prefix = "/admin/reports/v1/activity/users/"
		rest := r.URL.Path[len(prefix):]
		var userKey string
		for i, ch := range rest {
			if ch == '/' {
				userKey = rest[:i]
				break
			}
		}
		acts := activityForUser(userKey)
		if acts == nil {
			acts = &reportsAdmin.Activities{}
		}
		_ = json.NewEncoder(w).Encode(acts)
	})

	return httptest.NewServer(mux)
}

func newReportsServiceForTest(t *testing.T, baseURL string, hc *http.Client) *reportsAdmin.Service {
	t.Helper()
	srv, err := reportsAdmin.NewService(context.Background(), option.WithEndpoint(baseURL+"/"), option.WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("newReportsServiceForTest: %v", err)
	}
	return srv
}

func activityItem(uniqueQualifier int64, whenAgo time.Duration, actorEmail, actorProfileID string, params ...*reportsAdmin.ActivityEventsParameters) *reportsAdmin.Activity {
	return &reportsAdmin.Activity{
		Id: &reportsAdmin.ActivityId{
			Time:            time.Now().Add(-whenAgo).UTC().Format(time.RFC3339),
			UniqueQualifier: uniqueQualifier,
		},
		Actor: &reportsAdmin.ActivityActor{Email: actorEmail, ProfileId: actorProfileID},
		Events: []*reportsAdmin.ActivityEvents{
			{Parameters: params},
		},
	}
}

// TestScanUsersForEvents_PaginatesAcrossMultipleCallsWithoutLoss verifies that, with a reduced
// per-call batch size, walking N users across many small ListEvents calls visits every user
// exactly once and terminates (HasMore=false) rather than looping forever or stopping early —
// the two pagination failure modes called out in build-pagination.md and review-checklist.md.
func TestScanUsersForEvents_PaginatesAcrossMultipleCallsWithoutLoss(t *testing.T) {
	const totalUsers = 63 // deliberately not a multiple of the directory page size or batch size
	users := make([]*directoryAdmin.User, 0, totalUsers)
	for i := range totalUsers {
		users = append(users, &directoryAdmin.User{
			Id:           fmt.Sprintf("user-%d", i),
			PrimaryEmail: fmt.Sprintf("user-%d@example.com", i),
		})
	}

	visited := map[string]int{}
	server := newDirectoryUsersOnlyServer(t, users, 7 /* small directory page size */, func(userKey string) *reportsAdmin.Activities {
		visited[userKey]++
		return &reportsAdmin.Activities{} // no activity needed for this test
	})
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	client := &gwclient.GoogleWorkspaceClient{UserService: dir, ReportService: rep}

	var cursor string
	seenAny := true
	iterations := 0
	const maxIterations = 1000 // guard against a real infinite loop hanging the test
	for seenAny {
		iterations++
		if iterations > maxIterations {
			t.Fatalf("scanUsersForEvents did not terminate after %d iterations (possible infinite loop)", maxIterations)
		}

		_, state, err := scanUsersForEvents(context.Background(), client, "customer", "", nil, &pagination.StreamToken{Cursor: cursor},
			func(ctx context.Context, c *gwclient.GoogleWorkspaceClient, u pendingUser) ([]*v2.Event, error) {
				visited[u.Email+":lookup"]++
				return nil, nil
			})
		if err != nil {
			t.Fatalf("scanUsersForEvents: %v", err)
		}
		cursor = state.Cursor
		seenAny = state.HasMore
	}

	for i := range totalUsers {
		email := fmt.Sprintf("user-%d@example.com", i)
		if visited[email+":lookup"] != 1 {
			t.Fatalf("expected user %s to be visited exactly once, got %d", email, visited[email+":lookup"])
		}
	}
}

// TestScanUsersForEvents_FiltersEventsBeforeEarliestEvent verifies the event-feed-start-at floor:
// scanUsersForEvents drops any event whose OccurredAt is before earliestEvent, while keeping
// events at or after it, and a nil earliestEvent applies no floor at all.
func TestScanUsersForEvents_FiltersEventsBeforeEarliestEvent(t *testing.T) {
	users := []*directoryAdmin.User{{Id: "user-1", PrimaryEmail: "user-1@example.com"}}
	server := newDirectoryUsersOnlyServer(t, users, 10, func(string) *reportsAdmin.Activities {
		return &reportsAdmin.Activities{}
	})
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	client := &gwclient.GoogleWorkspaceClient{UserService: dir, ReportService: rep}

	now := time.Now().UTC()
	floor := timestamppb.New(now.Add(-1 * time.Hour))
	oldEvent := &v2.Event{Id: "old", OccurredAt: timestamppb.New(now.Add(-2 * time.Hour))}  // before floor
	newEvent := &v2.Event{Id: "new", OccurredAt: timestamppb.New(now.Add(-30 * time.Minute))} // after floor

	lookup := func(ctx context.Context, c *gwclient.GoogleWorkspaceClient, u pendingUser) ([]*v2.Event, error) {
		return []*v2.Event{oldEvent, newEvent}, nil
	}

	events, _, err := scanUsersForEvents(context.Background(), client, "customer", "", floor, &pagination.StreamToken{}, lookup)
	if err != nil {
		t.Fatalf("scanUsersForEvents: %v", err)
	}
	if len(events) != 1 || events[0].GetId() != "new" {
		t.Fatalf("expected only the event at/after earliestEvent to survive, got %v", events)
	}

	// A nil earliestEvent applies no floor: both events should come back.
	events, _, err = scanUsersForEvents(context.Background(), client, "customer", "", nil, &pagination.StreamToken{}, lookup)
	if err != nil {
		t.Fatalf("scanUsersForEvents (nil floor): %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected no filtering with a nil earliestEvent, got %d events", len(events))
	}
}

// TestUsageEventFeed_PicksLatestPerAppAndFiltersPrivateApps verifies the OAuth ("token") feed's
// per-user, per-app lookup: apps are enumerated via Tokens.list, private apps (client_id ==
// display_text and numeric) are excluded before ever issuing a Reports call for them, and for a
// real app, the query is scoped with filters=client_id==<id> and the newest of several returned
// activities wins (covering the "does maxResults=1 return newest-first?" ordering assumption).
func TestUsageEventFeed_PicksLatestPerAppAndFiltersPrivateApps(t *testing.T) {
	const userEmail = "alice@example.com"
	const privateClientID = "111111111111111111111"

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{})
			return
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{
			Users: []*directoryAdmin.User{{Id: "profile-alice", PrimaryEmail: userEmail}},
		})
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		// Tokens.list (GET .../users/{userKey}/tokens): alice authorized one real app and one
		// private (numeric client_id == display_text) app.
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{
			Items: []*directoryAdmin.Token{
				{ClientId: "client-a", DisplayText: "App A"},
				{ClientId: privateClientID, DisplayText: privateClientID},
			},
		})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		filters := r.URL.Query().Get("filters")
		if filters == "client_id=="+privateClientID {
			t.Fatalf("private app must be filtered out before ever issuing a Reports API call, got filters=%q", filters)
		}
		if filters != "client_id==client-a" {
			t.Fatalf("expected a client_id filter for the known app, got %q", filters)
		}
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{
			Items: []*reportsAdmin.Activity{
				activityItem(1, 2*time.Hour, userEmail, "profile-alice",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "client-a"},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "App A (old)"},
				),
				activityItem(2, 1*time.Minute, userEmail, "profile-alice",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "client-a"},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "App A (new)"},
				),
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")

	events, state, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if state.HasMore {
		t.Fatalf("expected single-user pass to complete in one call")
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 event (private app filtered, dup app collapsed to newest), got %d", len(events))
	}

	ue := events[0].GetUsageEvent()
	if ue == nil {
		t.Fatalf("expected a UsageEvent")
	}
	if ue.GetTargetResource().GetDisplayName() != "App A (new)" {
		t.Fatalf("expected the newer of the two same-app events to win, got %q", ue.GetTargetResource().GetDisplayName())
	}
	if ue.GetActorResource().GetId().GetResource() != "profile-alice" {
		t.Fatalf("expected actor resource id profile-alice, got %q", ue.GetActorResource().GetId().GetResource())
	}
}

// TestUsageEventFeed_DedupesRepeatedClientIDsInTokens verifies that when Tokens.list returns
// multiple Token entries for the same client_id (e.g. separate grants for different scope
// sets), the feed issues exactly one Reports API lookup for that app and emits exactly one
// event — never one per repeated Token entry.
func TestUsageEventFeed_DedupesRepeatedClientIDsInTokens(t *testing.T) {
	const userEmail = "bob@example.com"
	var reportsCalls int

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{})
			return
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{
			Users: []*directoryAdmin.User{{Id: "profile-bob", PrimaryEmail: userEmail}},
		})
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		// Bob granted "client-a" two different scope sets at different times: Tokens.list
		// returns two separate Token entries sharing the same client_id.
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{
			Items: []*directoryAdmin.Token{
				{ClientId: "client-a", DisplayText: "App A"},
				{ClientId: "client-a", DisplayText: "App A"},
			},
		})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		reportsCalls++
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{
			Items: []*reportsAdmin.Activity{
				activityItem(1, 1*time.Minute, userEmail, "profile-bob",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "client-a"},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "App A"},
				),
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")

	events, _, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if reportsCalls != 1 {
		t.Fatalf("expected exactly 1 Reports API call for the deduped app, got %d", reportsCalls)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 event for the deduped app, got %d", len(events))
	}
}
