package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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
			func(ctx context.Context, c *gwclient.GoogleWorkspaceClient, u pendingUser, _ string, _ int, _ time.Time) ([]*v2.Event, string, int, error) {
				visited[u.Email+":lookup"]++
				return nil, "", 1, nil
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
	oldEvent := &v2.Event{Id: "old", OccurredAt: timestamppb.New(now.Add(-2 * time.Hour))}    // before floor
	newEvent := &v2.Event{Id: "new", OccurredAt: timestamppb.New(now.Add(-30 * time.Minute))} // after floor

	lookup := func(ctx context.Context, c *gwclient.GoogleWorkspaceClient, u pendingUser, _ string, _ int, _ time.Time) ([]*v2.Event, string, int, error) {
		return []*v2.Event{oldEvent, newEvent}, "", 1, nil
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

// TestUsageEventFeed_LookupUserStopsAtDeadlineBetweenChunks verifies that lookupUser's per-app
// fan-out (usage_event_feed.go) respects the deadline passed in from scanUsersForEvents: once
// past deadline, it stops dispatching new app lookups rather than draining the whole remaining
// budget in one call, but it always dispatches at least the first app (the deadline is only
// checked *before dispatching the second and later apps*) so a single call still makes forward
// progress even if the deadline was already past when the call started.
func TestUsageEventFeed_LookupUserStopsAtDeadlineBetweenChunks(t *testing.T) {
	const userEmail = "heavy@example.com"
	const numApps = maxConcurrentAppLookups*2 + 3 // more than one chunk's worth

	tokens := make([]*directoryAdmin.Token, 0, numApps)
	for i := 0; i < numApps; i++ {
		tokens = append(tokens, &directoryAdmin.Token{ClientId: fmt.Sprintf("client-%d", i), DisplayText: fmt.Sprintf("App %d", i)})
	}

	var mu sync.Mutex
	callCounts := map[string]int{}

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{Items: tokens})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		clientID := strings.TrimPrefix(r.URL.Query().Get("filters"), "client_id==")
		mu.Lock()
		callCounts[clientID]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{
			Items: []*reportsAdmin.Activity{
				activityItem(1, time.Minute, userEmail, "profile-heavy",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: clientID},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: clientID},
				),
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")
	user := pendingUser{Email: userEmail, ID: "heavy-user"}

	// A deadline already in the past: only the check before dispatching the second (and later)
	// app sees it, so the first app is still dispatched and awaited before lookupUser bails out.
	events, nextResume, consumed, err := feed.lookupUser(context.Background(), feed.c, user, "", numApps, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("lookupUser: %v", err)
	}
	if consumed != 1 {
		t.Fatalf("expected exactly one app to be consumed before bailing out on the past deadline, got %d", consumed)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly one event (for the one dispatched app), got %d", len(events))
	}
	wantNextResume := "client-0"
	if nextResume != wantNextResume {
		t.Fatalf("expected nextResume to be the one app dispatched (%s), got %q", wantNextResume, nextResume)
	}
	if callCounts["client-0"] != 1 {
		t.Fatalf("expected app client-0 to be queried exactly once, got %d", callCounts["client-0"])
	}
	for i := 1; i < numApps; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		if callCounts[clientID] != 0 {
			t.Fatalf("expected app %s not to be queried once the deadline had passed, got %d", clientID, callCounts[clientID])
		}
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

// TestScanUsersForEvents_ResumesWithinUserWhenBudgetExhausted verifies that a user needing more
// Reports calls than the budget allows pauses mid-user, resumes via cursor state across calls,
// and has every unit of work visited exactly once. Uses a synthetic lookup to isolate
// scanUsersForEvents' generic budget/resume mechanics from any one feed's fan-out logic.
func TestScanUsersForEvents_ResumesWithinUserWhenBudgetExhausted(t *testing.T) {
	const totalUnits = maxLookupCallsPerEventFeedCall*2 + 7 // spans exactly 3 resumed calls
	user := pendingUser{Email: "heavy@example.com", ID: "heavy-user"}
	unitLookupCounts := map[int]int{}

	lookup := func(ctx context.Context, c *gwclient.GoogleWorkspaceClient, u pendingUser, resumeState string, budget int, _ time.Time) ([]*v2.Event, string, int, error) {
		startIdx := 0
		if resumeState != "" {
			idx, err := strconv.Atoi(resumeState)
			if err != nil {
				t.Fatalf("bad resume state %q: %v", resumeState, err)
			}
			startIdx = idx
		}
		consumed := totalUnits - startIdx
		if consumed > budget {
			consumed = budget
		}
		events := make([]*v2.Event, 0, consumed)
		for i := startIdx; i < startIdx+consumed; i++ {
			unitLookupCounts[i]++
			events = append(events, &v2.Event{Id: strconv.Itoa(i)})
		}
		nextResume := ""
		if startIdx+consumed < totalUnits {
			nextResume = strconv.Itoa(startIdx + consumed)
		}
		return events, nextResume, consumed, nil
	}

	server := newDirectoryUsersOnlyServer(t, []*directoryAdmin.User{{Id: user.ID, PrimaryEmail: user.Email}}, 10,
		func(string) *reportsAdmin.Activities { return &reportsAdmin.Activities{} })
	defer server.Close()
	dir := newTestDirectoryService(t, server.URL, server.Client())
	client := &gwclient.GoogleWorkspaceClient{UserService: dir}

	var cursor string
	var allEvents []*v2.Event
	calls := 0
	for {
		calls++
		if calls > 10 {
			t.Fatalf("expected this to resolve in a small, bounded number of calls, got stuck after %d", calls)
		}
		events, state, err := scanUsersForEvents(context.Background(), client, "customer", "", nil, &pagination.StreamToken{Cursor: cursor}, lookup)
		if err != nil {
			t.Fatalf("scanUsersForEvents: %v", err)
		}
		allEvents = append(allEvents, events...)
		cursor = state.Cursor
		if !state.HasMore {
			break
		}
	}

	if calls != 3 {
		t.Fatalf("expected exactly 3 calls (budget=%d, units=%d), got %d", maxLookupCallsPerEventFeedCall, totalUnits, calls)
	}
	if len(allEvents) != totalUnits {
		t.Fatalf("expected exactly %d events (one per unit), got %d", totalUnits, len(allEvents))
	}
	for i := 0; i < totalUnits; i++ {
		if unitLookupCounts[i] != 1 {
			t.Fatalf("expected unit %d to be looked up exactly once, got %d", i, unitLookupCounts[i])
		}
	}
}

// withUnlimitedReportsRateLimiter swaps sharedReportsRateLimiter for a large-capacity one for the
// test's duration, so tests issuing many real Reports calls aren't order-dependent on other tests.
func withUnlimitedReportsRateLimiter(t *testing.T) {
	t.Helper()
	original := sharedReportsRateLimiter
	sharedReportsRateLimiter = newReportsRateLimiter(1_000_000)
	t.Cleanup(func() { sharedReportsRateLimiter = original })
}

// TestUsageEventFeed_ResumesAcrossManyAuthorizedApps is the end-to-end version of the test above,
// exercising usage_event_feed's real lookupUser against a user with more authorized apps than
// maxLookupCallsPerEventFeedCall allows per call.
func TestUsageEventFeed_ResumesAcrossManyAuthorizedApps(t *testing.T) {
	withUnlimitedReportsRateLimiter(t)

	const userEmail = "heavy@example.com"
	const numApps = maxLookupCallsPerEventFeedCall + 15 // forces exactly 2 resumed calls

	tokens := make([]*directoryAdmin.Token, 0, numApps)
	for i := 0; i < numApps; i++ {
		tokens = append(tokens, &directoryAdmin.Token{ClientId: fmt.Sprintf("client-%d", i), DisplayText: fmt.Sprintf("App %d", i)})
	}

	var mu sync.Mutex
	callCounts := map[string]int{}

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{})
			return
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{
			Users: []*directoryAdmin.User{{Id: "profile-heavy", PrimaryEmail: userEmail}},
		})
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{Items: tokens})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		clientID := strings.TrimPrefix(r.URL.Query().Get("filters"), "client_id==")
		mu.Lock()
		callCounts[clientID]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{
			Items: []*reportsAdmin.Activity{
				activityItem(1, time.Minute, userEmail, "profile-heavy",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: clientID},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: clientID},
				),
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")

	var cursor string
	var allEvents []*v2.Event
	calls := 0
	for {
		calls++
		if calls > 10 {
			t.Fatalf("expected this to resolve in a small, bounded number of calls, got stuck after %d", calls)
		}
		events, state, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{Cursor: cursor})
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		allEvents = append(allEvents, events...)
		cursor = state.Cursor
		if !state.HasMore {
			break
		}
	}

	if calls != 2 {
		t.Fatalf("expected exactly 2 ListEvents calls (budget=%d, apps=%d), got %d", maxLookupCallsPerEventFeedCall, numApps, calls)
	}
	if len(allEvents) != numApps {
		t.Fatalf("expected %d events (one per app), got %d", numApps, len(allEvents))
	}
	if len(callCounts) != numApps {
		t.Fatalf("expected %d distinct apps queried, got %d", numApps, len(callCounts))
	}
	for clientID, n := range callCounts {
		if n != 1 {
			t.Fatalf("expected exactly 1 Reports API call for %s, got %d", clientID, n)
		}
	}
}

// TestUsageEventFeed_ResumeSurvivesAppListShift verifies that resuming a user's authorized-app
// lookup by client_id (not by positional index) tolerates the app list shifting between the
// first and second ListEvents call. A revoked app that was already processed disappears from
// Tokens.list, shifting every later app's position down by one; a positional-index resume would
// then skip whatever app lands at the old index in the shifted list, silently losing an event.
// Resuming by client_id instead re-locates the last-processed app in the fresh list (wherever it
// now sits) and continues right after it, so every not-yet-processed app is still visited exactly
// once.
func TestUsageEventFeed_ResumeSurvivesAppListShift(t *testing.T) {
	withUnlimitedReportsRateLimiter(t)

	const userEmail = "heavy@example.com"
	const numApps = maxLookupCallsPerEventFeedCall + 15 // forces exactly 2 resumed calls
	const revokedAfterFirstCall = "client-5"             // processed in call 1, then revoked

	var mu sync.Mutex
	callCounts := map[string]int{}
	tokensCalls := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{})
			return
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{
			Users: []*directoryAdmin.User{{Id: "profile-heavy", PrimaryEmail: userEmail}},
		})
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tokensCalls++
		shifted := tokensCalls > 1
		mu.Unlock()

		tokens := make([]*directoryAdmin.Token, 0, numApps)
		for i := 0; i < numApps; i++ {
			clientID := fmt.Sprintf("client-%d", i)
			if shifted && clientID == revokedAfterFirstCall {
				// Revoked between calls: every later app's index shifts down by one.
				continue
			}
			tokens = append(tokens, &directoryAdmin.Token{ClientId: clientID, DisplayText: clientID})
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{Items: tokens})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		clientID := strings.TrimPrefix(r.URL.Query().Get("filters"), "client_id==")
		mu.Lock()
		callCounts[clientID]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{
			Items: []*reportsAdmin.Activity{
				activityItem(1, time.Minute, userEmail, "profile-heavy",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: clientID},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: clientID},
				),
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")

	var cursor string
	calls := 0
	for {
		calls++
		if calls > 10 {
			t.Fatalf("expected this to resolve in a small, bounded number of calls, got stuck after %d", calls)
		}
		_, state, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{Cursor: cursor})
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		cursor = state.Cursor
		if !state.HasMore {
			break
		}
	}

	if calls != 2 {
		t.Fatalf("expected exactly 2 ListEvents calls, got %d", calls)
	}
	// Every app must be queried exactly once, including the ones after the revoked app whose
	// position shifted. A positional-index resume would skip one of them here.
	if len(callCounts) != numApps {
		t.Fatalf("expected %d distinct apps queried, got %d: %v", numApps, len(callCounts), callCounts)
	}
	for clientID, n := range callCounts {
		if n != 1 {
			t.Fatalf("expected exactly 1 Reports API call for %s, got %d", clientID, n)
		}
	}
}

// TestUsageEventFeed_ResumeRestartsWhenAnchorAppRevoked locks in lookupUser's "anchor not
// found" fallback (usage_event_feed.go): if the exact app the resume cursor anchors on — the
// last one processed in a prior call — is itself revoked before the next resumed call, the
// fresh app list no longer contains it, so lookupUser can't re-locate a resume position and
// instead restarts this user's app scan from the beginning. This is intentionally conservative
// (it never skips an app), but it isn't free: every app already processed before the anchor gets
// looked up — and its event re-emitted — a second time. TestUsageEventFeed_ResumeSurvivesAppListShift
// revokes an app *before* the anchor, which leaves the anchor itself findable and never exercises
// this restart path.
func TestUsageEventFeed_ResumeRestartsWhenAnchorAppRevoked(t *testing.T) {
	withUnlimitedReportsRateLimiter(t)

	const userEmail = "heavy@example.com"
	const numApps = maxLookupCallsPerEventFeedCall + 15 // forces exactly 2 resumed calls
	anchorClientID := fmt.Sprintf("client-%d", maxLookupCallsPerEventFeedCall-1)

	var mu sync.Mutex
	callCounts := map[string]int{}
	tokensCalls := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{})
			return
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{
			Users: []*directoryAdmin.User{{Id: "profile-heavy", PrimaryEmail: userEmail}},
		})
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tokensCalls++
		anchorRevoked := tokensCalls > 1
		mu.Unlock()

		tokens := make([]*directoryAdmin.Token, 0, numApps)
		for i := 0; i < numApps; i++ {
			clientID := fmt.Sprintf("client-%d", i)
			if anchorRevoked && clientID == anchorClientID {
				// The exact app the resume cursor anchors on is revoked before the next call.
				continue
			}
			tokens = append(tokens, &directoryAdmin.Token{ClientId: clientID, DisplayText: clientID})
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{Items: tokens})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		clientID := strings.TrimPrefix(r.URL.Query().Get("filters"), "client_id==")
		mu.Lock()
		callCounts[clientID]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{
			Items: []*reportsAdmin.Activity{
				activityItem(1, time.Minute, userEmail, "profile-heavy",
					&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: clientID},
					&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: clientID},
				),
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")

	var cursor string
	calls := 0
	for {
		calls++
		if calls > 10 {
			t.Fatalf("expected this to resolve in a small, bounded number of calls, got stuck after %d", calls)
		}
		_, state, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{Cursor: cursor})
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		cursor = state.Cursor
		if !state.HasMore {
			break
		}
	}

	if calls != 3 {
		t.Fatalf("expected exactly 3 ListEvents calls (call 1, the restart, and the tail), got %d", calls)
	}

	// The revoked anchor app was queried exactly once, in call 1, before it disappeared.
	if callCounts[anchorClientID] != 1 {
		t.Fatalf("expected the revoked anchor app %s to be queried exactly once, got %d", anchorClientID, callCounts[anchorClientID])
	}

	// Every app processed before the anchor in call 1 gets replayed once the restart-from-0
	// fallback kicks in — the bounded, non-skipping cost of the conservative fallback.
	for i := 0; i < maxLookupCallsPerEventFeedCall-1; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		if callCounts[clientID] != 2 {
			t.Fatalf("expected app %s (processed before the revoked anchor) to be replayed once after the restart, got %d calls", clientID, callCounts[clientID])
		}
	}

	// No app is ever skipped: every app still present after the revocation is queried at least
	// once.
	for i := maxLookupCallsPerEventFeedCall; i < numApps; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		if callCounts[clientID] < 1 {
			t.Fatalf("expected app %s to be queried at least once, got %d", clientID, callCounts[clientID])
		}
	}
}
