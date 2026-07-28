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

		_, state, err := scanUsersForEvents(context.Background(), client, "customer", "", &pagination.StreamToken{Cursor: cursor},
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

// TestUsageEventFeed_PicksLatestPerAppAndFiltersPrivateApps verifies the OAuth ("token") feed's
// per-user lookup: given a bounded window containing older and newer logins to the same app,
// it keeps only the newest per app; and it excludes private apps (client_id == app_name and
// numeric), matching the pre-refactor filtering behavior.
func TestUsageEventFeed_PicksLatestPerAppAndFiltersPrivateApps(t *testing.T) {
	const userEmail = "alice@example.com"
	server := newDirectoryUsersOnlyServer(t,
		[]*directoryAdmin.User{{Id: "profile-alice", PrimaryEmail: userEmail}},
		10,
		func(userKey string) *reportsAdmin.Activities {
			if userKey != userEmail {
				return &reportsAdmin.Activities{}
			}
			return &reportsAdmin.Activities{
				Items: []*reportsAdmin.Activity{
					activityItem(1, 2*time.Hour, userEmail, "profile-alice",
						&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "client-a"},
						&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "App A (old)"},
					),
					activityItem(2, 1*time.Minute, userEmail, "profile-alice",
						&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "client-a"},
						&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "App A (new)"},
					),
					activityItem(3, 30*time.Second, userEmail, "profile-alice",
						&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "111111111111111111111"},
						&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "111111111111111111111"},
					),
				},
			}
		})
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, ReportService: rep}, "customer", "")

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
