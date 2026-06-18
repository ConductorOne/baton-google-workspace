package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/pagination"
	reportsAdmin "google.golang.org/api/admin/reports/v1"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// newUsageFeedTestServer returns an httptest.Server that responds to the Reports
// activities.list endpoint used by the usage event feed with the provided payload.
func newUsageFeedTestServer(payload *reportsAdmin.Activities) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/reports/v1/activity/users/all/applications/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(payload)
	})
	return httptest.NewServer(mux)
}

// buildUsageActivity constructs a Reports Activity whose events will round-trip
// through newV2Event without being filtered out.
func buildUsageActivity(at time.Time, uniqueQualifier int64, clientID, appName string) *reportsAdmin.Activity {
	return &reportsAdmin.Activity{
		Id: &reportsAdmin.ActivityId{
			Time:            at.Format(time.RFC3339),
			UniqueQualifier: uniqueQualifier,
		},
		Actor: &reportsAdmin.ActivityActor{
			Email:     "actor@example.com",
			ProfileId: "actor-profile-id",
		},
		Events: []*reportsAdmin.ActivityEvents{
			{
				Type: "login",
				Name: "authorize",
				Parameters: []*reportsAdmin.ActivityEventsParameters{
					{Name: "client_id", Value: clientID},
					{Name: "app_name", Value: appName},
				},
			},
		},
	}
}

// encodeCursor returns the base64-encoded JSON payload accepted by the feed.
func encodeCursor(t *testing.T, pt pageToken) string {
	t.Helper()
	raw, err := json.Marshal(pt)
	if err != nil {
		t.Fatalf("encodeCursor: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// decodeCursor extracts the pageToken returned in a StreamState.Cursor.
func decodeCursor(t *testing.T, cursor string) pageToken {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	var pt pageToken
	if err := json.Unmarshal(raw, &pt); err != nil {
		t.Fatalf("decodeCursor unmarshal: %v", err)
	}
	return pt
}

// invokeListEvents calls usageEventFeed.ListEvents with a synthetic cursor and
// returns the resulting cursor and stream state for assertion.
func invokeListEvents(t *testing.T, payload *reportsAdmin.Activities, in pageToken) (pageToken, *pagination.StreamState) {
	t.Helper()
	server := newUsageFeedTestServer(payload)
	t.Cleanup(server.Close)

	rep := newReportsService(t, server.URL, server.Client())
	client := &gwclient.GoogleWorkspaceClient{ReportService: rep}
	feed := newUsageEventFeed(client)

	tok := &pagination.StreamToken{Size: 100, Cursor: encodeCursor(t, in)}
	_, state, _, err := feed.ListEvents(context.Background(), nil, tok)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if state == nil {
		t.Fatalf("ListEvents returned nil StreamState")
	}
	return decodeCursor(t, state.Cursor), state
}

// TestUsageFeed_CommitsOnEmptyNextPageToken verifies the existing behavior:
// when Google returns an empty NextPageToken the cursor advances to
// LatestEventSeen and pagination state is cleared.
func TestUsageFeed_CommitsOnEmptyNextPageToken(t *testing.T) {
	startAt := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Second)
	eventAt := startAt.Add(24 * time.Hour)

	payload := &reportsAdmin.Activities{
		Items:         []*reportsAdmin.Activity{buildUsageActivity(eventAt, 1, "client-a", "App A")},
		NextPageToken: "",
	}
	in := pageToken{StartAt: startAt.Format(time.RFC3339)}

	out, state := invokeListEvents(t, payload, in)

	if state.HasMore {
		t.Fatalf("expected HasMore=false")
	}
	if out.NextPageToken != "" || out.LatestEventSeen != "" || out.EarliestEventSeen != "" {
		t.Fatalf("expected pagination state cleared, got %+v", out)
	}
	if got := out.StartAt; got != eventAt.Format(time.RFC3339) {
		t.Fatalf("expected StartAt=%s, got %s", eventAt.Format(time.RFC3339), got)
	}
}

// TestUsageFeed_KeepsPaginatingWhenMorePagesAvailable verifies that with a
// non-empty NextPageToken and no back-drain signal the cursor preserves
// NextPageToken and continues without advancing StartAt.
func TestUsageFeed_KeepsPaginatingWhenMorePagesAvailable(t *testing.T) {
	startAt := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Second)
	eventAt := startAt.Add(15 * 24 * time.Hour)

	payload := &reportsAdmin.Activities{
		Items:         []*reportsAdmin.Activity{buildUsageActivity(eventAt, 2, "client-b", "App B")},
		NextPageToken: "next-token-xyz",
	}
	in := pageToken{StartAt: startAt.Format(time.RFC3339)}

	out, state := invokeListEvents(t, payload, in)

	if !state.HasMore {
		t.Fatalf("expected HasMore=true while NextPageToken is set")
	}
	if out.NextPageToken != "next-token-xyz" {
		t.Fatalf("expected NextPageToken preserved, got %q", out.NextPageToken)
	}
	if out.StartAt != startAt.Format(time.RFC3339) {
		t.Fatalf("expected StartAt unchanged at %s, got %s", startAt.Format(time.RFC3339), out.StartAt)
	}
	if out.LatestEventSeen == "" || out.EarliestEventSeen == "" {
		t.Fatalf("expected LatestEventSeen and EarliestEventSeen populated, got %+v", out)
	}
}

// TestUsageFeed_BackwardsCompatibleLegacyCursor verifies that a cursor written
// by the previous version of this code (no EarliestEventSeen field) deserializes
// cleanly and the feed continues paginating without losing state. The first
// batch processed under the new code populates EarliestEventSeen from the page
// contents, so subsequent invocations have the full set of cursor fields.
func TestUsageFeed_BackwardsCompatibleLegacyCursor(t *testing.T) {
	startAt := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Second)
	latestSeen := startAt.Add(20 * 24 * time.Hour)
	newPageEvent := startAt.Add(10 * 24 * time.Hour) // older than latest, newer than start

	// Build a legacy cursor JSON with only the fields that existed before this
	// change. Encoding via a plain map (rather than pageToken with
	// EarliestEventSeen omitted) guarantees the JSON has no earliest_event_seen
	// key at all, mirroring real persisted cursors.
	legacy := map[string]any{
		"latest_event_seen": latestSeen.Format(time.RFC3339),
		"next_page_token":   "resume-from-here",
		"start_at":          startAt.Format(time.RFC3339),
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	legacyCursor := base64.StdEncoding.EncodeToString(raw)

	payload := &reportsAdmin.Activities{
		Items:         []*reportsAdmin.Activity{buildUsageActivity(newPageEvent, 6, "client-f", "App F")},
		NextPageToken: "next-page-after-resume",
	}

	server := newUsageFeedTestServer(payload)
	t.Cleanup(server.Close)
	rep := newReportsService(t, server.URL, server.Client())
	client := &gwclient.GoogleWorkspaceClient{ReportService: rep}
	feed := newUsageEventFeed(client)

	tok := &pagination.StreamToken{Size: 100, Cursor: legacyCursor}
	_, state, _, err := feed.ListEvents(context.Background(), nil, tok)
	if err != nil {
		t.Fatalf("ListEvents (legacy cursor): %v", err)
	}
	if state == nil || !state.HasMore {
		t.Fatalf("expected HasMore=true while still paginating")
	}

	out := decodeCursor(t, state.Cursor)
	if out.StartAt != startAt.Format(time.RFC3339) {
		t.Fatalf("expected StartAt preserved from legacy cursor, got %s", out.StartAt)
	}
	if out.NextPageToken != payload.NextPageToken {
		t.Fatalf("expected NextPageToken updated to %q, got %q", payload.NextPageToken, out.NextPageToken)
	}
	// New field populated from the first batch processed under the new code.
	if out.EarliestEventSeen != newPageEvent.Format(time.RFC3339) {
		t.Fatalf("expected EarliestEventSeen populated from batch, got %q", out.EarliestEventSeen)
	}
	// LatestEventSeen should advance only if a newer event arrives; this batch
	// has an older event than the legacy LatestEventSeen, so it must not regress.
	if out.LatestEventSeen != latestSeen.Format(time.RFC3339) {
		t.Fatalf("expected LatestEventSeen preserved at %s, got %s", latestSeen.Format(time.RFC3339), out.LatestEventSeen)
	}
}

// TestUsageFeed_CommitsWhenPaginationReachesStartAt verifies the defensive
// drain check: if Google returns events at or before our requested StartAt
// (the pagination has reached the bottom of the requested range) we commit
// even if NextPageToken is non-empty. This protects against API misbehavior
// where NextPageToken would otherwise never become empty.
func TestUsageFeed_CommitsWhenPaginationReachesStartAt(t *testing.T) {
	startAt := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Second)
	// An event whose occurredAt is at-or-before startAt signals back-drain.
	backDrainEvent := startAt
	newerEvent := startAt.Add(15 * 24 * time.Hour)

	payload := &reportsAdmin.Activities{
		Items: []*reportsAdmin.Activity{
			buildUsageActivity(newerEvent, 4, "client-d", "App D"),
			buildUsageActivity(backDrainEvent, 5, "client-e", "App E"),
		},
		NextPageToken: "shouldnt-matter",
	}
	in := pageToken{StartAt: startAt.Format(time.RFC3339)}

	out, state := invokeListEvents(t, payload, in)

	if state.HasMore {
		t.Fatalf("expected HasMore=false after back-drain commit")
	}
	if out.NextPageToken != "" {
		t.Fatalf("expected NextPageToken cleared after back-drain, got %q", out.NextPageToken)
	}
	if out.StartAt != newerEvent.Format(time.RFC3339) {
		t.Fatalf("expected StartAt advanced to %s, got %s", newerEvent.Format(time.RFC3339), out.StartAt)
	}
}
