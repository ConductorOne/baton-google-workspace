package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/pagination"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// newUsageFeedTestServer builds a minimal httptest.Server for the token/authorize endpoint.
// activitiesResp is returned for every request; capturedEndTime (if non-nil) records the
// endTime query parameter seen by the handler.
func newUsageFeedTestServer(activitiesResp *reportsAdmin.Activities, capturedEndTime *string) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/reports/v1/activity/users/all/applications/token", func(w http.ResponseWriter, r *http.Request) {
		if capturedEndTime != nil {
			if et := r.URL.Query().Get("endTime"); et != "" {
				*capturedEndTime = et
			}
		}
		_ = json.NewEncoder(w).Encode(activitiesResp)
	})
	return httptest.NewServer(mux)
}

func newUsageFeedClient(t *testing.T, server *httptest.Server) *gwclient.GoogleWorkspaceClient {
	t.Helper()
	rep := newReportsService(t, server.URL, server.Client())
	return &gwclient.GoogleWorkspaceClient{ReportService: rep}
}

// encodeCursor marshals a pageToken and base64-encodes it, mimicking what cursor.marshal() does.
func encodeCursor(t *testing.T, pt pageToken) string {
	t.Helper()
	data, err := json.Marshal(pt)
	if err != nil {
		t.Fatalf("encodeCursor: json.Marshal: %v", err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

// decodeCursor extracts the pageToken from a StreamState cursor.
func decodeCursor(t *testing.T, cursor string) pageToken {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatalf("decodeCursor: base64 decode: %v", err)
	}
	var pt pageToken
	if err := json.Unmarshal(data, &pt); err != nil {
		t.Fatalf("decodeCursor: json.Unmarshal: %v", err)
	}
	return pt
}

func TestUsageEventFeed_EndTimeSentInRequest(t *testing.T) {
	var capturedEndTime string
	server := newUsageFeedTestServer(&reportsAdmin.Activities{}, &capturedEndTime)
	defer server.Close()

	client := newUsageFeedClient(t, server)
	feed := newUsageEventFeed(client)

	startAt := timestamppb.New(time.Now().Add(-30 * 24 * time.Hour))
	_, _, _, err := feed.ListEvents(context.Background(), startAt, &pagination.StreamToken{Size: 100})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if capturedEndTime == "" {
		t.Fatal("expected endTime to be sent in request, got none")
	}
	_, parseErr := time.Parse(time.RFC3339, capturedEndTime)
	if parseErr != nil {
		t.Fatalf("endTime is not valid RFC3339: %q", capturedEndTime)
	}
}

func TestUsageEventFeed_ChunkAdvancesWhenEndAtIsPast(t *testing.T) {
	// EndAt is 20 days ago (well past the catch-up buffer), so the feed should
	// advance to the next chunk and return HasMore=true.
	server := newUsageFeedTestServer(&reportsAdmin.Activities{}, nil)
	defer server.Close()

	client := newUsageFeedClient(t, server)
	feed := newUsageEventFeed(client)

	now := time.Now()
	startAt := now.Add(-30 * 24 * time.Hour)
	endAt := now.Add(-20 * 24 * time.Hour) // 20 days ago

	cursorStr := encodeCursor(t, pageToken{
		StartAt:         startAt.UTC().Format(time.RFC3339),
		EndAt:           endAt.UTC().Format(time.RFC3339),
		LatestEventSeen: startAt.UTC().Format(time.RFC3339),
	})
	st := &pagination.StreamToken{Size: 100, Cursor: cursorStr}

	_, state, _, err := feed.ListEvents(context.Background(), nil, st)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if !state.HasMore {
		t.Fatal("expected HasMore=true when EndAt is in the past (more chunks remain)")
	}

	nextCursor := decodeCursor(t, state.Cursor)
	// StartAt should have advanced to the old EndAt.
	if nextCursor.StartAt != endAt.UTC().Format(time.RFC3339) {
		t.Errorf("StartAt: got %q, want %q", nextCursor.StartAt, endAt.UTC().Format(time.RFC3339))
	}
	// EndAt should be endAt + chunkDuration, capped at now.
	expectedEnd := endAt.Add(eventFeedChunkDuration)
	if expectedEnd.After(now) {
		expectedEnd = now
	}
	nextEnd, parseErr := time.Parse(time.RFC3339, nextCursor.EndAt)
	if parseErr != nil {
		t.Fatalf("next EndAt is not valid RFC3339: %q", nextCursor.EndAt)
	}
	if nextEnd.Before(expectedEnd.Add(-time.Minute)) || nextEnd.After(now.Add(time.Minute)) {
		t.Errorf("next EndAt %v is outside expected range [%v, now]", nextEnd, expectedEnd)
	}
}

func TestUsageEventFeed_CaughtUpWhenEndAtIsRecent(t *testing.T) {
	// EndAt is 1 minute ago (within the catch-up buffer), so the feed should
	// signal caught-up (HasMore=false).
	server := newUsageFeedTestServer(&reportsAdmin.Activities{}, nil)
	defer server.Close()

	client := newUsageFeedClient(t, server)
	feed := newUsageEventFeed(client)

	now := time.Now()
	startAt := now.Add(-2 * time.Minute)
	endAt := now.Add(-1 * time.Minute) // within eventFeedCatchUpBuffer

	cursorStr := encodeCursor(t, pageToken{
		StartAt:         startAt.UTC().Format(time.RFC3339),
		EndAt:           endAt.UTC().Format(time.RFC3339),
		LatestEventSeen: startAt.UTC().Format(time.RFC3339),
	})
	st := &pagination.StreamToken{Size: 100, Cursor: cursorStr}

	_, state, _, err := feed.ListEvents(context.Background(), nil, st)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if state.HasMore {
		t.Fatal("expected HasMore=false when EndAt is within catch-up buffer (caught up)")
	}
	// After catch-up, StartAt should advance to EndAt for the next run.
	nextCursor := decodeCursor(t, state.Cursor)
	if nextCursor.StartAt != endAt.UTC().Format(time.RFC3339) {
		t.Errorf("StartAt after catch-up: got %q, want %q", nextCursor.StartAt, endAt.UTC().Format(time.RFC3339))
	}
	// EndAt should be cleared so it gets re-initialised on the next run.
	if nextCursor.EndAt != "" {
		t.Errorf("EndAt after catch-up should be empty, got %q", nextCursor.EndAt)
	}
}

func TestUsageEventFeed_BackwardCompatOldCursorMidPagination(t *testing.T) {
	// An old cursor (pre-chunking) has NextPageToken but no EndAt.
	// The feed should continue without setting endTime in the request.
	var capturedEndTime string
	server := newUsageFeedTestServer(&reportsAdmin.Activities{NextPageToken: ""}, &capturedEndTime)
	defer server.Close()

	client := newUsageFeedClient(t, server)
	feed := newUsageEventFeed(client)

	now := time.Now()
	// Simulate an old cursor: has NextPageToken but no EndAt.
	cursorStr := encodeCursor(t, pageToken{
		StartAt:         now.Add(-5 * 24 * time.Hour).UTC().Format(time.RFC3339),
		LatestEventSeen: now.Add(-5 * 24 * time.Hour).UTC().Format(time.RFC3339),
		NextPageToken:   "old-page-token",
		// EndAt deliberately absent.
	})
	st := &pagination.StreamToken{Size: 100, Cursor: cursorStr}

	_, _, _, err := feed.ListEvents(context.Background(), nil, st)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	// No endTime should have been sent for a legacy cursor still mid-pagination.
	if capturedEndTime != "" {
		t.Errorf("expected no endTime for backward-compat cursor, got %q", capturedEndTime)
	}
}

func TestUsageEventFeed_NewCursorInitializesChunk(t *testing.T) {
	// A brand-new cursor (no Cursor field in StreamToken) should initialize a chunk
	// with EndAt = startAt + chunkDuration (or now if that's sooner).
	server := newUsageFeedTestServer(&reportsAdmin.Activities{}, nil)
	defer server.Close()

	client := newUsageFeedClient(t, server)
	feed := newUsageEventFeed(client)

	// Start 30 days ago; expect EndAt = 30 days ago + 7 days = 23 days ago.
	startAt := timestamppb.New(time.Now().Add(-30 * 24 * time.Hour))
	st := &pagination.StreamToken{Size: 100}

	_, state, _, err := feed.ListEvents(context.Background(), startAt, st)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}

	// HasMore should be true (23 days ago → more chunks remain).
	if !state.HasMore {
		t.Fatal("expected HasMore=true for a 30-day backfill on first call")
	}

	nextCursor := decodeCursor(t, state.Cursor)
	nextStart, err := time.Parse(time.RFC3339, nextCursor.StartAt)
	if err != nil {
		t.Fatalf("StartAt not valid RFC3339: %q", nextCursor.StartAt)
	}
	expectedChunkEnd := startAt.AsTime().Add(eventFeedChunkDuration)
	// Allow ±1 minute tolerance for test execution time.
	if diff := nextStart.Sub(expectedChunkEnd); diff < -time.Minute || diff > time.Minute {
		t.Errorf("StartAt after first chunk: got %v, expected ~%v (diff %v)", nextStart, expectedChunkEnd, diff)
	}
}

func TestUsageEventFeed_EventsReturnedWithinChunk(t *testing.T) {
	// Verify that events from the response are correctly returned.
	now := time.Now()
	eventTime := now.Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	resp := &reportsAdmin.Activities{
		Items: []*reportsAdmin.Activity{{
			Id:    &reportsAdmin.ActivityId{Time: eventTime, UniqueQualifier: 42},
			Actor: &reportsAdmin.ActivityActor{Email: "user@example.com", ProfileId: "uid-1"},
			Events: []*reportsAdmin.ActivityEvents{{
				Name: "authorize",
				Parameters: []*reportsAdmin.ActivityEventsParameters{
					{Name: "client_id", Value: "client-abc"},
					{Name: "app_name", Value: "TestApp"},
				},
			}},
		}},
	}
	server := newUsageFeedTestServer(resp, nil)
	defer server.Close()

	client := newUsageFeedClient(t, server)
	feed := newUsageEventFeed(client)

	startAt := timestamppb.New(now.Add(-2 * time.Hour))
	endAt := now.Add(-30 * time.Minute)

	cursorStr := encodeCursor(t, pageToken{
		StartAt:         startAt.AsTime().UTC().Format(time.RFC3339),
		EndAt:           endAt.UTC().Format(time.RFC3339),
		LatestEventSeen: startAt.AsTime().UTC().Format(time.RFC3339),
	})
	st := &pagination.StreamToken{Size: 100, Cursor: cursorStr}

	events, _, _, err := feed.ListEvents(context.Background(), nil, st)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ue := events[0].GetUsageEvent()
	if ue == nil {
		t.Fatal("expected UsageEvent, got nil")
	}
	if ue.TargetResource.GetId().GetResource() != "client-abc" {
		t.Errorf("target resource ID: got %q, want %q", ue.TargetResource.GetId().GetResource(), "client-abc")
	}
	_ = fmt.Sprintf("actor: %s", ue.ActorResource.GetDisplayName()) // exercise actor field
}
