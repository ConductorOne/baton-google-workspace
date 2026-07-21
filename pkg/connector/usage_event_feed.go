package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

var privateAppIDRegex = regexp.MustCompile("[0-9]{21}")

// usageActivitiesPageSize is the number of activity items requested per ListActivities
// call. The Google Reports API maximum is 1000.
const usageActivitiesPageSize = 1000

// maxEventFeedLookback caps how far back event feeds query the Google Reports API.
// Google page tokens expire after ~24h, so a cursor left mid-pagination (e.g. after
// a connector restart or a transient timeout) would otherwise keep requesting the
// full historical window on every retry, causing HTTP timeout death spirals on large
// orgs. 90 days balances sufficient event history against query size; Google retains
// Reports data for 6 months so there is headroom if the window needs to grow.
const maxEventFeedLookback = 90 * 24 * time.Hour

// eventFeedChunkDuration is the time window per ListActivities call.
// Splitting the lookback into small windows bounds how much data Google must scan per
// request: a 90-day open-ended query on a large tenant (e.g. DoorDash with 10k+ users)
// can have millions of events that exhaust the HTTP timeout before the first response
// header arrives. The baton SDK runner imposes a 60-second gRPC deadline on each
// ListEvents call; Google's server-side scan must complete within that budget.
// 1-day chunks keep per-request scan time well under that limit (~90 chunks for a
// full 90-day backfill, each covering at most ~1/90th of the historical data).
const eventFeedChunkDuration = 24 * time.Hour

// eventFeedCatchUpBuffer is the minimum age an EndAt must have before we consider the
// chunk "not yet at now". Google's Reports API has event ingestion lag (typically minutes
// to hours); events produced in the last few minutes may not yet appear in the API.
// Using a small buffer avoids an infinite catch-up loop where the final chunk always
// appears to have more data.
const eventFeedCatchUpBuffer = 5 * time.Minute

type usageEventFeed struct {
	c *gwclient.GoogleWorkspaceClient
}

func rfc3339ToTimestamp(s string) *timestamppb.Timestamp {
	i, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(i)
}

func unixSecondStringToTimestamp(s string) *timestamppb.Timestamp {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return timestamppb.New(time.Unix(i, 0))
}

func convertIdTimeToTimestamp(s string) *timestamppb.Timestamp {
	if time := rfc3339ToTimestamp(s); time != nil {
		return time
	}
	if time := unixSecondStringToTimestamp(s); time != nil {
		return time
	}
	return nil
}

func getValueFromParameters(name string, parameters []*reportsAdmin.ActivityEventsParameters) string {
	for _, p := range parameters {
		p := p
		if p.Name == name {
			return p.Value
		}
	}
	return ""
}
func hasParameter(name string, parameters []*reportsAdmin.ActivityEventsParameters) bool {
	for _, p := range parameters {
		p := p
		if p.Name == name {
			return true
		}
	}
	return false
}

type pageToken struct {
	LatestEventSeen string            `json:"latest_event_seen,omitempty"`
	NextPageToken   string            `json:"next_page_token,omitempty"`
	StartAt         string            `json:"start_at,omitempty"`
	// EndAt is the upper bound of the current time-window chunk sent to the API as endTime.
	// When empty the feed has no upper bound (backward-compat with pre-chunking cursors that
	// are still mid-pagination via NextPageToken or EventPageTokens).
	EndAt    string `json:"end_at,omitempty"`
	PageSize int    `json:"page_size,omitempty"`
	// EventPageTokens holds per-event-name pagination cursors for feeds that
	// issue one ListActivities request per event name (e.g. adminEventFeed).
	EventPageTokens map[string]string `json:"event_page_tokens,omitempty"`
}

func unmarshalPageToken(token *pagination.StreamToken, defaultStart *timestamppb.Timestamp) (*pageToken, error) {
	pt := &pageToken{}
	if token != nil && token.Cursor != "" {
		data, err := base64.StdEncoding.DecodeString(token.Cursor)
		if err != nil {
			return nil, fmt.Errorf("failed to decode page token: %w", err)
		}

		if err := json.Unmarshal(data, pt); err != nil {
			return nil, fmt.Errorf("failed to unmarshal page token JSON: %w", err)
		}

		pt.PageSize = token.Size
	}

	// Enforce the lookback cap regardless of what the caller passes as defaultStart.
	// This prevents stale cursors (e.g. from an expired mid-pagination pageToken) from
	// requesting years of data and timing out on every retry.
	cutoff := time.Now().Add(-maxEventFeedLookback)
	if defaultStart == nil || defaultStart.AsTime().Before(cutoff) {
		// There's lag on these events, so we're going to start roughly when google says events should come in
		// https://support.google.com/a/answer/7061566?fl=1&sjid=13551023455982018638-NC (Data Retention and Lag Times)
		defaultStart = timestamppb.New(cutoff)
	}

	if pt.StartAt == "" {
		pt.StartAt = defaultStart.AsTime().Format(time.RFC3339)
	} else {
		cursorStart, err := time.Parse(time.RFC3339, pt.StartAt)
		if err != nil || cursorStart.Before(cutoff) {
			pt.StartAt = cutoff.UTC().Format(time.RFC3339)
			pt.NextPageToken = ""
			pt.LatestEventSeen = ""
		}
	}

	if pt.LatestEventSeen == "" {
		pt.LatestEventSeen = pt.StartAt
	}

	// Initialize the chunk end time (EndAt) when starting a new chunk.
	// Guard: only set when not already mid-pagination (no active page tokens) so we
	// never clobber an in-progress chunk's EndAt on backward-compat cursors.
	if pt.EndAt == "" && pt.NextPageToken == "" && len(pt.EventPageTokens) == 0 {
		chunkEnd := time.Now().UTC()
		if startAt, parseErr := time.Parse(time.RFC3339, pt.StartAt); parseErr == nil {
			candidate := startAt.UTC().Add(eventFeedChunkDuration)
			if candidate.Before(chunkEnd) {
				chunkEnd = candidate
			}
		}
		pt.EndAt = chunkEnd.Format(time.RFC3339)
	}

	return pt, nil
}

func (pt *pageToken) marshal() (string, error) {
	data, err := json.Marshal(pt)
	if err != nil {
		return "", fmt.Errorf("failed to marshal page token: %w", err)
	}

	basedToken := base64.StdEncoding.EncodeToString(data)

	return basedToken, nil
}

func (f *usageEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	cursor, err := unmarshalPageToken(pToken, startAt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to unmarshal page token in usage event feed: %w", err)
	}

	// Pass endTime (cursor.EndAt) to bound the Google-side scan to the current chunk window.
	// For backward-compat cursors still mid-pagination (NextPageToken set, no EndAt), EndAt
	// is empty and the call falls through to the open-ended legacy behaviour.
	r, err := f.c.ListActivities(ctx, "all", "token", "authorize", cursor.StartAt, cursor.EndAt, cursor.NextPageToken, usageActivitiesPageSize)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace: failed to list token activities: %w", err)
	}

	latestEvent, err := time.Parse(time.RFC3339, cursor.LatestEventSeen)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse latest event time in usage event feed: %w", err)
	}
	events := []*v2.Event{}
	for _, activity := range r.Items {
		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			// Set occurred at to epoch so that it should never be after the latest event
			// Unless latest event is before epoch for some reason
			occurredAt = timestamppb.New(time.Unix(0, 0))
		}
		if occurredAt.AsTime().After(latestEvent) {
			cursor.LatestEventSeen = occurredAt.AsTime().Format(time.RFC3339)
			latestEvent = occurredAt.AsTime()
		}
		// There can be multiple events, have not found an example of this yet
		for _, e := range activity.Events {
			userTrait, err := resource.NewUserTrait(
				resource.WithEmail(activity.Actor.Email, true),
				resource.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
			)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to create user trait: %w", err)
			}
			event, err := newV2Event(activity, occurredAt, e, userTrait)
			if err != nil {
				l.Error("google-workspace-event-feed: failed to create event", zap.Error(err))
				// Let's not bail the whole feed because of one bad event
				continue
			}
			if event == nil {
				continue
			}

			events = append(events, event)
		}
	}

	cursor.NextPageToken = r.NextPageToken

	hasMore := r.NextPageToken != ""
	if !hasMore {
		// Current chunk is exhausted — decide whether to advance to the next chunk or
		// signal that we're caught up to the present.
		if cursor.EndAt != "" {
			chunkEnd, parseErr := time.Parse(time.RFC3339, cursor.EndAt)
			if parseErr != nil {
				chunkEnd = time.Now().UTC()
			}
			now := time.Now().UTC()
			if chunkEnd.Before(now.Add(-eventFeedCatchUpBuffer)) {
				// More time remains between chunkEnd and now: advance the window forward.
				cursor.StartAt = cursor.EndAt
				nextEnd := chunkEnd.UTC().Add(eventFeedChunkDuration)
				if nextEnd.After(now) {
					nextEnd = now
				}
				cursor.EndAt = nextEnd.UTC().Format(time.RFC3339)
				cursor.LatestEventSeen = ""
				hasMore = true
			} else {
				// Caught up: store the chunk boundary as StartAt so the next scheduled
				// run picks up from here rather than rescanning this window.
				cursor.StartAt = cursor.EndAt
				cursor.EndAt = ""
				cursor.LatestEventSeen = ""
			}
		} else {
			// Legacy path (no EndAt): preserve existing behavior.
			cursor.StartAt = cursor.LatestEventSeen
			cursor.LatestEventSeen = ""
		}
	}

	l.Debug("google-workspace-event-feed: listed usage events",
		zap.Int("events_produced", len(events)),
		zap.String("chunk_start", cursor.StartAt),
		zap.String("chunk_end", cursor.EndAt),
		zap.Bool("has_more", hasMore),
	)

	cursorToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to marshal cursor token in usage event feed: %w", err)
	}
	return events, &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: hasMore,
	}, nil, nil
}

func newV2Event(activity *reportsAdmin.Activity, occurredAt *timestamppb.Timestamp, e *reportsAdmin.ActivityEvents, userTrait *v2.UserTrait) (*v2.Event, error) {
	if !hasParameter("client_id", e.Parameters) {
		return nil, fmt.Errorf("no client_id in event parameters")
	}
	if !hasParameter("app_name", e.Parameters) {
		return nil, fmt.Errorf("no app_name in event parameters")
	}

	clientID := getValueFromParameters("client_id", e.Parameters)
	appName := getValueFromParameters("app_name", e.Parameters)

	if clientID == appName && privateAppIDRegex.MatchString(clientID) {
		// This is a private app, we don't want to report on these
		return nil, nil
	}

	event := &v2.Event{
		Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_UsageEvent{
			UsageEvent: &v2.UsageEvent{
				TargetResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeEnterpriseApplication.Id,
						Resource:     clientID,
					},
					DisplayName: appName,
				},
				ActorResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeUser.Id,
						Resource:     activity.Actor.ProfileId,
					},
					DisplayName: activity.Actor.Email,
					Annotations: annotations.New(userTrait),
				},
			},
		},
	}
	return event, nil
}

func (f *usageEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "usage_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
	}
}

func newUsageEventFeed(client *gwclient.GoogleWorkspaceClient) *usageEventFeed {
	return &usageEventFeed{
		c: client,
	}
}
