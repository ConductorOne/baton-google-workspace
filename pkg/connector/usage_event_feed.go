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
)

var privateAppIDRegex = regexp.MustCompile("[0-9]{21}")

type usageEventFeed struct {
	c *GoogleWorkspace
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
	LatestEventSeen string `json:"latest_event_seen,omitempty"`
	NextPageToken   string `json:"next_page_token,omitempty"`
	StartAt         string `json:"start_at,omitempty"`
	PageSize        int    `json:"page_size,omitempty"`
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

	if pt.StartAt == "" {
		if defaultStart == nil {
			// There's lag on these events, so we're going to start roughly when google says events should come in
			// https://support.google.com/a/answer/7061566?fl=1&sjid=13551023455982018638-NC (Data Retention and Lag Times)
			defaultStart = timestamppb.New(time.Now().Add(-2 * time.Hour))
		}
		pt.StartAt = defaultStart.AsTime().Format(time.RFC3339)
	}

	if pt.LatestEventSeen == "" {
		pt.LatestEventSeen = pt.StartAt
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

	var streamState *pagination.StreamState
	s, err := f.c.getReportService(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get report service in usage event feed: %w", err)
	}

	req := s.Activities.List("all", "token")
	req.MaxResults(int64(pToken.Size))
	req.EventName("authorize")

	cursor, err := unmarshalPageToken(pToken, startAt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to unmarshal page token in usage event feed: %w", err)
	}

	if cursor.StartAt != "" {
		req.StartTime(cursor.StartAt)
	}
	if cursor.NextPageToken != "" {
		req.PageToken(cursor.NextPageToken)
	}

	r, err := req.Do()
	if err != nil {
		return nil, nil, nil, wrapGoogleApiErrorWithContext(err, "failed to list usage activities")
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
	if r.NextPageToken == "" {
		cursor.StartAt = cursor.LatestEventSeen
		cursor.LatestEventSeen = ""
	}

	cursorToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to marshal cursor token in usage event feed: %w", err)
	}
	streamState = &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: r.NextPageToken != "",
	}
	return events, streamState, nil, nil
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

func newUsageEventFeed(connector *GoogleWorkspace) *usageEventFeed {
	return &usageEventFeed{
		c: connector,
	}
}
