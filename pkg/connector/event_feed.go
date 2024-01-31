package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
			return nil, err
		}

		if err := json.Unmarshal(data, pt); err != nil {
			return nil, err
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
		return "", err
	}

	basedToken := base64.StdEncoding.EncodeToString(data)

	return basedToken, nil
}

func (c *GoogleWorkspace) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	var streamState *pagination.StreamState
	s, err := c.getReportService(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	req := s.Activities.List("all", "token")
	req.MaxResults(int64(pToken.Size))

	cursor, err := unmarshalPageToken(pToken, startAt)
	if err != nil {
		return nil, nil, nil, err
	}

	if cursor.StartAt != "" {
		req.StartTime(cursor.StartAt)
	}
	if cursor.NextPageToken != "" {
		req.PageToken(cursor.NextPageToken)
	}

	r, err := req.Do()
	if err != nil {
		return nil, nil, nil, err
	}

	latestEvent, err := time.Parse(time.RFC3339, cursor.LatestEventSeen)
	if err != nil {
		return nil, nil, nil, err
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
				return nil, nil, nil, err
			}
			event := &v2.Event{
				Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
				OccurredAt: occurredAt,
				Event: &v2.Event_UsageEvent{
					UsageEvent: &v2.UsageEvent{
						TargetResource: &v2.Resource{
							Id: &v2.ResourceId{
								ResourceType: resourceTypeEnterpriseApplication.Id,
								Resource:     getValueFromParameters("client_id", e.Parameters),
							},
							DisplayName: getValueFromParameters("app_name", e.Parameters),
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
				Annotations: nil,
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
		return nil, nil, nil, err
	}
	streamState = &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: r.NextPageToken != "",
	}
	return events, streamState, nil, nil
}
