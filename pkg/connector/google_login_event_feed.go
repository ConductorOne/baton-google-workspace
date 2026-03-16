package connector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// googleLoginEventFeed emits UsageEvents from Google Workspace sign-in activity.
// Unlike SAML/OAuth feeds, the target resource is always Google Workspace itself.
type googleLoginEventFeed struct {
	connector *GoogleWorkspace
}

func newGoogleLoginEventFeed(connector *GoogleWorkspace) *googleLoginEventFeed {
	return &googleLoginEventFeed{connector: connector}
}

func (f *googleLoginEventFeed) EventFeedMetadata(_ context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "google_login_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
	}
}

func (f *googleLoginEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	s, err := f.connector.getReportService(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to get report service in google login event feed: %w", err)
	}

	req := s.Activities.List(reportsUserAll, reportsAppLogin)
	req.MaxResults(int64(pToken.Size))
	req.EventName("login_success")

	cursor, err := unmarshalPageToken(pToken, startAt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to unmarshal page token in google login event feed: %w", err)
	}

	if cursor.StartAt != "" {
		req.StartTime(cursor.StartAt)
	}
	if cursor.NextPageToken != "" {
		req.PageToken(cursor.NextPageToken)
	}

	r, err := req.Context(ctx).Do()
	if err != nil {
		return nil, nil, nil, wrapGoogleApiErrorWithContext(err, "failed to list google login activities")
	}

	latestEvent, err := time.Parse(time.RFC3339, cursor.LatestEventSeen)
	if err != nil {
		latestEvent = time.Unix(0, 0)
	}

	events := []*v2.Event{}
	for _, activity := range r.Items {
		if activity.Actor.ProfileId == "" {
			continue
		}

		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			occurredAt = timestamppb.New(time.Unix(0, 0))
		}
		if occurredAt.AsTime().After(latestEvent) {
			cursor.LatestEventSeen = occurredAt.AsTime().Format(time.RFC3339)
			latestEvent = occurredAt.AsTime()
		}

		userTrait, err := resource.NewUserTrait(
			resource.WithEmail(activity.Actor.Email, true),
			resource.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
		)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to create user trait in google login event feed: %w", err)
		}

		events = append(events, &v2.Event{
			Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
			OccurredAt: occurredAt,
			Event: &v2.Event_UsageEvent{
				UsageEvent: &v2.UsageEvent{
					TargetResource: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: resourceTypeApplication.Id,
							Resource:     googleWorkspaceAppID,
						},
						DisplayName: "Google Workspace",
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
		})
	}

	cursor.NextPageToken = r.NextPageToken
	if r.NextPageToken == "" {
		cursor.StartAt = cursor.LatestEventSeen
		cursor.LatestEventSeen = ""
	}

	cursorToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to marshal cursor token in google login event feed: %w", err)
	}

	return events, &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: r.NextPageToken != "",
	}, nil, nil
}
