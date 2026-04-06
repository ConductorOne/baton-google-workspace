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
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// samlEventFeed emits UsageEvents from Google Workspace SAML app login activity.
type samlEventFeed struct {
	client     *gwclient.GoogleWorkspaceClient
	customerID string
}

func newSamlEventFeed(client *gwclient.GoogleWorkspaceClient, customerID string) *samlEventFeed {
	return &samlEventFeed{client: client, customerID: customerID}
}

func (f *samlEventFeed) EventFeedMetadata(_ context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "saml_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
	}
}

// ListEvents tracks SAML app usage via Google's "saml" audit log.
//
// Unlike OAuth apps (see usage_event_feed.go), SAML "login_success" fires on every SSO authentication,
// so last login timestamps are accurate. SAML apps are identified by app name (no numeric client_id).
func (f *samlEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	samlProfileMap, err := loadSAMLProfileMap(ctx, f.client, f.customerID)
	if err != nil {
		return nil, nil, nil, err
	}

	cursor, err := unmarshalPageToken(pToken, startAt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to unmarshal page token in saml event feed: %w", err)
	}

	r, err := f.client.ListActivities(ctx, reportsUserAll, reportsAppSAML, "login_success", cursor.StartAt, cursor.NextPageToken, int64(pToken.Size))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to list saml login activities: %w", err)
	}

	latestEvent, err := time.Parse(time.RFC3339, cursor.LatestEventSeen)
	if err != nil {
		latestEvent = time.Unix(0, 0)
	}

	events := []*v2.Event{}
	for _, activity := range r.Items {
		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			occurredAt = timestamppb.New(time.Unix(0, 0))
		}
		if occurredAt.AsTime().After(latestEvent) {
			cursor.LatestEventSeen = occurredAt.AsTime().Format(time.RFC3339)
			latestEvent = occurredAt.AsTime()
		}

		for _, e := range activity.Events {
			userTrait, err := resource.NewUserTrait(
				resource.WithEmail(activity.Actor.Email, true),
				resource.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
			)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to create user trait in saml event feed: %w", err)
			}

			if event := newSamlV2Event(activity, occurredAt, e, userTrait, samlProfileMap); event != nil {
				events = append(events, event)
			}
		}
	}

	cursor.NextPageToken = r.NextPageToken
	if r.NextPageToken == "" {
		cursor.StartAt = cursor.LatestEventSeen
		cursor.LatestEventSeen = ""
	}

	cursorToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace-connector: failed to marshal cursor token in saml event feed: %w", err)
	}

	streamState := &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: r.NextPageToken != "",
	}

	return events, streamState, nil, nil
}

func newSamlV2Event(activity *reportsAdmin.Activity, occurredAt *timestamppb.Timestamp, e *reportsAdmin.ActivityEvents, userTrait *v2.UserTrait, samlProfileMap map[string]string) *v2.Event {
	appName := getValueFromParameters("application_name", e.Parameters)
	if appName == "" {
		return nil
	}

	actorID := activity.Actor.ProfileId
	if actorID == "" {
		return nil
	}

	appID := appName
	if profileName, ok := samlProfileMap[appName]; ok {
		appID = profileName
	}

	return &v2.Event{
		Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_UsageEvent{
			UsageEvent: &v2.UsageEvent{
				TargetResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeEnterpriseApplication.Id,
						Resource:     samlAppIDPrefix + appID,
					},
					DisplayName: appName,
				},
				ActorResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeUser.Id,
						Resource:     actorID,
					},
					DisplayName: activity.Actor.Email,
					Annotations: annotations.New(userTrait),
				},
			},
		},
	}
}
