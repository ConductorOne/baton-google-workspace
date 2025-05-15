package connector

import (
	"context"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type adminEventFeed struct {
	connector *GoogleWorkspace
}

func (e *adminEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	var streamState *pagination.StreamState
	s, err := e.connector.getReportService(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	req := s.Activities.List("all", "admin")
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
	events := make([]*v2.Event, 0)
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
			switch e.Name {
			/*
				case "DOMAIN_SETTINGS":
					switch e.Type {
					case "ADD_APPLICATION":
						// APP_ID
						// APPLICATION_ENABLED
						// APPLICATION_NAME
					}
			*/
			case "GROUP_SETTINGS":
				switch e.Type {
				case "CREATE_GROUP", "DELETE_GROUP", "CHANGE_GROUP_DESCRIPTION", "CHANGE_GROUP_NAME":
					// GROUP_EMAIL
					evt := &v2.Event{
						Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
						OccurredAt: occurredAt,
						Event: &v2.Event_ResourceChangeEvent{
							ResourceChangeEvent: &v2.ResourceChangeEvent{
								ResourceId: &v2.ResourceId{
									ResourceType: resourceTypeGroup.Id,
									Resource:     "", // TODO: Lookup resource
								},
							},
						},
					}
					events = append(events, evt)
					l.Debug("google-workspace-event-feed: group settings create", zap.String("event", e.Name), zap.String("type", e.Type))
				case "CHANGE_GROUP_EMAIL":
					// GROUP_EMAIL
					// NEW_VALUE
					l.Debug("google-workspace-event-feed: group settings update", zap.String("event", e.Name), zap.String("type", e.Type))
				case "ADD_GROUP_MEMBER", "REMOVE_GROUP_MEMBER", "UPDATE_GROUP_MEMBER":
					// GROUP_EMAIL
					// USER_EMAIL
				}
			case "USER_SETTINGS":
				switch e.Type {
				case "ACCEPT_USER_INVITATION", "CHANGE_USER_ORGANIZATION", "DELETE_ACCOUNT_INFO_DUMP", "ADD_DISPLAY_NAME", "CHANGE_DISPLAY_NAME",
					"REMOVE_DISPLAY_NAME", "CHANGE_FIRST_NAME", "CHANGE_LAST_NAME", "ARCHIVE_USER", "CREATE_USER", "DELETE_USER", "RENAME_USER", "SUSPEND_USER",
					"UNARCHIVE_USER", "UNDELETE_USER", "UNSUSPEND_USER":
					// USER_EMAIL
				case "CANCEL_USER_INVITE":
					// DOMAIN_NAME
					// USER_EMAIL
				}
			default:
				l.Debug("google-workspace-event-feed: unsupported event", zap.String("event", e.Name), zap.String("type", e.Type))
				continue
			}

			userTrait, err := resource.NewUserTrait(
				resource.WithEmail(activity.Actor.Email, true),
				resource.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
			)
			if err != nil {
				return nil, nil, nil, err
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
		return nil, nil, nil, err
	}
	streamState = &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: r.NextPageToken != "",
	}
	return events, streamState, nil, nil
}

func (e *adminEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "admin_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_RESOURCE_CHANGE,
		},
		StartAt: v2.EventFeedStartAt_EVENT_FEED_START_AT_HEAD,
	}
}
