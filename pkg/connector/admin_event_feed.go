package connector

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	directory "google.golang.org/api/admin/directory/v1"
	reports "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type adminEventFeed struct {
	connector *GoogleWorkspace

	groupIdCache map[string]string
	userIdCache  map[string]string

	groupIdMtx sync.Mutex
	userIdMtx  sync.Mutex
}

func (f *adminEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	var streamState *pagination.StreamState
	s, err := f.connector.getReportService(ctx)
	if err != nil {
		fmt.Println("DEBUG: getReportService error:", err)
		return nil, nil, nil, err
	}
	fmt.Println("DEBUG: getReportService successful")

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

	l.Debug("google-workspace-event-feed: found events", zap.Int("count", len(r.Items)), zap.String("next_page_token", r.NextPageToken), zap.Any("start_at", startAt))

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
		for _, evt := range activity.Events {
			switch evt.Type {
			case "GROUP_SETTINGS":
				changeEvents, err := f.handleGroupEvent(ctx, activity.Id.UniqueQualifier, occurredAt, evt)
				if err != nil {
					l.Error("google-workspace: failed to handle group event", zap.Error(err))
					continue
				}
				events = append(events, changeEvents...)
			case "USER_SETTINGS":
				changeEvents, err := f.handleUserEvent(ctx, activity.Id.UniqueQualifier, occurredAt, evt)
				if err != nil {
					l.Error("google-workspace: failed to handle user event", zap.Error(err))
					continue
				}
				events = append(events, changeEvents...)
			default:
				l.Debug("google-workspace-event-feed: skipping event", zap.String("event", evt.Name), zap.String("type", evt.Type))
				continue
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
		return nil, nil, nil, err
	}
	streamState = &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: r.NextPageToken != "",
	}
	return events, streamState, nil, nil
}

func (f *adminEventFeed) handleGroupEvent(ctx context.Context, uniqueQualifier int64, occurredAt *timestamppb.Timestamp, activityEvt *reports.ActivityEvents) ([]*v2.Event, error) {
	events := make([]*v2.Event, 0)
	switch activityEvt.Name {
	case "CREATE_GROUP", "CHANGE_GROUP_DESCRIPTION", "CHANGE_GROUP_NAME":
		evt, err := f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	case "CHANGE_GROUP_EMAIL":
		evt, err := f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)

		evt, err = f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "NEW_VALUE", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	case "ADD_GROUP_MEMBER", "REMOVE_GROUP_MEMBER", "UPDATE_GROUP_MEMBER":
		evt, err := f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)

		// Generate separate event for USER_EMAIL?
	}

	return events, nil
}

func (f *adminEventFeed) handleUserEvent(ctx context.Context, uniqueQualifier int64, occurredAt *timestamppb.Timestamp, activityEvt *reports.ActivityEvents) ([]*v2.Event, error) {
	events := make([]*v2.Event, 0)
	switch activityEvt.Name {
	case "ACCEPT_USER_INVITATION", "CHANGE_USER_ORGANIZATION", "DELETE_ACCOUNT_INFO_DUMP", "ADD_DISPLAY_NAME", "CHANGE_DISPLAY_NAME",
		"REMOVE_DISPLAY_NAME", "CHANGE_FIRST_NAME", "CHANGE_LAST_NAME", "ARCHIVE_USER", "CREATE_USER", "DELETE_USER", "RENAME_USER", "SUSPEND_USER",
		"UNARCHIVE_USER", "UNDELETE_USER", "UNSUSPEND_USER", "CANCEL_USER_INVITE":
		evt, err := f.newUserChangedEvent(ctx, uniqueQualifier, occurredAt, "USER_EMAIL", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	}
	return events, nil
}

func (f *adminEventFeed) newGroupChangedEvent(ctx context.Context, uniqueQualifier int64, occurredAt *timestamppb.Timestamp, parameterName string, activityEvent *reports.ActivityEvents) (*v2.Event, error) {
	groupEmail := getValueFromParameters(parameterName, activityEvent.Parameters)

	if groupEmail == "" {
		return nil, nil
	}

	groupId, err := f.lookupGroupId(ctx, groupEmail)
	if err != nil {
		return nil, err
	}

	if groupId == "" {
		return nil, nil
	}

	return &v2.Event{
		Id:         strconv.FormatInt(uniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_ResourceChangeEvent{
			ResourceChangeEvent: &v2.ResourceChangeEvent{
				ResourceId: &v2.ResourceId{
					ResourceType: resourceTypeGroup.Id,
					Resource:     groupId,
				},
			},
		},
	}, nil
}

func (f *adminEventFeed) newUserChangedEvent(ctx context.Context, uniqueQualifier int64, occurredAt *timestamppb.Timestamp, parameterName string, activityEvent *reports.ActivityEvents) (*v2.Event, error) {
	userEmail := getValueFromParameters(parameterName, activityEvent.Parameters)

	if userEmail == "" {
		return nil, nil
	}

	userId, err := f.lookupUserId(ctx, userEmail)
	if err != nil {
		return nil, err
	}

	if userId == "" {
		return nil, nil
	}

	return &v2.Event{
		Id:         strconv.FormatInt(uniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_ResourceChangeEvent{
			ResourceChangeEvent: &v2.ResourceChangeEvent{
				ResourceId: &v2.ResourceId{
					ResourceType: resourceTypeUser.Id,
					Resource:     userId,
				},
			},
		},
	}, nil
}

func (f *adminEventFeed) lookupUserId(ctx context.Context, email string) (string, error) {
	f.userIdMtx.Lock()
	defer f.userIdMtx.Unlock()

	if id, ok := f.userIdCache[email]; ok {
		return id, nil
	}

	userService, err := f.connector.getDirectoryService(ctx, directory.AdminDirectoryUserReadonlyScope)
	if err != nil {
		return "", err
	}

	user, err := userService.Users.Get(email).Do()
	if err != nil {
		return "", err
	}

	f.userIdCache[email] = user.Id

	if user.Id == "" {
		l := ctxzap.Extract(ctx)
		l.Warn("google-workspace: user has no id", zap.String("email", user.PrimaryEmail))
		return "", nil
	}

	return user.Id, nil
}

func (f *adminEventFeed) lookupGroupId(ctx context.Context, email string) (string, error) {
	f.groupIdMtx.Lock()
	defer f.groupIdMtx.Unlock()

	if id, ok := f.groupIdCache[email]; ok {
		return id, nil
	}

	groupService, err := f.connector.getDirectoryService(ctx, directory.AdminDirectoryGroupReadonlyScope)
	if err != nil {
		return "", err
	}

	group, err := groupService.Groups.Get(email).Do()
	if err != nil {
		return "", err
	}

	f.groupIdCache[email] = group.Id

	if group.Id == "" {
		l := ctxzap.Extract(ctx)
		l.Warn("google-workspace: group has no id", zap.String("email", group.Email))
		return "", nil
	}

	return group.Id, nil
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

func newAdminEventFeed(connector *GoogleWorkspace) *adminEventFeed {
	return &adminEventFeed{
		connector:    connector,
		groupIdCache: make(map[string]string),
		userIdCache:  make(map[string]string),
	}
}
