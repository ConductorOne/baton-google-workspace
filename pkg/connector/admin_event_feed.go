package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
	directory "google.golang.org/api/admin/directory/v1"
	reports "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type cacheEntry struct {
	Id          string
	DisplayName string
}

type cacheMap map[string]cacheEntry

type adminEventFeed struct {
	connector *GoogleWorkspace

	groupCache cacheMap
	userCache  cacheMap

	groupMtx sync.Mutex
	userMtx  sync.Mutex
}

func (f *adminEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	var streamState *pagination.StreamState
	s, err := f.connector.getReportService(ctx)
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

	l.Debug("google-workspace-event-feed: listed events",
		zap.Int("count", len(r.Items)),
		zap.String("next_page_token", r.NextPageToken),
		zap.Any("start_at", startAt),
		zap.Any("latest_event", cursor.LatestEventSeen),
	)

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
	l := ctxzap.Extract(ctx)

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
	case "ADD_GROUP_MEMBER", "UPDATE_GROUP_MEMBER":
		evt, err := f.newGroupMemberGrantEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", "USER_EMAIL", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)

	// We're unable to look up the id for a deleted group, so we skip it
	case "DELETE_GROUP":
	default:
		l.Debug("google-workspace-event-feed: skipping group event", zap.String("event", activityEvt.Type))
	}

	return events, nil
}

func (f *adminEventFeed) handleUserEvent(ctx context.Context, uniqueQualifier int64, occurredAt *timestamppb.Timestamp, activityEvt *reports.ActivityEvents) ([]*v2.Event, error) {
	l := ctxzap.Extract(ctx)

	events := make([]*v2.Event, 0)
	switch activityEvt.Name {
	case "ACCEPT_USER_INVITATION", "CHANGE_USER_ORGANIZATION", "ADD_DISPLAY_NAME", "CHANGE_DISPLAY_NAME", "CHANGE_FIRST_NAME", "CHANGE_LAST_NAME", "CREATE_USER", "RENAME_USER":
		evt, err := f.newUserChangedEvent(ctx, uniqueQualifier, occurredAt, "USER_EMAIL", activityEvt)
		if err != nil {
			return nil, err
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	default:
		l.Debug("google-workspace-event-feed: skipping user event", zap.String("event", activityEvt.Type))
	}
	return events, nil
}

func (f *adminEventFeed) newGroupChangedEvent(
	ctx context.Context,
	uniqueQualifier int64,
	occurredAt *timestamppb.Timestamp,
	parameterName string,
	activityEvent *reports.ActivityEvents,
) (*v2.Event, error) {
	groupEmail := getValueFromParameters(parameterName, activityEvent.Parameters)

	if groupEmail == "" {
		return nil, nil
	}

	group, err := f.lookupGroup(ctx, groupEmail)
	if err != nil {
		return nil, err
	}
	if group == nil || group.Id == "" {
		return nil, nil
	}

	return &v2.Event{
		Id:         strconv.FormatInt(uniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_ResourceChangeEvent{
			ResourceChangeEvent: &v2.ResourceChangeEvent{
				ResourceId: &v2.ResourceId{
					ResourceType: resourceTypeGroup.Id,
					Resource:     group.Id,
				},
			},
		},
	}, nil
}

func (f *adminEventFeed) newGroupMemberGrantEvent(
	ctx context.Context,
	uniqueQualifier int64,
	occurredAt *timestamppb.Timestamp,
	groupEmailName string,
	userEmailName string,
	activityEvent *reports.ActivityEvents,
) (*v2.Event, error) {
	groupEmail := getValueFromParameters(groupEmailName, activityEvent.Parameters)

	if groupEmail == "" {
		return nil, nil
	}

	group, err := f.lookupGroup(ctx, groupEmail)
	if err != nil {
		return nil, err
	}

	if group == nil || group.Id == "" {
		return nil, nil
	}

	userEmail := getValueFromParameters(userEmailName, activityEvent.Parameters)
	if userEmail == "" {
		return nil, nil
	}

	user, err := f.lookupUser(ctx, userEmail)
	if err != nil {
		return nil, err
	}

	if user == nil || user.Id == "" {
		return nil, nil
	}

	// group email is also not the display name
	groupResource, err := sdkResource.NewGroupResource(group.DisplayName, resourceTypeGroup, group.Id, nil, sdkResource.WithAnnotation(&v2.V1Identifier{Id: group.Id}))
	if err != nil {
		return nil, err
	}
	entitlement := sdkEntitlement.NewAssignmentEntitlement(groupResource, groupMemberEntitlement, sdkEntitlement.WithGrantableTo(resourceTypeUser))

	userResource, err := sdkResource.NewUserResource(
		user.DisplayName, // mJP do we really need this ? it was user.Name.FullName
		resourceTypeUser,
		user.Id,
		nil,
		sdkResource.WithAnnotation(
			&v2.V1Identifier{
				Id: user.Id,
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return &v2.Event{
		Id:         strconv.FormatInt(uniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_CreateGrantEvent{
			CreateGrantEvent: &v2.CreateGrantEvent{
				Entitlement: entitlement,
				Principal:   userResource,
			},
		},
	}, nil
}

func (f *adminEventFeed) newUserChangedEvent(
	ctx context.Context,
	uniqueQualifier int64,
	occurredAt *timestamppb.Timestamp,
	parameterName string,
	activityEvent *reports.ActivityEvents,
) (*v2.Event, error) {
	userEmail := getValueFromParameters(parameterName, activityEvent.Parameters)

	if userEmail == "" {
		return nil, nil
	}

	user, err := f.lookupUser(ctx, userEmail)
	if err != nil {
		return nil, err
	}

	if user == nil || user.Id == "" {
		return nil, nil
	}

	return &v2.Event{
		Id:         strconv.FormatInt(uniqueQualifier, 10),
		OccurredAt: occurredAt,
		Event: &v2.Event_ResourceChangeEvent{
			ResourceChangeEvent: &v2.ResourceChangeEvent{
				ResourceId: &v2.ResourceId{
					ResourceType: resourceTypeUser.Id,
					Resource:     user.Id,
				},
			},
		},
	}, nil
}

func (f *adminEventFeed) lookupUser(ctx context.Context, email string) (*cacheEntry, error) {
	f.userMtx.Lock()
	defer f.userMtx.Unlock()

	if entry, ok := f.userCache[email]; ok {
		return &entry, nil
	}

	l := ctxzap.Extract(ctx)

	userService, err := f.connector.getDirectoryService(ctx, directory.AdminDirectoryUserReadonlyScope)
	if err != nil {
		return nil, err
	}

	user, err := userService.Users.Get(email).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				l.Info("google-workspace: user no longer exists", zap.String("email", email))
				delete(f.userCache, email)
				return nil, nil
			}
		}
		return nil, fmt.Errorf("google-workspace: failed to get user %s: %w", email, err)
	}

	entry := cacheEntry{
		Id:          user.Id,
		DisplayName: user.Name.DisplayName,
	}

	f.userCache[email] = entry

	if user.Id == "" {
		l.Warn("google-workspace: user has no id", zap.String("email", user.PrimaryEmail))
		return nil, nil
	}

	return &entry, nil
}

func (f *adminEventFeed) lookupGroup(ctx context.Context, email string) (*cacheEntry, error) {
	f.groupMtx.Lock()
	defer f.groupMtx.Unlock()

	if entry, ok := f.groupCache[email]; ok {
		return &entry, nil
	}

	l := ctxzap.Extract(ctx)

	groupService, err := f.connector.getDirectoryService(ctx, directory.AdminDirectoryGroupReadonlyScope)
	if err != nil {
		return nil, err
	}

	group, err := groupService.Groups.Get(email).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				l.Info("google-workspace: group no longer exists", zap.String("email", email))
				delete(f.groupCache, email)
				return nil, nil
			}
		}
		return nil, fmt.Errorf("google-workspace: failed to get group %s: %w", email, err)
	}

	entry := cacheEntry{
		Id:          group.Id,
		DisplayName: group.Name,
	}

	f.groupCache[email] = entry

	if group.Id == "" {
		l.Warn("google-workspace: group has no id", zap.String("email", group.Email))
		return nil, nil
	}

	return &entry, nil
}

func (f *adminEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "admin_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_RESOURCE_CHANGE,
			v2.EventType_EVENT_TYPE_CREATE_GRANT,
		},
	}
}

func newAdminEventFeed(connector *GoogleWorkspace) *adminEventFeed {
	return &adminEventFeed{
		connector:  connector,
		groupCache: make(cacheMap),
		userCache:  make(cacheMap),
	}
}
