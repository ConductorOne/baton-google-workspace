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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	reports "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// adminEventNames lists the admin-application event names the feed subscribes to.
// The Google Reports API accepts one eventName per request, so ListEvents issues
// one ListActivities call per name and merges the results.
var adminEventNames = []string{
	// GROUP_SETTINGS
	"CREATE_GROUP",
	"CHANGE_GROUP_DESCRIPTION",
	"CHANGE_GROUP_NAME",
	"CHANGE_GROUP_EMAIL",
	"ADD_GROUP_MEMBER",
	"UPDATE_GROUP_MEMBER",
	"DELETE_GROUP",
	// USER_SETTINGS
	"ACCEPT_USER_INVITATION",
	"CHANGE_USER_ORGANIZATION",
	"ADD_DISPLAY_NAME",
	"CHANGE_DISPLAY_NAME",
	"CHANGE_FIRST_NAME",
	"CHANGE_LAST_NAME",
	"CREATE_USER",
	"RENAME_USER",
}

// adminGroupEventNames is the subset of adminEventNames that belong to GROUP_SETTINGS handling.
var adminGroupEventNames = map[string]bool{
	"CREATE_GROUP":             true,
	"CHANGE_GROUP_DESCRIPTION": true,
	"CHANGE_GROUP_NAME":        true,
	"CHANGE_GROUP_EMAIL":       true,
	"ADD_GROUP_MEMBER":         true,
	"UPDATE_GROUP_MEMBER":      true,
	"DELETE_GROUP":             true,
}

type cacheEntry struct {
	Id          string
	DisplayName string
}

type cacheMap map[string]cacheEntry

// adminActivitiesPageSize is the number of activity items requested per ListActivities
// call. The Google Reports API maximum is 1000.
const adminActivitiesPageSize = 1000

type adminEventFeed struct {
	client *gwclient.GoogleWorkspaceClient

	groupCache cacheMap
	userCache  cacheMap

	groupMtx sync.Mutex
	userMtx  sync.Mutex
}

func (f *adminEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	cursor, err := unmarshalPageToken(pToken, startAt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to unmarshal page token: %w", err)
	}

	latestEvent, err := time.Parse(time.RFC3339, cursor.LatestEventSeen)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse latest event time in admin event feed: %w", err)
	}

	// On the first call EventPageTokens is nil — fetch every event name from StartAt.
	// On continuation calls fetch only names that still have a next page token.
	fetchNames := adminEventNames
	if len(cursor.EventPageTokens) > 0 {
		fetchNames = make([]string, 0, len(cursor.EventPageTokens))
		for name := range cursor.EventPageTokens {
			fetchNames = append(fetchNames, name)
		}
	}

	events := make([]*v2.Event, 0)
	nextTokens := make(map[string]string)

	for _, eventName := range fetchNames {
		isGroupEvent := adminGroupEventNames[eventName]
		r, err := f.client.ListActivities(ctx, "all", "admin", eventName, cursor.StartAt, cursor.EndAt, cursor.EventPageTokens[eventName], adminActivitiesPageSize)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("google-workspace: failed to list admin activities for %s: %w", eventName, err)
		}

		for _, activity := range r.Items {
			occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
			if occurredAt == nil {
				occurredAt = timestamppb.New(time.Unix(0, 0))
			}
			if occurredAt.AsTime().After(latestEvent) {
				cursor.LatestEventSeen = occurredAt.AsTime().Format(time.RFC3339)
				latestEvent = occurredAt.AsTime()
			}
			for _, evt := range activity.Events {
				if evt.Name != eventName {
					continue
				}
				var changeEvents []*v2.Event
				var evtErr error
				if isGroupEvent {
					changeEvents, evtErr = f.handleGroupEvent(ctx, activity.Id.UniqueQualifier, occurredAt, evt)
				} else {
					changeEvents, evtErr = f.handleUserEvent(ctx, activity.Id.UniqueQualifier, occurredAt, evt)
				}
				if evtErr != nil {
					l.Error("failed to handle admin event", zap.String("event_name", evt.Name), zap.Error(evtErr))
					continue
				}
				events = append(events, changeEvents...)
			}
		}

		if r.NextPageToken != "" {
			nextTokens[eventName] = r.NextPageToken
		}
	}

	l.Debug("google-workspace-event-feed: listed admin events",
		zap.Int("event_names_fetched", len(fetchNames)),
		zap.Int("events_produced", len(events)),
		zap.String("latest_event", cursor.LatestEventSeen),
	)

	hasMore := len(nextTokens) > 0
	cursor.EventPageTokens = nextTokens
	if !hasMore {
		// All event names exhausted for this chunk — advance to the next chunk or
		// signal caught-up, same logic as usageEventFeed.
		if cursor.EndAt != "" {
			chunkEnd, parseErr := time.Parse(time.RFC3339, cursor.EndAt)
			if parseErr != nil {
				chunkEnd = time.Now()
			}
			now := time.Now()
			if chunkEnd.Before(now.Add(-eventFeedCatchUpBuffer)) {
				cursor.StartAt = cursor.EndAt
				nextEnd := chunkEnd.Add(eventFeedChunkDuration)
				if nextEnd.After(now) {
					nextEnd = now
				}
				cursor.EndAt = nextEnd.Format(time.RFC3339)
				cursor.LatestEventSeen = ""
				hasMore = true
			} else {
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

	cursorToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to marshal cursor token in admin event feed: %w", err)
	}

	return events, &pagination.StreamState{
		Cursor:  cursorToken,
		HasMore: hasMore,
	}, nil, nil
}

func (f *adminEventFeed) handleGroupEvent(ctx context.Context, uniqueQualifier int64, occurredAt *timestamppb.Timestamp, activityEvt *reports.ActivityEvents) ([]*v2.Event, error) {
	l := ctxzap.Extract(ctx)

	events := make([]*v2.Event, 0)
	switch activityEvt.Name {
	case "CREATE_GROUP", "CHANGE_GROUP_DESCRIPTION", "CHANGE_GROUP_NAME":
		evt, err := f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", activityEvt)
		if err != nil {
			return nil, fmt.Errorf("failed to create group changed event: %w", err)
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	case "CHANGE_GROUP_EMAIL":
		evt, err := f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", activityEvt)
		if err != nil {
			return nil, fmt.Errorf("failed to create group changed event (group email): %w", err)
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)

		evt, err = f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "NEW_VALUE", activityEvt)
		if err != nil {
			return nil, fmt.Errorf("failed to create group changed event (new value): %w", err)
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	case "ADD_GROUP_MEMBER":
		evt, err := f.newGroupMemberGrantEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", "USER_EMAIL", activityEvt)
		if err != nil {
			return nil, fmt.Errorf("failed to create group member grant event: %w", err)
		}
		if evt == nil {
			return nil, nil
		}
		events = append(events, evt)
	case "UPDATE_GROUP_MEMBER":
		evt, err := f.newGroupChangedEvent(ctx, uniqueQualifier, occurredAt, "GROUP_EMAIL", activityEvt)
		if err != nil {
			return nil, fmt.Errorf("failed to create group changed event (update member): %w", err)
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
			return nil, fmt.Errorf("failed to create user changed event: %w", err)
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
		return nil, fmt.Errorf("failed to lookup group %s: %w", groupEmail, err)
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
		return nil, fmt.Errorf("failed to lookup user %s in newGroupMemberGrantEvent: %w", userEmail, err)
	}

	if user == nil || user.Id == "" {
		return nil, nil
	}

	// group email is also not the display name
	resourceOpts := []sdkResource.ResourceOption{
		sdkResource.WithAnnotation(&v2.V1Identifier{
			Id: group.Id,
		}),
		sdkResource.WithAnnotation(&v2.RawId{
			Id: group.Id,
		}),
	}
	groupResource, err := sdkResource.NewGroupResource(group.DisplayName, resourceTypeGroup, group.Id, nil, resourceOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create group resource for grant event: %w", err)
	}
	entitlement := sdkEntitlement.NewAssignmentEntitlement(groupResource, groupMemberEntitlement, sdkEntitlement.WithGrantableTo(resourceTypeUser))

	userResource, err := sdkResource.NewUserResource(
		user.DisplayName,
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
		return nil, fmt.Errorf("failed to create user resource for grant event: %w", err)
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
		return nil, fmt.Errorf("failed to lookup user %s in newUserChangedEvent: %w", userEmail, err)
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

	user, err := f.client.GetUser(ctx, email)
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			l.Debug("user no longer exists")
			delete(f.userCache, email)
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace: failed to get user in admin event feed: %w", err)
	}

	entry := cacheEntry{
		Id:          user.Id,
		DisplayName: user.Name.DisplayName,
	}

	f.userCache[email] = entry

	if user.Id == "" {
		l.Debug("user has no id")
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

	group, err := f.client.GetGroup(ctx, email)
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			l.Debug("group no longer exists")
			delete(f.groupCache, email)
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace: failed to get group in admin event feed: %w", err)
	}

	entry := cacheEntry{
		Id:          group.Id,
		DisplayName: group.Name,
	}

	f.groupCache[email] = entry

	if group.Id == "" {
		l.Warn("group has no id", zap.String("email", group.Email))
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

func newAdminEventFeed(client *gwclient.GoogleWorkspaceClient) *adminEventFeed {
	return &adminEventFeed{
		client:     client,
		groupCache: make(cacheMap),
		userCache:  make(cacheMap),
	}
}
