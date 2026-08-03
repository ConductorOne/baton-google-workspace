package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// maxEventFeedLookback caps how far back the admin event feed queries the Google Reports API.
// Google page tokens expire after ~24h, so a cursor left mid-pagination (e.g. after a connector
// restart or a transient timeout) would otherwise keep requesting the full historical window on
// every retry, causing HTTP timeout death spirals on large orgs. 90 days balances sufficient
// event history against query size; Google retains Reports data for 6 months so there is
// headroom if the window needs to grow.
//
// Note: this cursor/lookback scheme is specific to the admin event feed (resource-change/grant
// events). The usage-tracking feeds (usage_event_feed.go, google_login_event_feed.go,
// saml_event_feed.go) no longer replay bulk activity history at all — see event_feed_common.go.
const maxEventFeedLookback = 90 * 24 * time.Hour

type adminEventFeedPageToken struct {
	LatestEventSeen string `json:"latest_event_seen,omitempty"`
	NextPageToken   string `json:"next_page_token,omitempty"`
	StartAt         string `json:"start_at,omitempty"`
	PageSize        int    `json:"page_size,omitempty"`
}

func unmarshalAdminEventFeedPageToken(ctx context.Context, token *pagination.StreamToken, defaultStart *timestamppb.Timestamp) (*adminEventFeedPageToken, error) {
	l := ctxzap.Extract(ctx)

	pt := &adminEventFeedPageToken{}
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

	cutoff := time.Now().Add(-maxEventFeedLookback)

	switch {
	case pt.StartAt == "":
		// Fresh cursor: pick a starting point, clamped to the lookback cap.
		start := defaultStart
		if start == nil || start.AsTime().Before(cutoff) {
			// There's lag on these events, so we're going to start roughly when google says events should come in
			// https://support.google.com/a/answer/7061566?fl=1&sjid=13551023455982018638-NC (Data Retention and Lag Times)
			start = timestamppb.New(cutoff)
		}
		pt.StartAt = start.AsTime().Format(time.RFC3339)
	case pt.NextPageToken == "":
		// Not mid-pagination: safe to re-validate staleness (e.g. a cursor left over from a
		// completed page-walk long ago) against the lookback cap.
		cursorStart, err := time.Parse(time.RFC3339, pt.StartAt)
		switch {
		case err != nil:
			l.Debug("google-workspace: admin event feed cursor start_at was unparseable, resetting to lookback cutoff",
				zap.String("start_at", pt.StartAt), zap.Error(err))
			pt.StartAt = cutoff.Format(time.RFC3339)
			pt.LatestEventSeen = ""
		case cursorStart.Before(cutoff):
			l.Debug("google-workspace: admin event feed cursor start_at is stale, resetting to lookback cutoff",
				zap.String("start_at", pt.StartAt))
			pt.StartAt = cutoff.Format(time.RFC3339)
			pt.LatestEventSeen = ""
		}
	}

	if pt.LatestEventSeen == "" {
		pt.LatestEventSeen = pt.StartAt
	}

	return pt, nil
}

func (pt *adminEventFeedPageToken) marshal() (string, error) {
	data, err := json.Marshal(pt)
	if err != nil {
		return "", fmt.Errorf("failed to marshal page token: %w", err)
	}

	basedToken := base64.StdEncoding.EncodeToString(data)

	return basedToken, nil
}

type cacheEntry struct {
	Id          string
	DisplayName string
}

type cacheMap map[string]cacheEntry

type adminEventFeed struct {
	client *gwclient.GoogleWorkspaceClient

	groupCache cacheMap
	userCache  cacheMap

	groupMtx sync.Mutex
	userMtx  sync.Mutex
}

func (f *adminEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	var streamState *pagination.StreamState

	cursor, err := unmarshalAdminEventFeedPageToken(ctx, pToken, startAt)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to unmarshal page token: %w", err)
	}

	r, err := f.client.ListActivities(ctx, "all", "admin", "", cursor.StartAt, cursor.NextPageToken, "", int64(pToken.Size))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("google-workspace: failed to list admin activities: %w", err)
	}

	latestEvent, err := time.Parse(time.RFC3339, cursor.LatestEventSeen)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse latest event time in admin event feed: %w", err)
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
					l.Error("failed to handle group event", zap.Error(err))
					continue
				}
				events = append(events, changeEvents...)
			case "USER_SETTINGS":
				changeEvents, err := f.handleUserEvent(ctx, activity.Id.UniqueQualifier, occurredAt, evt)
				if err != nil {
					l.Error("failed to handle user event", zap.Error(err))
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
		return nil, nil, nil, fmt.Errorf("failed to marshal cursor token in admin event feed: %w", err)
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
