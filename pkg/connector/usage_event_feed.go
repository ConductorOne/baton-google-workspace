package connector

import (
	"context"
	"fmt"
	"regexp"
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

var privateAppIDRegex = regexp.MustCompile("[0-9]{21}")

// oauthAppLookupMaxResults bounds the per-user Reports API lookup for OAuth ("token") app
// logins. A single user can have authorized multiple distinct OAuth apps, and the Reports API
// has no way to filter activities.list by a specific app within applicationName="token", so a
// small recent window is fetched and grouped by client_id, keeping only the newest event per
// app. This also covers the "does maxResults=1 return newest-first?" ordering assumption from
// the acceptance criteria: with a >1 window, the true latest is picked client-side regardless.
const oauthAppLookupMaxResults = 25

type usageEventFeed struct {
	c          *gwclient.GoogleWorkspaceClient
	customerID string
	domain     string
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
		if p.Name == name {
			return p.Value
		}
	}
	return ""
}

func hasParameter(name string, parameters []*reportsAdmin.ActivityEventsParameters) bool {
	for _, p := range parameters {
		if p.Name == name {
			return true
		}
	}
	return false
}

// oauthAppActivity is the best (most recent) activity/event seen so far for one OAuth app
// (keyed by client_id) within a user's lookup window.
type oauthAppActivity struct {
	activity   *reportsAdmin.Activity
	event      *reportsAdmin.ActivityEvents
	occurredAt *timestamppb.Timestamp
	appName    string
}

func (f *usageEventFeed) lookupUser(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser) ([]*v2.Event, error) {
	r, err := listActivitiesRateLimited(ctx, client, user.Email, "token", "authorize", "", "", oauthAppLookupMaxResults)
	if err != nil {
		return nil, fmt.Errorf("google-workspace: failed to list token activities for %s: %w", user.Email, err)
	}

	best := map[string]*oauthAppActivity{}
	for _, activity := range r.Items {
		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			continue
		}
		for _, e := range activity.Events {
			if !hasParameter("client_id", e.Parameters) || !hasParameter("app_name", e.Parameters) {
				continue
			}
			clientID := getValueFromParameters("client_id", e.Parameters)
			appName := getValueFromParameters("app_name", e.Parameters)
			if clientID == appName && privateAppIDRegex.MatchString(clientID) {
				// Private app; not reported on.
				continue
			}

			existing, ok := best[clientID]
			if ok && !occurredAt.AsTime().After(existing.occurredAt.AsTime()) {
				continue
			}
			best[clientID] = &oauthAppActivity{activity: activity, event: e, occurredAt: occurredAt, appName: appName}
		}
	}

	events := make([]*v2.Event, 0, len(best))
	for clientID, b := range best {
		userTrait, err := resource.NewUserTrait(
			resource.WithEmail(b.activity.Actor.Email, true),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create user trait: %w", err)
		}
		events = append(events, &v2.Event{
			Id:         strconv.FormatInt(b.activity.Id.UniqueQualifier, 10),
			OccurredAt: b.occurredAt,
			Event: &v2.Event_UsageEvent{
				UsageEvent: &v2.UsageEvent{
					TargetResource: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: resourceTypeEnterpriseApplication.Id,
							Resource:     clientID,
						},
						DisplayName: b.appName,
					},
					ActorResource: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: resourceTypeUser.Id,
							Resource:     b.activity.Actor.ProfileId,
						},
						DisplayName: b.activity.Actor.Email,
						Annotations: annotations.New(userTrait),
						Status: v2.Status_builder{
							Status: v2.Status_RESOURCE_STATUS_ENABLED,
						}.Build(),
					},
				},
			},
		})
	}

	return events, nil
}

func (f *usageEventFeed) ListEvents(ctx context.Context, _ *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	events, streamState, err := scanUsersForEvents(ctx, f.c, f.customerID, f.domain, pToken, f.lookupUser)
	if err != nil {
		return nil, nil, nil, err
	}
	return events, streamState, nil, nil
}

func (f *usageEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "usage_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
	}
}

func newUsageEventFeed(client *gwclient.GoogleWorkspaceClient, customerID, domain string) *usageEventFeed {
	return &usageEventFeed{
		c:          client,
		customerID: customerID,
		domain:     domain,
	}
}
