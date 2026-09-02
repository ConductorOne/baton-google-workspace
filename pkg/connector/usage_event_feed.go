package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

var privateAppIDRegex = regexp.MustCompile("[0-9]{21}")

// oauthAppLookupMaxResults bounds each per-(user, app) Reports API lookup; the true latest event
// is picked client-side, so this just needs to be large enough to avoid pagination.
const oauthAppLookupMaxResults = 50

// oauthAppLookupTimeout caps a single (user, app) lookup, including retries. Kept above the
// worst case of a hung attempt plus backoff plus a full retry (~51s) so the hung-attempt retry
// in listActivitiesFilteredRateLimitedBounded has room to complete.
const oauthAppLookupTimeout = 60 * time.Second

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

// oauthAppActivity is the best (most recent) activity/event seen so far for one OAuth app
// (keyed by client_id) within a user's lookup window.
type oauthAppActivity struct {
	activity   *reportsAdmin.Activity
	event      *reportsAdmin.ActivityEvents
	occurredAt *timestamppb.Timestamp
	appName    string
}

// lookupUser enumerates the OAuth apps this user has authorized via Directory API Tokens.list
// (Reports API has no endpoint for "which apps has this user used" — Tokens.list is the cheap,
// Directory-quota source of truth for that), then issues one Reports API lookup per app,
// scoped with filters=client_id==<id>. This trades "1 Reports call per user" (old per-app-type
// window, prone to one app's activity crowding another out of a shared count-bounded window) for
// "1 Directory call + N Reports calls per user" (N = apps that user has authorized) — each app's
// freshness is queried independently, so it can never be crowded out by another app's activity.
func (f *usageEventFeed) lookupUser(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser) ([]*v2.Event, error) {
	tokenResp, err := client.ListTokens(ctx, user.ID)
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			// Benign: the user was deleted between the directory listing and this lookup.
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace: failed to list oauth tokens for %s: %w", user.Email, err)
	}
	// Tokens.list can return multiple Token entries for the same client_id.
	// Dedupe here so each distinct app only ever triggers one Reports API lookup, never repeated ones.
	seenClientIDs := make(map[string]struct{}, len(tokenResp.Items))

	events := make([]*v2.Event, 0, len(tokenResp.Items))
	for _, t := range tokenResp.Items {
		if t.ClientId == "" || t.DisplayText == "" {
			continue
		}
		if t.ClientId == t.DisplayText && privateAppIDRegex.MatchString(t.ClientId) {
			// Private app; not reported on.
			continue
		}
		if _, dup := seenClientIDs[t.ClientId]; dup {
			continue
		}
		seenClientIDs[t.ClientId] = struct{}{}

		event, err := f.lookupAppLogin(ctx, client, user, t.ClientId, t.DisplayText)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
		}
	}

	return events, nil
}

// lookupAppLogin fetches this user's most recent "authorize" activity for one specific OAuth
// app (client_id), returning nil if there is no such activity, or if the lookup timed out.
func (f *usageEventFeed) lookupAppLogin(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser, clientID, displayName string) (*v2.Event, error) {
	l := ctxzap.Extract(ctx)
	filters := "client_id==" + clientID
	startTime := time.Now().Add(-reportsLookback).UTC().Format(time.RFC3339)

	lookupCtx, cancel := context.WithTimeout(ctx, oauthAppLookupTimeout)
	defer cancel()

	r, err := listActivitiesFilteredRateLimitedBounded(lookupCtx, client, user.Email, "token", "authorize", startTime, "", filters, oauthAppLookupMaxResults)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// Our sub-deadline fired, not the caller's context: skip this one app instead of
			// failing the whole batch.
			l.Debug("google-workspace: timed out listing token activities, skipping",
				zap.String("user", user.Email), zap.String("client_id", clientID))
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace: failed to list token activities for %s app %s: %w", user.Email, clientID, err)
	}

	var best *oauthAppActivity
	for _, activity := range r.Items {
		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			continue
		}
		for _, e := range activity.Events {
			// Defensive: confirm the filter actually matched this app, in case Google ever
			// returns extra parameters/events beyond what was requested.
			if getValueFromParameters("client_id", e.Parameters) != clientID {
				continue
			}
			if best == nil || occurredAt.AsTime().After(best.occurredAt.AsTime()) {
				appName := getValueFromParameters("app_name", e.Parameters)
				if appName == "" {
					appName = displayName
				}
				best = &oauthAppActivity{activity: activity, event: e, occurredAt: occurredAt, appName: appName}
			}
		}
	}
	if best == nil {
		return nil, nil
	}

	userTrait, err := resource.NewUserTrait(
		resource.WithEmail(best.activity.Actor.Email, true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create user trait: %w", err)
	}

	return &v2.Event{
		Id:         strconv.FormatInt(best.activity.Id.UniqueQualifier, 10),
		OccurredAt: best.occurredAt,
		Event: &v2.Event_UsageEvent{
			UsageEvent: &v2.UsageEvent{
				TargetResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeEnterpriseApplication.Id,
						Resource:     clientID,
					},
					DisplayName: best.appName,
				},
				ActorResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeUser.Id,
						Resource:     best.activity.Actor.ProfileId,
					},
					DisplayName: best.activity.Actor.Email,
					Status:      &v2.Status{Status: v2.Status_RESOURCE_STATUS_ENABLED},
					Annotations: annotations.New(userTrait),
				},
			},
		},
	}, nil
}

func (f *usageEventFeed) ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	events, streamState, err := scanUsersForEvents(ctx, f.c, f.customerID, f.domain, earliestEvent, pToken, f.lookupUser)
	if err != nil {
		return nil, streamState, nil, err
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
