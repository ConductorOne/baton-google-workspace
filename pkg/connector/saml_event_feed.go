package connector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// samlAppLookupMaxResults bounds the per-user Reports API lookup for SAML app logins. A single
// user can have logged into multiple distinct SAML apps, and activities.list cannot filter by a
// specific app within applicationName="saml", so a small recent window is fetched and grouped
// by resolved app ID, keeping only the newest event per app.
const samlAppLookupMaxResults = 50

// samlAppLookupLookback bounds startTime so each lookup stays fast, per Google's guidance that
// narrower time ranges respond faster; 180 days matches Google's own Reports retention window.
const samlAppLookupLookback = 180 * 24 * time.Hour

// samlAppLookupTimeout caps a single user's lookup, including retries. Kept above the worst case
// of a hung attempt plus backoff plus a full retry (~51s) so the hung-attempt retry in
// listActivitiesRateLimitedBounded has room to complete.
const samlAppLookupTimeout = 60 * time.Second

// samlEventFeed emits UsageEvents from Google Workspace SAML app login activity.
type samlEventFeed struct {
	client     *gwclient.GoogleWorkspaceClient
	customerID string
	domain     string
}

func newSamlEventFeed(client *gwclient.GoogleWorkspaceClient, customerID, domain string) *samlEventFeed {
	return &samlEventFeed{client: client, customerID: customerID, domain: domain}
}

func (f *samlEventFeed) EventFeedMetadata(_ context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "saml_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
	}
}

type samlAppActivity struct {
	activity   *reportsAdmin.Activity
	event      *reportsAdmin.ActivityEvents
	occurredAt *timestamppb.Timestamp
	appName    string
}

// lookupUser tracks SAML app usage via Google's "saml" audit log.
//
// Unlike OAuth apps (see usage_event_feed.go), SAML "login_success" fires on every SSO
// authentication, so last login timestamps are accurate. SAML apps are identified by app name
// (no numeric client_id).
func (f *samlEventFeed) lookupUser(ctx context.Context, client *gwclient.GoogleWorkspaceClient, samlProfileMap map[string]string, user pendingUser) ([]*v2.Event, error) {
	startTime := time.Now().Add(-samlAppLookupLookback).UTC().Format(time.RFC3339)

	lookupCtx, cancel := context.WithTimeout(ctx, samlAppLookupTimeout)
	defer cancel()

	r, err := listActivitiesRateLimitedBounded(lookupCtx, client, user.Email, reportsAppSAML, "login_success", startTime, "", samlAppLookupMaxResults)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// Our sub-deadline fired, not the caller's context: skip this user instead of
			// failing the whole batch.
			ctxzap.Extract(ctx).Warn("google-workspace-connector: timed out listing saml login activities, skipping",
				zap.String("user", user.Email))
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace-connector: failed to list saml login activities for %s: %w", user.Email, err)
	}

	best := map[string]*samlAppActivity{}
	for _, activity := range r.Items {
		if activity.Actor.ProfileId == "" {
			continue
		}
		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			continue
		}
		for _, e := range activity.Events {
			appName := getValueFromParameters("application_name", e.Parameters)
			if appName == "" {
				continue
			}
			appID := appName
			if profileName, ok := samlProfileMap[appName]; ok {
				appID = profileName
			}

			existing, ok := best[appID]
			if ok && !occurredAt.AsTime().After(existing.occurredAt.AsTime()) {
				continue
			}
			best[appID] = &samlAppActivity{activity: activity, event: e, occurredAt: occurredAt, appName: appName}
		}
	}

	events := make([]*v2.Event, 0, len(best))
	for appID, b := range best {
		userTrait, err := resource.NewUserTrait(
			resource.WithEmail(b.activity.Actor.Email, true),
		)
		if err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to create user trait in saml event feed: %w", err)
		}

		events = append(events, &v2.Event{
			Id:         strconv.FormatInt(b.activity.Id.UniqueQualifier, 10),
			OccurredAt: b.occurredAt,
			Event: &v2.Event_UsageEvent{
				UsageEvent: &v2.UsageEvent{
					TargetResource: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: resourceTypeEnterpriseApplication.Id,
							Resource:     samlAppIDPrefix + appID,
						},
						DisplayName: b.appName,
					},
					ActorResource: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: resourceTypeUser.Id,
							Resource:     b.activity.Actor.ProfileId,
						},
						DisplayName: b.activity.Actor.Email,
						Status:      &v2.Status{Status: v2.Status_RESOURCE_STATUS_ENABLED},
						Annotations: annotations.New(userTrait),
					},
				},
			},
		})
	}

	return events, nil
}

func (f *samlEventFeed) ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	// Resolved once per call (not once per user): it lists every SAML profile for the whole
	// customer, so looking it up per-user in the batch would multiply Cloud Identity calls
	// unnecessarily.
	samlProfileMap, err := loadSAMLProfileMap(ctx, f.client, f.customerID)
	if err != nil {
		return nil, nil, nil, err
	}

	events, streamState, err := scanUsersForEvents(ctx, f.client, f.customerID, f.domain, earliestEvent, pToken,
		func(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser) ([]*v2.Event, error) {
			return f.lookupUser(ctx, client, samlProfileMap, user)
		})
	if err != nil {
		return nil, streamState, nil, err
	}
	return events, streamState, nil, nil
}
