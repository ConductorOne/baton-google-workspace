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

type bestActivity struct {
	activity   *reportsAdmin.Activity
	occurredAt *timestamppb.Timestamp
}

// googleLoginLookupMaxResults bounds the per-user Reports API lookup; the true latest event is
// picked client-side, so this just needs to be large enough to avoid pagination.
const googleLoginLookupMaxResults = 50

// googleLoginLookupTimeout caps a single user's lookup, including retries. Kept above the worst
// case of a hung attempt plus backoff plus a full retry (~51s) so the hung-attempt retry in
// listActivitiesRateLimitedBounded has room to complete.
const googleLoginLookupTimeout = 60 * time.Second

// googleLoginEventFeed emits UsageEvents from Google Workspace sign-in activity.
// Unlike SAML/OAuth feeds, the target resource is always Google Workspace itself.
type googleLoginEventFeed struct {
	client     *gwclient.GoogleWorkspaceClient
	customerID string
	domain     string
}

func newGoogleLoginEventFeed(client *gwclient.GoogleWorkspaceClient, customerID, domain string) *googleLoginEventFeed {
	return &googleLoginEventFeed{client: client, customerID: customerID, domain: domain}
}

func (f *googleLoginEventFeed) EventFeedMetadata(_ context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "google_login_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
	}
}

func (f *googleLoginEventFeed) lookupUser(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser) ([]*v2.Event, error) {
	l := ctxzap.Extract(ctx)
	startTime := time.Now().Add(-reportsLookback).UTC().Format(time.RFC3339)

	lookupCtx, cancel := context.WithTimeout(ctx, googleLoginLookupTimeout)
	defer cancel()

	r, err := listActivitiesRateLimitedBounded(lookupCtx, client, user.Email, reportsAppLogin, "login_success", startTime, "", googleLoginLookupMaxResults)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// Our sub-deadline fired, not the caller's context: skip this user instead of
			// failing the whole batch.
			l.Debug("google-workspace-connector: timed out listing google login activities, skipping",
				zap.String("user", user.Email))
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace-connector: failed to list google login activities for %s: %w", user.Email, err)
	}

	var best *bestActivity
	for _, activity := range r.Items {
		if activity.Actor.ProfileId == "" {
			continue
		}
		occurredAt := convertIdTimeToTimestamp(activity.Id.Time)
		if occurredAt == nil {
			continue
		}
		if best == nil || occurredAt.AsTime().After(best.occurredAt.AsTime()) {
			best = &bestActivity{activity: activity, occurredAt: occurredAt}
		}
	}
	if best == nil {
		return nil, nil
	}

	userTrait, err := resource.NewUserTrait(
		resource.WithEmail(best.activity.Actor.Email, true),
	)
	if err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to create user trait in google login event feed: %w", err)
	}

	return []*v2.Event{{
		Id:         strconv.FormatInt(best.activity.Id.UniqueQualifier, 10),
		OccurredAt: best.occurredAt,
		Event: &v2.Event_UsageEvent{
			UsageEvent: &v2.UsageEvent{
				TargetResource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: resourceTypeEnterpriseApplication.Id,
						Resource:     googleWorkspaceAppID,
					},
					DisplayName: googleWorkspaceAppDisplayName,
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
	}}, nil
}

func (f *googleLoginEventFeed) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	events, streamState, err := scanUsersForEvents(ctx, f.client, f.customerID, f.domain, earliestEvent, pToken, f.lookupUser)
	if err != nil {
		return nil, streamState, nil, err
	}
	return events, streamState, nil, nil
}
