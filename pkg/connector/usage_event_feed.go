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
	"golang.org/x/sync/errgroup"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

var privateAppIDRegex = regexp.MustCompile("[0-9]{21}")

// oauthAppLookupMaxResults bounds each per-(user, app) Reports API lookup. Since the query is
// now scoped to one specific client_id via the `filters` param, there's no cross-app crowding to
// worry about — this only needs to cover the "does maxResults=1 return newest-first?" ordering
// assumption from the acceptance criteria: with a >1 window, the true latest is picked
// client-side regardless of Google's actual ordering.
const oauthAppLookupMaxResults = 5

// maxConcurrentAppLookups bounds concurrent per-app Reports lookups for one user, and also sizes
// the fan-out chunks in lookupUser (see below) between which the deadline budget is checked. The
// shared reportsRateLimiter still caps overall quota use; this just overlaps network latency.
const maxConcurrentAppLookups = 8

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

// oauthApp is one distinct app a user has authorized, as discovered via Directory Tokens.list.
type oauthApp struct {
	ClientID    string
	DisplayText string
}

// distinctAuthorizedApps enumerates this user's authorized OAuth apps via Directory Tokens.list,
// deduping repeated client_ids and filtering out private apps before spending any Reports budget.
func distinctAuthorizedApps(tokenResp *directoryAdmin.Tokens) []oauthApp {
	seenClientIDs := make(map[string]struct{}, len(tokenResp.Items))
	apps := make([]oauthApp, 0, len(tokenResp.Items))
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
		apps = append(apps, oauthApp{ClientID: t.ClientId, DisplayText: t.DisplayText})
	}
	return apps
}

// lookupUser issues one Reports API lookup per authorized OAuth app, scoped by client_id. A user
// can have more apps than the per-call budget allows: resumeState is the client_id of the last
// app processed on a prior call for this user, and if apps remain after spending budget,
// nextResumeState carries the new last-processed client_id forward. A client_id (rather than a
// positional index) is used because Tokens.list is re-fetched fresh on every call and its
// ordering is not guaranteed stable across calls — a positional index could silently skip or
// re-visit apps if the set of authorized apps changes between calls. If resumeState's client_id
// is no longer present (e.g. the app was deauthorized between calls), lookupUser conservatively
// restarts from the beginning rather than guessing a position, which can revisit already-seen
// apps (harmless — the lookup is idempotent) but never skips one.
//
// Per-app lookups run concurrently, up to maxConcurrentAppLookups in flight at once; the shared
// rate limiter still caps quota use, so this only overlaps network latency and retry/backoff
// time. deadline is checked before dispatching each app (never before the first one, so a call
// always makes some progress) — once past it, lookupUser stops dispatching new lookups but still
// waits for the ones already in flight, and returns what it has with nextResumeState set to the
// last app actually dispatched. Without this, a single user with a full budget's worth of apps
// could keep dispatching lookups — each with its own rate-limiter wait and up to
// reportsMaxRetries backoff — well past the caller's soft wall-clock budget.
func (f *usageEventFeed) lookupUser(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser, resumeState string, budget int, deadline time.Time) ([]*v2.Event, string, int, error) {
	tokenResp, err := client.ListTokens(ctx, user.ID)
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			// Benign: the user was deleted between the directory listing and this lookup.
			return nil, "", 0, nil
		}
		return nil, "", 0, fmt.Errorf("google-workspace: failed to list oauth tokens for %s: %w", user.Email, err)
	}
	apps := distinctAuthorizedApps(tokenResp)

	startIdx := 0
	if resumeState != "" {
		for i, app := range apps {
			if app.ClientID == resumeState {
				startIdx = i + 1
				break
			}
		}
	}

	remaining := apps[startIdx:]
	if len(remaining) == 0 {
		return nil, "", 0, nil
	}
	toProcess := remaining
	if len(toProcess) > budget {
		toProcess = toProcess[:budget]
	}

	results := make([]*v2.Event, len(toProcess))
	consumed := 0
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentAppLookups)
	for i, app := range toProcess {
		if i > 0 && !deadline.IsZero() && time.Now().After(deadline) {
			// Past budget: stop dispatching new lookups. Ones already in flight are awaited
			// below; nextResume picks up right after the last app actually dispatched.
			break
		}
		consumed = i + 1
		g.Go(func() error {
			event, err := f.lookupAppLogin(gctx, client, user, app.ClientID, app.DisplayText)
			if err != nil {
				return err
			}
			results[i] = event
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, "", 0, err
	}

	events := make([]*v2.Event, 0, consumed)
	for _, e := range results[:consumed] {
		if e != nil {
			events = append(events, e)
		}
	}

	nextResume := ""
	if consumed > 0 && startIdx+consumed < len(apps) {
		nextResume = toProcess[consumed-1].ClientID
	}
	return events, nextResume, consumed, nil
}

// lookupAppLogin fetches this user's most recent "authorize" activity for one specific OAuth
// app (client_id), returning nil if there is no such activity within the lookup window.
func (f *usageEventFeed) lookupAppLogin(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser, clientID, displayName string) (*v2.Event, error) {
	filters := "client_id==" + clientID
	r, err := listActivitiesFilteredRateLimited(ctx, client, user.Email, "token", "authorize", "", "", filters, oauthAppLookupMaxResults)
	if err != nil {
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
