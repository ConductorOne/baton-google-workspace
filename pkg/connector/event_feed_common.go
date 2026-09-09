// event_feed_common.go provides a shared driver for the three "last app login" event feeds
// (usage_event_feed.go, google_login_event_feed.go, saml_event_feed.go).
//
// These feeds no longer replay bulk activity history for all users (userKey="all", paginated
// across up to 180 days). Instead, each feed walks the user directory page by page — reusing
// the same paginated user listing as OAuth app discovery — and, for a small bounded batch of
// users per call, asks the Reports API for only that user's most recent login(s) per app.
// This bounds per-call cost to a fixed number of Reports API calls, checkpoints resumably via
// pagination.StreamToken, and never loops internally across directory pages (see
// ref-antipatterns.md, "Client-Side Pagination Loop").
//
// Ordering note: activities.list ordering is undocumented, so with startTime=180 days back and
// maxResults=50, each lookup still picks the maximum occurredAt client-side rather than trusting result order.
package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// usersPerEventFeedCall bounds how many users are processed per ListEvents invocation, so a
// single call issues at most this many Reports API filter-queries and returns quickly instead
// of blocking on the shared 250/min quota for an entire directory page (up to 500 users).
const usersPerEventFeedCall = 25

// pendingUser.Retries caps the attempt count for this user lookup operation to prevent
// a repeatedly failing user from blocking the batch. The user is skipped after 2 failures.
type pendingUser struct {
	Email   string `json:"email"`
	ID      string `json:"id"`
	Retries int    `json:"retries,omitempty"`
}

const maxUserLookupRetries = 2

// userScanCursor tracks progress through a rolling, continuous walk of the user directory.
// PendingUsers holds users fetched from the current directory page not yet processed;
// DirectoryPageToken is the token for the directory page after the one already fetched.
// When both are empty/exhausted, the walk is complete and the cursor resets to nil so the
// next call starts a fresh pass — there is no "since last poll" time window to track, since
// each lookup always asks for the current latest login, not a delta.
type userScanCursor struct {
	PendingUsers       []pendingUser `json:"pending_users,omitempty"`
	DirectoryPageToken string        `json:"directory_page_token,omitempty"`
}

func unmarshalUserScanCursor(pToken *pagination.StreamToken) (*userScanCursor, error) {
	if pToken == nil {
		return unmarshalUserScanCursorFromString("")
	}
	return unmarshalUserScanCursorFromString(pToken.Cursor)
}

// unmarshalUserScanCursorFromString decodes a userScanCursor from a plain opaque token string,
// used directly by ResourceSyncerV2 pagination (resource.SyncOpAttrs.PageToken.Token /
// resource.SyncOpResults.NextPageToken), which — unlike EventFeed's pagination.StreamToken —
// carries the cursor as a bare string.
func unmarshalUserScanCursorFromString(s string) (*userScanCursor, error) {
	c := &userScanCursor{}
	if s == "" {
		return c, nil
	}
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("failed to decode page token: %w", err)
	}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("failed to unmarshal page token JSON: %w", err)
	}
	return c, nil
}

func (c *userScanCursor) marshal() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to marshal page token: %w", err)
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// userEventLookup fetches events for a single user via at most one Reports API call.
type userEventLookup func(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser) ([]*v2.Event, error)

// scanUsersForEvents drives one bounded step of the rolling user-directory walk shared by all
// three "last login" event feeds.
//
// earliestEvent is the caller-supplied floor (event-feed-start-at / the SDK's earliestEvent
// param): since each lookup only ever returns a user's *current* most recent login — which may
// be older than earliestEvent, or unchanged since the last pass — any event whose OccurredAt
// falls before earliestEvent is dropped here rather than emitted. A nil earliestEvent applies no
// floor.
func scanUsersForEvents(
	ctx context.Context,
	client *gwclient.GoogleWorkspaceClient,
	customerID, domain string,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
	lookup userEventLookup,
) ([]*v2.Event, *pagination.StreamState, error) {
	cursor, err := unmarshalUserScanCursor(pToken)
	if err != nil {
		return nil, nil, err
	}

	if len(cursor.PendingUsers) == 0 {
		usersResp, err := client.ListUserIDsPage(ctx, customerID, domain, cursor.DirectoryPageToken)
		if err != nil {
			// Preserve the cursor as-is so a transient Directory API failure does not rewind
			// the walk back to the start on retry.
			cursorToken, marshalErr := cursor.marshal()
			if marshalErr != nil {
				return nil, nil, fmt.Errorf("google-workspace-connector: failed to marshal cursor token in event feed: %w", marshalErr)
			}
			return nil, &pagination.StreamState{Cursor: cursorToken, HasMore: true},
				fmt.Errorf("google-workspace-connector: failed to list users for event feed: %w", err)
		}
		cursor.DirectoryPageToken = usersResp.NextPageToken
		for _, u := range usersResp.Users {
			if u.PrimaryEmail == "" || u.Id == "" {
				ctxzap.Extract(ctx).Debug("google-workspace-connector: directory user missing id or primary email, skipping for event feed",
					zap.String("user_id", u.Id))
				continue
			}
			cursor.PendingUsers = append(cursor.PendingUsers, pendingUser{Email: u.PrimaryEmail, ID: u.Id})
		}

		if len(cursor.PendingUsers) == 0 && cursor.DirectoryPageToken == "" {
			// Directory is empty (or the last page had no usable users) and there is no next
			// page: the walk is complete. Reset so the next call starts a fresh pass.
			return []*v2.Event{}, &pagination.StreamState{Cursor: "", HasMore: false}, nil
		}
	}

	// Quota already drained: fail fast with a classified error so the SDK backs off, instead of
	// burning the per-user retry budget against a wall we already know is up.
	if sharedReportsRateLimiter.AvailableTokens() < 1 {
		cursorToken, marshalErr := cursor.marshal()
		if marshalErr != nil {
			return nil, nil, fmt.Errorf("failed to marshal cursor token in event feed: %w", marshalErr)
		}
		return nil, &pagination.StreamState{Cursor: cursorToken, HasMore: true},
			uhttp.WrapErrors(codes.ResourceExhausted, "google-workspace-connector: reports api quota exhausted, deferring")
	}

	batch := cursor.PendingUsers
	if len(batch) > usersPerEventFeedCall {
		batch = batch[:usersPerEventFeedCall]
	}

	events := []*v2.Event{}
	// retryQueue holds batch users whose lookup failed but haven't hit maxUserLookupRetries yet,
	// so they stay queued for the next call instead of being dropped.
	retryQueue := make([]pendingUser, 0, len(batch))
	for _, u := range batch {
		if u.Retries >= maxUserLookupRetries {
			// Failed too many times already: skip instead of retrying forever.
			ctxzap.Extract(ctx).Warn("google-workspace-connector: user exceeded lookup retry limit for event feed, skipping",
				zap.String("user", u.Email), zap.Int("retries", u.Retries))
			continue
		}

		userEvents, err := lookup(ctx, client, u)
		if err != nil {
			// Must return with a nil error: the SDK discards streamState.
			// Track the failure and retry this user on the next call.
			u.Retries++
			ctxzap.Extract(ctx).Warn("google-workspace-connector: user lookup failed for event feed, will retry",
				zap.String("user", u.Email), zap.Int("retries", u.Retries), zap.Error(err))
			retryQueue = append(retryQueue, u)
			continue
		}
		for _, e := range userEvents {
			if earliestEvent != nil && e.GetOccurredAt() != nil && e.GetOccurredAt().AsTime().Before(earliestEvent.AsTime()) {
				continue
			}
			events = append(events, e)
		}
	}
	cursor.PendingUsers = append(retryQueue, cursor.PendingUsers[len(batch):]...)

	hasMore := len(cursor.PendingUsers) > 0 || cursor.DirectoryPageToken != ""
	if !hasMore {
		cursor = &userScanCursor{}
	}

	cursorToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal cursor token in event feed: %w", err)
	}

	return events, &pagination.StreamState{Cursor: cursorToken, HasMore: hasMore}, nil
}
