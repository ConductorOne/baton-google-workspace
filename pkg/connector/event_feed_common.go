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
// Ordering note: it is not documented whether activities.list returns newest-first when no
// startTime/orderBy is given, so each lookup fetches a small bounded window (not maxResults=1)
// and picks the maximum occurredAt client-side, per the plan's acceptance-criteria fallback.
package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// usersPerEventFeedCall bounds how many users are considered per ListEvents invocation. This
// alone does not bound the number of Reports API calls issued (see maxLookupCallsPerEventFeedCall
// below), but keeps the directory-side bookkeeping (ListTokens, cursor size) proportional.
const usersPerEventFeedCall = 25

// maxLookupCallsPerEventFeedCall caps Reports API calls per ListEvents invocation. Bounds cost,
// not time (shared rate limiter, retries with backoff) — see maxEventFeedCallDuration for that.
const maxLookupCallsPerEventFeedCall = 60

// maxEventFeedCallDuration is a soft wall-clock budget per ListEvents invocation, checked between
// users, since a call-count budget alone doesn't bound elapsed time.
const maxEventFeedCallDuration = 45 * time.Second

type pendingUser struct {
	Email string `json:"email"`
	ID    string `json:"id"`
}

// userScanCursor tracks progress through a rolling, continuous walk of the user directory.
// PendingUsers holds users fetched from the current directory page not yet processed;
// DirectoryPageToken is the token for the directory page after the one already fetched.
// When both are empty/exhausted, the walk is complete and the cursor resets to nil so the
// next call starts a fresh pass — there is no "since last poll" time window to track, since
// each lookup always asks for the current latest login, not a delta.
//
// ResumeState is opaque, feed-defined progress for PendingUsers[0]'s own lookup (e.g. which
// authorized app to resume from), set when a call exhausts its budget mid-user.
type userScanCursor struct {
	PendingUsers       []pendingUser `json:"pending_users,omitempty"`
	DirectoryPageToken string        `json:"directory_page_token,omitempty"`
	ResumeState        string        `json:"resume_state,omitempty"`
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

// userEventLookup fetches events for one user, spending at most `budget` Reports API calls.
// resumeState picks up where a prior call for this user left off ("" = start fresh); if more
// than `budget` calls are needed, it returns a non-empty nextResumeState instead of finishing.
// consumed is how many calls this invocation issued (meaningful only when err == nil).
type userEventLookup func(ctx context.Context, client *gwclient.GoogleWorkspaceClient, user pendingUser, resumeState string, budget int) (events []*v2.Event, nextResumeState string, consumed int, err error)

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

	batch := cursor.PendingUsers
	if len(batch) > usersPerEventFeedCall {
		batch = batch[:usersPerEventFeedCall]
	}

	// finish marshals the cursor and returns the call's result; shared by every exit path.
	finish := func(events []*v2.Event, hasMore bool) ([]*v2.Event, *pagination.StreamState, error) {
		if !hasMore {
			cursor = &userScanCursor{}
		}
		cursorToken, err := cursor.marshal()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal cursor token in event feed: %w", err)
		}
		return events, &pagination.StreamState{Cursor: cursorToken, HasMore: hasMore}, nil
	}

	events := []*v2.Event{}
	budget := maxLookupCallsPerEventFeedCall
	resumeState := cursor.ResumeState
	start := time.Now()

	for i, u := range batch {
		if budget <= 0 || time.Since(start) >= maxEventFeedCallDuration {
			// Budget or time spent before starting u; resume here fresh next call.
			cursor.PendingUsers = cursor.PendingUsers[i:]
			cursor.ResumeState = ""
			return finish(events, true)
		}

		userEvents, nextResume, consumed, err := lookup(ctx, client, u, resumeState, budget)
		if err != nil {
			// The SDK drops this StreamState on error, so a retry replays the last successful
			// cursor regardless; return it unchanged (it's untouched at this point) rather than
			// advancing it for no effect.
			cursorToken, marshalErr := cursor.marshal()
			if marshalErr != nil {
				return nil, nil, fmt.Errorf("failed to marshal cursor token in event feed: %w", marshalErr)
			}
			return nil, &pagination.StreamState{Cursor: cursorToken, HasMore: true}, err
		}
		budget -= consumed
		resumeState = "" // only the batch's first (possibly-resumed) user carries one in

		for _, e := range userEvents {
			if earliestEvent != nil && e.GetOccurredAt() != nil && e.GetOccurredAt().AsTime().Before(earliestEvent.AsTime()) {
				continue
			}
			events = append(events, e)
		}

		if nextResume != "" {
			// u isn't finished; keep it at the front and stop the batch here.
			cursor.PendingUsers = cursor.PendingUsers[i:]
			cursor.ResumeState = nextResume
			return finish(events, true)
		}
	}

	cursor.PendingUsers = cursor.PendingUsers[len(batch):]
	cursor.ResumeState = ""

	hasMore := len(cursor.PendingUsers) > 0 || cursor.DirectoryPageToken != ""
	return finish(events, hasMore)
}
