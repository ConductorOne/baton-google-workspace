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

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// usersPerEventFeedCall bounds how many users are processed per ListEvents invocation, so a
// single call issues at most this many Reports API filter-queries and returns quickly instead
// of blocking on the shared 250/min quota for an entire directory page (up to 500 users).
const usersPerEventFeedCall = 25

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
func scanUsersForEvents(
	ctx context.Context,
	client *gwclient.GoogleWorkspaceClient,
	customerID, domain string,
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
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to list users for event feed: %w", err)
		}
		cursor.DirectoryPageToken = usersResp.NextPageToken
		for _, u := range usersResp.Users {
			if u.PrimaryEmail == "" || u.Id == "" {
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
	cursor.PendingUsers = cursor.PendingUsers[len(batch):]

	events := []*v2.Event{}
	for _, u := range batch {
		userEvents, err := lookup(ctx, client, u)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, userEvents...)
	}

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
