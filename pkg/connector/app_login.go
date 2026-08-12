// app_login.go discovers which OAuth and Google Workspace apps users have accessed.
// It uses two data sources: the Directory API's token list (OAuth apps a user has granted access to)
// and the Admin Reports audit log (actual login events for OAuth, SAML, and Google Workspace apps).
// Results are stored in the session so applicationResource.List() and Grants() can read them
// without re-fetching across sync phases.
//
// All three data sources are looked up per user, in one bounded batch per applicationResource.List()
// call, using the same rolling user-directory walk as the usage/login event feeds
// (see event_feed_common.go). There is no internal loop across directory pages or Reports pages
// inside a single List() call — pagination is driven entirely by resource.SyncOpAttrs.PageToken /
// resource.SyncOpResults.NextPageToken, so the SDK checkpoints progress between calls.
package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

const (
	reportsAppLogin = "login"
	reportsAppSAML  = "saml"

	samlAppIDPrefix               = "saml:"
	googleWorkspaceAppID          = "google_workspace"
	googleWorkspaceAppDisplayName = "Google Workspace"
	// appLoginUsersPerPage bounds how many users applicationResource.List() processes per call,
	// mirroring usersPerEventFeedCall's rationale: each user costs one Directory Tokens.list call
	// plus up to two rate-limited Reports API filter-queries, so a call must stay small to return
	// quickly instead of blocking on the shared quota for an entire directory page.
	appLoginUsersPerPage = 25
	// oauthPresenceValue is the sentinel stored when a user has authorized an OAuth app via
	// Tokens.List() but no Reports timestamp is available. Epoch ensures any real timestamp
	// from the Reports API takes precedence.
	oauthPresenceValue = "1970-01-01T00:00:00Z"
)

var (
	appLoginOAuthAppsNamespace     = sessions.WithPrefix("app_login_oauth_apps")
	appLoginDirectoryUserNamespace = sessions.WithPrefix("app_login_directory_user")
	appLoginEmittedAppNamespace    = sessions.WithPrefix("app_login_emitted_app")
	samlProfileMapNamespace        = sessions.WithPrefix("saml_profile_map")
	samlProfileMapLoadedNamespace  = sessions.WithPrefix("saml_profile_map_loaded")
)

func appLoginLoginsNamespace(appID string) sessions.SessionStoreOption {
	return sessions.WithPrefix("app_login_logins:" + appID)
}

// oauthAppEntry describes one OAuth app a user has authorized, as discovered via Tokens.list.
type oauthAppEntry struct {
	clientID    string
	displayText string
}

// scanAppLoginsPage advances one bounded step of the rolling user-directory walk, discovering
// OAuth/SAML apps and recording each user's latest login timestamp per app in the session.
// It returns any apps discovered on this page that have not been returned by a prior call
// (appID -> displayName, oauthApp or samlApp), plus the next page token (empty when the walk
// has completed a full pass).
func scanAppLoginsPage(
	ctx context.Context,
	ss sessions.SessionStore,
	client *gwclient.GoogleWorkspaceClient,
	customerID, domain string,
	pageToken string,
	samlProfileMap map[string]string,
) (map[string]string, map[string]string, string, error) {
	cursor, err := unmarshalUserScanCursorFromString(pageToken)
	if err != nil {
		return nil, nil, "", err
	}

	if len(cursor.PendingUsers) == 0 {
		usersResp, err := client.ListUserIDsPage(ctx, customerID, domain, cursor.DirectoryPageToken)
		if err != nil {
			return nil, nil, "", fmt.Errorf("google-workspace-connector: failed to list users for applications: %w", err)
		}
		cursor.DirectoryPageToken = usersResp.NextPageToken

		dirUserBatch := make(map[string]string, len(usersResp.Users))
		for _, u := range usersResp.Users {
			if u.Id == "" || u.PrimaryEmail == "" {
				ctxzap.Extract(ctx).Debug("google-workspace-connector: directory user missing id or primary email, skipping for app login discovery",
					zap.String("user_id", u.Id))
				continue
			}
			cursor.PendingUsers = append(cursor.PendingUsers, pendingUser{Email: u.PrimaryEmail, ID: u.Id})
			dirUserBatch[u.Id] = "1"
		}
		if len(dirUserBatch) > 0 {
			if err := session.SetManyJSON(ctx, ss, dirUserBatch, appLoginDirectoryUserNamespace); err != nil {
				return nil, nil, "", fmt.Errorf("google-workspace-connector: failed to store directory user IDs in session: %w", err)
			}
		}

		if len(cursor.PendingUsers) == 0 && cursor.DirectoryPageToken == "" {
			// Return empty (not nil) maps: callers range over and write into these.
			return map[string]string{}, map[string]string{}, "", nil
		}
	}

	batch := cursor.PendingUsers
	if len(batch) > appLoginUsersPerPage {
		batch = batch[:appLoginUsersPerPage]
	}
	cursor.PendingUsers = cursor.PendingUsers[len(batch):]

	newOAuthApps := map[string]string{}
	newSAMLApps := map[string]string{}
	for _, u := range batch {
		oauthApps, err := fetchUserOAuthApps(ctx, client, u)
		if err != nil {
			return nil, nil, "", err
		}
		if err := storeOAuthLogins(ctx, ss, u.ID, oauthApps, newOAuthApps); err != nil {
			return nil, nil, "", err
		}

		if client.ReportService != nil {
			if err := recordLatestGoogleLogin(ctx, ss, client, u); err != nil {
				return nil, nil, "", err
			}

			if err := recordLatestSAMLLogins(ctx, ss, client, u, samlProfileMap, newSAMLApps); err != nil {
				return nil, nil, "", err
			}
		}
	}

	hasMore := len(cursor.PendingUsers) > 0 || cursor.DirectoryPageToken != ""
	if !hasMore {
		cursor = &userScanCursor{}
	}
	nextPageToken, err := cursor.marshal()
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to marshal cursor token in app login scan: %w", err)
	}
	if !hasMore {
		nextPageToken = ""
	}

	return newOAuthApps, newSAMLApps, nextPageToken, nil
}

// fetchUserOAuthApps lists OAuth tokens for a single user.
//
// A failed ListTokens call is only tolerated (the user is skipped) when it is a genuine 404 —
// i.e. the user was deleted mid-sync. Any other failure (403/429/5xx/network/context) is
// surfaced as an error that aborts the sync. Silently skipping a user on a transient error
// would drop that user's OAuth-app associations: apps only that user authorized may then be
// missed entirely, and the user's app-access grants would be under-reported — which c1 prunes
// as a revocation. Failing loudly forces a retry instead of persisting a partial result.
func fetchUserOAuthApps(ctx context.Context, client *gwclient.GoogleWorkspaceClient, u pendingUser) ([]oauthAppEntry, error) {
	tokenResp, err := client.ListTokens(ctx, u.ID)
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			ctxzap.Extract(ctx).Debug("google-workspace-connector: user not found during token fetch, skipping", zap.String("user_id", u.ID))
			return nil, nil
		}
		return nil, fmt.Errorf("google-workspace-connector: failed to list tokens for user %s: %w", u.ID, err)
	}

	var filtered []oauthAppEntry
	for _, t := range tokenResp.Items {
		if t.ClientId == "" || t.DisplayText == "" {
			continue
		}
		if t.ClientId == t.DisplayText && privateAppIDRegex.MatchString(t.ClientId) {
			continue
		}
		filtered = append(filtered, oauthAppEntry{clientID: t.ClientId, displayText: t.DisplayText})
	}
	return filtered, nil
}

func storeOAuthLogins(ctx context.Context, ss sessions.SessionStore, userID string, apps []oauthAppEntry, newApps map[string]string) error {
	if len(apps) == 0 {
		return nil
	}
	appsBatch := make(map[string]string, len(apps))
	for _, a := range apps {
		appsBatch[a.clientID] = a.displayText
		newApps[a.clientID] = a.displayText
	}
	if err := session.SetManyJSON(ctx, ss, appsBatch, appLoginOAuthAppsNamespace); err != nil {
		return fmt.Errorf("google-workspace-connector: failed to store oauth apps in session: %w", err)
	}

	for _, a := range apps {
		if err := storeLoginIfNewer(ctx, ss, a.clientID, userID, oauthPresenceValue); err != nil {
			return err
		}
	}
	return nil
}

// storeLoginIfNewer writes loginTime for (appID, userID) only if it is newer than what is
// already stored, so a later page's stale/earlier timestamp never clobbers a fresher one.
func storeLoginIfNewer(ctx context.Context, ss sessions.SessionStore, appID, userID, loginTime string) error {
	ns := appLoginLoginsNamespace(appID)
	existing, found, err := session.GetJSON[string](ctx, ss, userID, ns)
	if err != nil {
		return fmt.Errorf("google-workspace-connector: failed to read login from session for app %s user %s: %w", appID, userID, err)
	}
	if found && existing >= loginTime {
		return nil
	}
	if err := session.SetJSON(ctx, ss, userID, loginTime, ns); err != nil {
		return fmt.Errorf("google-workspace-connector: failed to store login data for app %s user %s: %w", appID, userID, err)
	}
	return nil
}

// recordLatestGoogleLogin looks up a single user's most recent Google Workspace sign-in, which
// has exactly one target app, so no per-app grouping is needed (unlike SAML/OAuth apps).
func recordLatestGoogleLogin(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, u pendingUser) error {
	r, err := listActivitiesRateLimited(ctx, client, u.Email, reportsAppLogin, "login_success", "", "", googleLoginLookupMaxResults)
	if err != nil {
		return fmt.Errorf("google-workspace-connector: failed to fetch google login activity for %s: %w", u.Email, err)
	}

	var latest time.Time
	found := false
	for _, activity := range r.Items {
		ts := convertIdTimeToTimestamp(activity.Id.Time)
		if ts == nil || activity.Actor.ProfileId == "" {
			continue
		}
		if !found || ts.AsTime().After(latest) {
			latest = ts.AsTime()
			found = true
		}
	}
	if !found {
		return nil
	}

	return storeLoginIfNewer(ctx, ss, googleWorkspaceAppID, u.ID, latest.UTC().Format(time.RFC3339))
}

// recordLatestSAMLLogins looks up a single user's recent SAML logins, grouped by app (since a
// user can have distinct, independently-timestamped logins to multiple SAML apps), and records
// the newest login per app plus any newly-discovered app names.
func recordLatestSAMLLogins(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, u pendingUser, samlProfileMap map[string]string, newApps map[string]string) error {
	r, err := listActivitiesRateLimited(ctx, client, u.Email, reportsAppSAML, "login_success", "", "", samlAppLookupMaxResults)
	if err != nil {
		return fmt.Errorf("google-workspace-connector: failed to fetch saml activity for %s: %w", u.Email, err)
	}

	type best struct {
		appID string
		name  string
		at    time.Time
	}
	bestByApp := map[string]best{}
	for _, activity := range r.Items {
		ts := convertIdTimeToTimestamp(activity.Id.Time)
		if ts == nil || activity.Actor.ProfileId == "" {
			continue
		}
		for _, ev := range activity.Events {
			appName := getValueFromParameters("application_name", ev.Parameters)
			if appName == "" {
				continue
			}
			stableID := appName
			if profileName, ok := samlProfileMap[appName]; ok {
				stableID = profileName
			}
			appID := samlAppIDPrefix + stableID
			existing, ok := bestByApp[appID]
			if ok && !ts.AsTime().After(existing.at) {
				continue
			}
			bestByApp[appID] = best{appID: appID, name: appName, at: ts.AsTime()}
		}
	}

	for appID, b := range bestByApp {
		if _, seen := newApps[appID]; !seen {
			newApps[appID] = b.name
		}
		if err := storeLoginIfNewer(ctx, ss, appID, u.ID, b.at.UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return nil
}

func discoverSAMLApps(profileMap map[string]string) map[string]string {
	apps := make(map[string]string, len(profileMap))
	for displayName, profileName := range profileMap {
		apps[samlAppIDPrefix+profileName] = displayName
	}
	return apps
}

// fetchSAMLProfileMap calls Cloud Identity to build a displayName → profile.Name map.
// Returns nil without error if the service is unavailable or the call fails (soft failure).
func fetchSAMLProfileMap(ctx context.Context, client *gwclient.GoogleWorkspaceClient, customerID string) map[string]string {
	if client.CloudIdentityService == nil {
		return nil
	}
	m, err := client.BuildSAMLProfileMap(ctx, customerID)
	if err != nil {
		ctxzap.Extract(ctx).Info("google-workspace: failed to load SAML profiles from Cloud Identity; SAML app IDs will use display names. "+
			"Grant the 'https://www.googleapis.com/auth/cloud-identity.inboundsso.readonly' scope to fix this.", zap.Error(err))
		return nil
	}
	return m
}

// loadCloudIdentitySAMLProfileMap resolves the Cloud Identity SAML profile map on the first
// applicationResource.List() page of a sync, applying the isCloudIdentityAPIDisabledError
// fallback, and caches the result in session so later pages of the same sync reuse it without
// re-calling Cloud Identity or re-running the fallback logic.
func loadCloudIdentitySAMLProfileMap(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, customerID string, isFirstPage bool) (map[string]string, error) {
	if !isFirstPage {
		m, err := session.GetAllJSON[string](ctx, ss, samlProfileMapNamespace)
		if err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to read saml profile map from session: %w", err)
		}
		return m, nil
	}

	profileMap, err := client.BuildSAMLProfileMap(ctx, customerID)
	if err != nil {
		// Exception: when the Cloud Identity API is not enabled in the customer's GCP project,
		// the service still initialises (the scope was granted) but every call returns a
		// permanent 403 SERVICE_DISABLED. That is a stable feature-unavailable condition, not a
		// transient blip — such a customer's SAML apps have ALWAYS been resolved by display
		// name, so there is no profile-name state to flip and nothing to prune. Treat it like a
		// missing scope: warn and fall back to display-name IDs instead of failing the whole
		// sync. Any other failure (transient 5xx/429, network, or a 403 that is NOT "API
		// disabled") still propagates: falling back to a nil profile map would drop SAML apps
		// discovered only via Cloud Identity AND flip the IDs of SAML apps found via login
		// events from their stable profile name to a display-name-derived ID, causing c1 to
		// prune the old resource and all of its access grants on a transient blip.
		if isCloudIdentityAPIDisabledError(err) {
			ctxzap.Extract(ctx).Info("google-workspace: Cloud Identity API is not enabled for this project; "+
				"SAML app IDs will use display names. Enable the Cloud Identity API "+
				"(cloudidentity.googleapis.com) for this project to use stable SAML profile IDs.",
				zap.Error(err))
			profileMap = nil
		} else {
			return nil, fmt.Errorf("google-workspace-connector: failed to load SAML profiles from Cloud Identity: %w", err)
		}
	}

	if len(profileMap) > 0 {
		if err := session.SetManyJSON(ctx, ss, profileMap, samlProfileMapNamespace); err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to store saml profile map in session: %w", err)
		}
	}
	return profileMap, nil
}

// loadSAMLProfileMap returns the SAML profile map, using the session store as a cache
// so Cloud Identity is queried at most once per sync.
func loadSAMLProfileMap(ctx context.Context, client *gwclient.GoogleWorkspaceClient, customerID string) (map[string]string, error) {
	ss, _ := ctx.Value(sessions.SessionStoreKey{}).(sessions.SessionStore)
	if ss == nil {
		return fetchSAMLProfileMap(ctx, client, customerID), nil
	}
	return loadSAMLProfileMapFromSession(ctx, ss, client, customerID)
}

func loadSAMLProfileMapFromSession(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, customerID string) (map[string]string, error) {
	_, loaded, err := session.GetJSON[string](ctx, ss, "done", samlProfileMapLoadedNamespace)
	if err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to check saml profile map loaded flag: %w", err)
	}
	if loaded {
		m, err := session.GetAllJSON[string](ctx, ss, samlProfileMapNamespace)
		if err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to read saml profile map from session: %w", err)
		}
		return m, nil
	}

	profileMap := fetchSAMLProfileMap(ctx, client, customerID)
	if len(profileMap) > 0 {
		if err := session.SetManyJSON(ctx, ss, profileMap, samlProfileMapNamespace); err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to store saml profile map in session: %w", err)
		}
	}
	if err := session.SetJSON(ctx, ss, "done", "true", samlProfileMapLoadedNamespace); err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to mark saml profile map as loaded: %w", err)
	}
	return profileMap, nil
}

// isAppEmitted reports whether appID has already been returned as a resource by a prior
// applicationResource.List() call. Read-only: the caller accumulates newly-emitted app IDs
// locally and persists them in a single batched write only once the whole List() call has
// succeeded — see the emittedThisCall handling in applicationResource.List(). Writing markers
// per-app as soon as each resource is built would leave earlier apps marked emitted even if a
// later item in the same call aborted List() with an error (the SDK retries with the same page
// token), silently skipping those apps forever without ever having actually been returned.
func isAppEmitted(ctx context.Context, ss sessions.SessionStore, appID string) (bool, error) {
	_, found, err := session.GetJSON[string](ctx, ss, appID, appLoginEmittedAppNamespace)
	if err != nil {
		return false, fmt.Errorf("google-workspace-connector: failed to check emitted-app marker for %s: %w", appID, err)
	}
	return found, nil
}
