// app_login.go discovers which OAuth and Google Workspace apps users have accessed.
// It uses two data sources: the Directory API's token list (OAuth apps a user has granted access to)
// and the Admin Reports audit log (actual login events for OAuth and Google Workspace apps).
// Results are stored in the session so applicationResource.List() and Grants() can read them
// without re-fetching across sync phases.
package connector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	admin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/googleapi"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

const (
	reportsUserAll  = "all"
	reportsAppLogin = "login"
	reportsAppSAML  = "saml"

	samlAppIDPrefix               = "saml:"
	googleWorkspaceAppID          = "google_workspace"
	googleWorkspaceAppDisplayName = "Google Workspace"
	// appLoginLookbackDays limits how far back login events are scanned.
	// Since events are returned newest-first, this bounds the total pages fetched.
	appLoginLookbackDays = 180
	// appLoginMaxPages caps pagination for SAML events (OAuth is per-app, no pagination needed).
	appLoginMaxPages    = 20
	appDiscoveryWorkers = 10
	// appDiscoveryMaxUserPages caps user pages scanned during OAuth app discovery.
	appDiscoveryMaxUserPages = 200
	// oauthPresenceValue is the sentinel stored when a user has authorized an OAuth app via
	// Tokens.List() but no Reports timestamp is available. Epoch ensures any real timestamp
	// from the Reports API takes precedence.
	oauthPresenceValue = "1970-01-01T00:00:00Z"
)

var (
	appLoginAppNamespace           = sessions.WithPrefix("app_login_app")
	appLoginOAuthAppsNamespace     = sessions.WithPrefix("app_login_oauth_apps")
	appDiscoveryLoadedNamespace    = sessions.WithPrefix("app_login_tokens_loaded")
	appLoginDataLoadedNamespace    = sessions.WithPrefix("app_login_data_loaded")
	appLoginDirectoryUserNamespace = sessions.WithPrefix("app_login_directory_user")
	samlProfileMapNamespace        = sessions.WithPrefix("saml_profile_map")
	samlProfileMapLoadedNamespace  = sessions.WithPrefix("saml_profile_map_loaded")
)

func appLoginLoginsNamespace(appID string) sessions.SessionStoreOption {
	return sessions.WithPrefix("app_login_logins:" + appID)
}

// loadLoginEvents fetches Google sign-in and SAML login events from the Reports API,
// stores per-user last-login timestamps in session for use by Grants(), and returns
// the discovered SAML apps (appID → displayName). Runs only once per sync.
func loadLoginEvents(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, samlProfileMap map[string]string) (map[string]string, error) {
	_, loaded, err := session.GetJSON[string](ctx, ss, "done", appLoginDataLoadedNamespace)
	if err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to check app login data loaded flag: %w", err)
	}
	if loaded {
		return session.GetAllJSON[string](ctx, ss, appLoginAppNamespace)
	}

	startTime := time.Now().UTC().AddDate(0, 0, -appLoginLookbackDays).Format(time.RFC3339)

	if err := loadGoogleLogins(ctx, ss, client, startTime); err != nil {
		return nil, err
	}
	if err := loadSAMLLogins(ctx, ss, client, startTime, samlProfileMap); err != nil {
		return nil, err
	}

	if err := session.SetJSON(ctx, ss, "done", "true", appLoginDataLoadedNamespace); err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to mark app login data as loaded: %w", err)
	}
	return session.GetAllJSON[string](ctx, ss, appLoginAppNamespace)
}

func loadGoogleLogins(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, startTime string) error {
	l := ctxzap.Extract(ctx)
	var pageToken string
	var lastResp *reportsAdmin.Activities
	for range appLoginMaxPages {
		resp, err := client.ListActivities(ctx, reportsUserAll, reportsAppLogin, "login_success", startTime, "", pageToken, 1000)
		if err != nil {
			return fmt.Errorf("google-workspace-connector: failed to fetch google login activity: %w", err)
		}
		lastResp = resp

		for _, activity := range resp.Items {
			ts := convertIdTimeToTimestamp(activity.Id.Time)
			if ts == nil || activity.Actor.ProfileId == "" {
				continue
			}
			profileID := activity.Actor.ProfileId
			_, isDir, err := session.GetJSON[string](ctx, ss, profileID, appLoginDirectoryUserNamespace)
			if err != nil {
				return fmt.Errorf("google-workspace-connector: failed to check directory user %s: %w", profileID, err)
			}
			if !isDir {
				l.Debug("google-workspace: skipping non-directory user in google login events", zap.String("profile_id", profileID))
				continue
			}
			loginTime := ts.AsTime().UTC().Format(time.RFC3339)
			existing, found, err := session.GetJSON[string](ctx, ss, profileID, appLoginLoginsNamespace(googleWorkspaceAppID))
			if err != nil {
				return fmt.Errorf("google-workspace-connector: failed to read google login from session for %s: %w", profileID, err)
			}
			if found && existing >= loginTime {
				continue
			}
			if err := session.SetJSON(ctx, ss, profileID, loginTime, appLoginLoginsNamespace(googleWorkspaceAppID)); err != nil {
				return fmt.Errorf("google-workspace-connector: failed to store google login in session for %s: %w", profileID, err)
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	if lastResp != nil && lastResp.NextPageToken != "" {
		l.Debug("google-workspace: google login pagination cap reached, data may be incomplete", zap.Int("max_pages", appLoginMaxPages))
	}
	return nil
}

func loadSAMLLogins(ctx context.Context, ss sessions.SessionStore, client *gwclient.GoogleWorkspaceClient, startTime string, profileMap map[string]string) error {
	l := ctxzap.Extract(ctx)
	var pageToken string
	for range appLoginMaxPages {
		resp, err := client.ListActivities(ctx, reportsUserAll, reportsAppSAML, "login_success", startTime, "", pageToken, 1000)
		if err != nil {
			return fmt.Errorf("google-workspace-connector: failed to fetch saml activity: %w", err)
		}

		newApps, newPairs, err := processSAMLPage(ctx, ss, resp.Items, profileMap)
		if err != nil {
			return err
		}

		if len(newApps) > 0 {
			if err := session.SetManyJSON(ctx, ss, newApps, appLoginAppNamespace); err != nil {
				return fmt.Errorf("google-workspace-connector: failed to store saml apps in session: %w", err)
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		// Stop early: events are newest-first, so if this page had no new data, subsequent pages won't either.
		if newPairs == 0 && len(newApps) == 0 {
			l.Debug("google-workspace: no new saml apps or login pairs on page, stopping early")
			break
		}
		pageToken = resp.NextPageToken
	}
	return nil
}

// processSAMLPage processes a page of SAML activities, writes per-(app,user) login timestamps to
// session, and returns newly discovered apps and the count of new (app, user) pairs written.
func processSAMLPage(ctx context.Context, ss sessions.SessionStore, activities []*reportsAdmin.Activity, profileMap map[string]string) (map[string]string, int, error) {
	newApps := map[string]string{}
	newPairs := 0
	for _, activity := range activities {
		ts := convertIdTimeToTimestamp(activity.Id.Time)
		if ts == nil || activity.Actor.ProfileId == "" {
			continue
		}
		profileID := activity.Actor.ProfileId
		loginTime := ts.AsTime().UTC().Format(time.RFC3339)

		for _, ev := range activity.Events {
			appName := getValueFromParameters("application_name", ev.Parameters)
			if appName == "" {
				continue
			}
			stableID := appName
			if profileName, ok := profileMap[appName]; ok {
				stableID = profileName
			}
			appID := samlAppIDPrefix + stableID
			if _, seen := newApps[appID]; !seen {
				newApps[appID] = appName
			}

			existing, found, err := session.GetJSON[string](ctx, ss, profileID, appLoginLoginsNamespace(appID))
			if err != nil {
				return nil, 0, fmt.Errorf("google-workspace-connector: failed to read login from session for app %s user %s: %w", appID, profileID, err)
			}
			if found && existing >= loginTime {
				continue
			}
			newPairs++
			if err := session.SetJSON(ctx, ss, profileID, loginTime, appLoginLoginsNamespace(appID)); err != nil {
				return nil, 0, fmt.Errorf("google-workspace-connector: failed to store login data for app %s user %s: %w", appID, profileID, err)
			}
		}
	}
	return newApps, newPairs, nil
}

type oauthAppEntry struct {
	clientID    string
	displayText string
}

type userAppsResult struct {
	userID string
	apps   []oauthAppEntry
}

// discoverOAuthApps lists OAuth tokens for all users and stores user+app associations in session.
// Token fetching is parallelized with a bounded worker pool. Uses a run-once session flag.
func discoverOAuthApps(
	ctx context.Context,
	ss sessions.SessionStore,
	client *gwclient.GoogleWorkspaceClient,
	customerID string,
	domain string,
) (map[string]string, error) {
	_, loaded, err := session.GetJSON[string](ctx, ss, "done", appDiscoveryLoadedNamespace)
	if err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to check oauth app discovery loaded flag: %w", err)
	}
	if loaded {
		apps, err := session.GetAllJSON[string](ctx, ss, appLoginOAuthAppsNamespace)
		if err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to read oauth apps from session: %w", err)
		}
		return apps, nil
	}

	l := ctxzap.Extract(ctx)
	sem := semaphore.NewWeighted(appDiscoveryWorkers)
	var nextPageToken string

	for range appDiscoveryMaxUserPages {
		userResp, err := client.ListUserIDsPage(ctx, customerID, domain, nextPageToken)
		if err != nil {
			return nil, fmt.Errorf("google-workspace-connector: failed to list users for applications: %w", err)
		}

		dirUserBatch := make(map[string]string, len(userResp.Users))
		for _, u := range userResp.Users {
			if u.Id != "" {
				dirUserBatch[u.Id] = "1"
			}
		}
		if len(dirUserBatch) > 0 {
			if err := session.SetManyJSON(ctx, ss, dirUserBatch, appLoginDirectoryUserNamespace); err != nil {
				return nil, fmt.Errorf("google-workspace-connector: failed to store directory user IDs in session: %w", err)
			}
		}

		results, err := fetchUserTokens(ctx, sem, client, userResp.Users)
		if err != nil {
			return nil, err
		}

		if err := storeOAuthPageResults(ctx, ss, results); err != nil {
			return nil, err
		}

		if userResp.NextPageToken == "" {
			break
		}
		nextPageToken = userResp.NextPageToken
	}
	if nextPageToken != "" {
		l.Debug("google-workspace: user pagination cap reached during OAuth app discovery, data may be incomplete",
			zap.Int("max_pages", appDiscoveryMaxUserPages))
	}

	if err := session.SetJSON(ctx, ss, "done", "true", appDiscoveryLoadedNamespace); err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to mark app discovery as loaded: %w", err)
	}

	apps, err := session.GetAllJSON[string](ctx, ss, appLoginOAuthAppsNamespace)
	if err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to read oauth apps from session: %w", err)
	}
	return apps, nil
}

// fetchUserTokens concurrently fetches OAuth tokens for each user using a bounded worker pool.
//
// A failed ListTokens call is only tolerated (the user is skipped) when it is a genuine 404 —
// i.e. the user was deleted mid-sync. Any other failure (403/429/5xx/network/context) is
// surfaced as an error that aborts discovery. Silently skipping a user on a transient error
// would drop that user's OAuth-app associations: apps only that user authorized may then be
// missed entirely, and the user's app-access grants would be under-reported — which c1 prunes
// as a revocation. Failing loudly forces a retry instead of persisting a partial result.
func fetchUserTokens(ctx context.Context, sem *semaphore.Weighted, client *gwclient.GoogleWorkspaceClient, users []*admin.User) ([]userAppsResult, error) {
	results := make([]userAppsResult, len(users))
	var wg sync.WaitGroup
	var (
		errMu    sync.Mutex
		firstErr error
	)
	recordErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	for i, u := range users {
		if err := sem.Acquire(ctx, 1); err != nil {
			wg.Wait()
			return nil, fmt.Errorf("google-workspace-connector: context cancelled during token fetch: %w", err)
		}
		wg.Add(1)
		go func(idx int, user *admin.User) {
			defer wg.Done()
			defer sem.Release(1)
			defer func() {
				if r := recover(); r != nil {
					ctxzap.Extract(ctx).Error("google-workspace-connector: fetchUserTokens goroutine recovered from panic",
						zap.String("user_id", user.Id),
						zap.Any("panic", r),
						zap.Stack("stack"),
					)
				}
			}()

			tokenResp, err := client.ListTokens(ctx, user.Id)
			if err != nil {
				var gerr *googleapi.Error
				if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
					// Benign: the user was deleted between listing and token fetch. Skipping
					// them is correct — they have no apps to associate.
					ctxzap.Extract(ctx).Debug("google-workspace-connector: user not found during token fetch, skipping", zap.String("user_id", user.Id), zap.Error(err))
					return
				}
				// Transient/auth/network error: surface it so discovery aborts rather than
				// persisting a partial OAuth-app map that c1 would treat as a revocation.
				recordErr(fmt.Errorf("google-workspace-connector: failed to list tokens for user %s: %w", user.Id, err))
				return
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
			results[idx] = userAppsResult{userID: user.Id, apps: filtered}
		}(i, u)
	}
	wg.Wait()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func storeOAuthPageResults(ctx context.Context, ss sessions.SessionStore, results []userAppsResult) error {
	appsBatch := map[string]string{}
	loginBatches := map[string]map[string]string{}

	for _, r := range results {
		for _, t := range r.apps {
			appsBatch[t.clientID] = t.displayText
			if loginBatches[t.clientID] == nil {
				loginBatches[t.clientID] = map[string]string{}
			}
			loginBatches[t.clientID][r.userID] = oauthPresenceValue
		}
	}

	if len(appsBatch) > 0 {
		if err := session.SetManyJSON(ctx, ss, appsBatch, appLoginOAuthAppsNamespace); err != nil {
			return fmt.Errorf("google-workspace-connector: failed to store oauth apps in session: %w", err)
		}
	}

	for appID, logins := range loginBatches {
		ns := appLoginLoginsNamespace(appID)
		existing, err := session.GetManyJSON[string](ctx, ss, slices.Collect(maps.Keys(logins)), ns)
		if err != nil {
			return fmt.Errorf("google-workspace-connector: failed to read existing logins from session for app %s: %w", appID, err)
		}
		newLogins := map[string]string{}
		for profileID, val := range logins {
			if existingVal, found := existing[profileID]; found && existingVal >= val {
				continue
			}
			newLogins[profileID] = val
		}
		if len(newLogins) > 0 {
			if err := session.SetManyJSON(ctx, ss, newLogins, ns); err != nil {
				return fmt.Errorf("google-workspace-connector: failed to store login data for app %s: %w", appID, err)
			}
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

// loadSAMLProfileMap returns the SAML profile map, using the session store as a cache
// so Cloud Identity is queried at most once per sync.
func loadSAMLProfileMap(ctx context.Context, client *gwclient.GoogleWorkspaceClient, customerID string) (map[string]string, error) {
	ss, _ := ctx.Value(sessions.SessionStoreKey{}).(sessions.SessionStore)
	if ss == nil {
		return fetchSAMLProfileMap(ctx, client, customerID), nil
	}

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
