package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/api/googleapi"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

const applicationAccessEntitlement = "access"

// errorReasonAccessNotConfigured is the legacy googleapi.ErrorItem.Reason value
// for a disabled API (extracted to satisfy goconst).
const errorReasonAccessNotConfigured = "accessNotConfigured"

type applicationResource struct {
	client     *gwclient.GoogleWorkspaceClient
	customerID string
	domain     string
}

func newApplicationResource(client *gwclient.GoogleWorkspaceClient, customerID, domain string) *applicationResource {
	return &applicationResource{
		client:     client,
		customerID: customerID,
		domain:     domain,
	}
}

func (ar *applicationResource) ResourceType(_ context.Context) *v2.ResourceType {
	return resourceTypeEnterpriseApplication
}

func (ar *applicationResource) List(ctx context.Context, _ *v2.ResourceId, attrs rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	isFirstPage := attrs.PageToken.Token == ""

	var samlProfileMap map[string]string
	if ar.client.CloudIdentityService != nil {
		// The Cloud Identity service is only non-nil when the inboundsso.readonly scope was
		// granted and service init succeeded, so a failure here is a transient/real API error,
		// not a missing-scope condition. Do NOT swallow it: falling back to a nil profile map
		// drops SAML apps discovered only via Cloud Identity AND flips the IDs of SAML apps
		// found via login events from their stable profile name to a display-name-derived ID.
		// A previously-synced SAML app would then change resource ID and c1 would prune the old
		// resource and all of its access grants — a silent false-revocation on a transient blip.
		// (When the scope is NOT granted, CloudIdentityService is nil and we consistently use
		// display-name IDs every sync, so no ID flip occurs.)
		//
		// This is only fetched on the first page and cached in session for subsequent pages —
		// each applicationResource.List() call now covers one bounded batch of users, not the
		// whole directory, so re-fetching every call would be wasteful and would re-run the
		// error-handling below on every page.
		var err error
		samlProfileMap, err = loadCloudIdentitySAMLProfileMap(ctx, attrs.Session, ar.client, ar.customerID, isFirstPage)
		if err != nil {
			return nil, nil, err
		}
	}

	newOAuthApps, newSAMLApps, nextPageToken, err := scanAppLoginsPage(ctx, attrs.Session, ar.client, ar.customerID, ar.domain, attrs.PageToken.Token, samlProfileMap)
	if err != nil {
		return nil, nil, err
	}

	if samlProfileMap != nil {
		for appID, name := range discoverSAMLApps(samlProfileMap) {
			if _, exists := newSAMLApps[appID]; !exists {
				newSAMLApps[appID] = name
			}
		}
	}

	resources := make([]*v2.Resource, 0, len(newOAuthApps)+len(newSAMLApps)+1)
	// emittedThisCall accumulates emitted-app markers locally and is only persisted once, right
	// before the successful return below. Writing markers per-app mid-loop (as before) let an
	// error on a later app in the same call abort List() with `resources` discarded by the SDK,
	// while earlier apps' markers were already committed — permanently skipping those apps on
	// the SDK's retry with the same page token, since they'd read back as already emitted.
	emittedThisCall := make(map[string]string)

	for appID, displayName := range newOAuthApps {
		if _, isSAML := newSAMLApps[appID]; isSAML {
			continue
		}
		alreadyEmitted, err := isAppEmitted(ctx, attrs.Session, appID)
		if err != nil {
			return nil, nil, err
		}
		if alreadyEmitted {
			continue
		}
		r, err := rs.NewAppResource(displayName, resourceTypeEnterpriseApplication, appID, nil,
			rs.WithNHIType(v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "gws.oauth_app"))
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", appID, err)
		}
		resources = append(resources, r)
		emittedThisCall[appID] = "1"
	}

	for appID, displayName := range newSAMLApps {
		alreadyEmitted, err := isAppEmitted(ctx, attrs.Session, appID)
		if err != nil {
			return nil, nil, err
		}
		if alreadyEmitted {
			continue
		}
		r, err := rs.NewAppResource(displayName, resourceTypeEnterpriseApplication, appID, nil,
			rs.WithNHIType(v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "gws.saml_app"))
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", appID, err)
		}
		resources = append(resources, r)
		emittedThisCall[appID] = "1"
	}

	if nextPageToken == "" {
		// Final page of this pass: Google Workspace itself is always an app — sign-in events
		// from googleLoginEventFeed target this resource. Emit it once, only here, so it is
		// never returned more than once across pages.
		alreadyEmitted, err := isAppEmitted(ctx, attrs.Session, googleWorkspaceAppID)
		if err != nil {
			return nil, nil, err
		}
		if !alreadyEmitted {
			r, err := rs.NewAppResource(googleWorkspaceAppDisplayName, resourceTypeEnterpriseApplication, googleWorkspaceAppID, nil,
				rs.WithNHIType(v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "gws.workspace"))
			if err != nil {
				return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", googleWorkspaceAppID, err)
			}
			resources = append(resources, r)
			emittedThisCall[googleWorkspaceAppID] = "1"
		}
	}

	if len(emittedThisCall) > 0 {
		if err := session.SetManyJSON(ctx, attrs.Session, emittedThisCall, appLoginEmittedAppNamespace); err != nil {
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to store emitted-app markers: %w", err)
		}
	}

	return resources, &rs.SyncOpResults{NextPageToken: nextPageToken}, nil
}

func (ar *applicationResource) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return []*v2.Entitlement{
		entitlement.NewAssignmentEntitlement(
			resource,
			applicationAccessEntitlement,
			entitlement.WithDisplayName("Has Access"),
			entitlement.WithDescription("User has logged in to this application"),
			entitlement.WithAnnotation(&v2.EntitlementImmutable{}),
			entitlement.WithGrantableTo(resourceTypeUser),
		),
	}, &rs.SyncOpResults{}, nil
}

func (ar *applicationResource) Grants(ctx context.Context, resource *v2.Resource, attrs rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	appID := resource.Id.Resource

	userLogins, err := session.GetAllJSON[string](ctx, attrs.Session, appLoginLoginsNamespace(appID))
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace-connector: failed to read app logins from session: %w", err)
	}
	if len(userLogins) == 0 {
		return nil, &rs.SyncOpResults{}, nil
	}

	directoryUsers, err := session.GetAllJSON[string](ctx, attrs.Session, appLoginDirectoryUserNamespace)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace-connector: failed to read directory users from session: %w", err)
	}
	if len(directoryUsers) == 0 {
		return nil, &rs.SyncOpResults{}, nil
	}

	grants := make([]*v2.Grant, 0, len(userLogins))
	for profileID := range userLogins {
		if _, isDirectoryUser := directoryUsers[profileID]; !isDirectoryUser {
			continue
		}
		principal := &v2.ResourceId{
			Resource:     profileID,
			ResourceType: resourceTypeUser.Id,
		}

		g := grant.NewGrant(resource, applicationAccessEntitlement, principal)
		grants = append(grants, g)
	}

	return grants, &rs.SyncOpResults{}, nil
}

// isCloudIdentityAPIDisabledError reports whether err is Google's permanent
// "this API is not enabled for the project" failure: HTTP 403 with reason
// SERVICE_DISABLED (structured google.rpc.ErrorInfo) or accessNotConfigured
// (legacy error item). This is distinct from a transient 403/5xx or a network
// error — it is a stable customer-configuration condition (the Cloud Identity
// API has never been enabled), so it is safe to treat like a missing scope.
func isCloudIdentityAPIDisabledError(err error) bool {
	var ge *googleapi.Error
	if !errors.As(err, &ge) {
		return false
	}
	if ge.Code != http.StatusForbidden {
		return false
	}
	// Covers both the legacy error item (e.g. {"reason": "accessNotConfigured"})
	// and the structured google.rpc.ErrorInfo{reason: "SERVICE_DISABLED"} shape.
	for _, reason := range gwclient.GoogleAPIErrorReasons(ge) {
		if reason == errorReasonAccessNotConfigured || reason == "SERVICE_DISABLED" {
			return true
		}
	}
	return false
}
