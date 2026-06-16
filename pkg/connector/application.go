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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

const applicationAccessEntitlement = "access"

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
		var err error
		samlProfileMap, err = ar.client.BuildSAMLProfileMap(ctx, ar.customerID)
		if err != nil {
			// handleSAMLProfileMapError swallows only the permanent "Cloud Identity API
			// disabled" 403 (falling back to display-name IDs); every other failure is
			// returned so the sync aborts rather than silently flip SAML app IDs and prune
			// the old apps + grants. samlProfileMap is already nil on error.
			if hErr := handleSAMLProfileMapError(ctx, err); hErr != nil {
				return nil, nil, hErr
			}
		}
	}

	oauthApps, err := discoverOAuthApps(ctx, attrs.Session, ar.client, ar.customerID, ar.domain)
	if err != nil {
		return nil, nil, err
	}

	samlApps, err := loadLoginEvents(ctx, attrs.Session, ar.client, samlProfileMap)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace-connector: failed to load login events: %w", err)
	}
	if samlProfileMap != nil {
		for appID, name := range discoverSAMLApps(samlProfileMap) {
			if _, exists := samlApps[appID]; !exists {
				samlApps[appID] = name
			}
		}
	}

	resources := make([]*v2.Resource, 0, len(oauthApps)+len(samlApps)+1)

	for appID, displayName := range oauthApps {
		if _, isSAML := samlApps[appID]; isSAML {
			continue
		}
		r, err := rs.NewAppResource(displayName, resourceTypeEnterpriseApplication, appID, nil,
			rs.WithNHIType(v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "gws.oauth_app"))
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", appID, err)
		}
		resources = append(resources, r)
	}

	for appID, displayName := range samlApps {
		r, err := rs.NewAppResource(displayName, resourceTypeEnterpriseApplication, appID, nil,
			rs.WithNHIType(v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "gws.saml_app"))
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", appID, err)
		}
		resources = append(resources, r)
	}

	// Google Workspace itself is always an app — sign-in events from googleLoginEventFeed target this resource.
	r, err := rs.NewAppResource(googleWorkspaceAppDisplayName, resourceTypeEnterpriseApplication, googleWorkspaceAppID, nil,
		rs.WithNHIType(v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "gws.workspace"))
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", googleWorkspaceAppID, err)
	}
	resources = append(resources, r)

	return resources, &rs.SyncOpResults{}, nil
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
	// Legacy error items (e.g. {"reason": "accessNotConfigured"}).
	for _, item := range ge.Errors {
		if item.Reason == "accessNotConfigured" {
			return true
		}
	}
	// Structured details carrying google.rpc.ErrorInfo{reason: "SERVICE_DISABLED"}.
	for _, detail := range ge.Details {
		if m, ok := detail.(map[string]interface{}); ok {
			if reason, _ := m["reason"].(string); reason == "SERVICE_DISABLED" {
				return true
			}
		}
	}
	return false
}

// handleSAMLProfileMapError classifies a BuildSAMLProfileMap failure for callers that build a
// SAML displayName → profile.Name map. A permanent "Cloud Identity API not enabled" 403 is a
// soft failure: it is logged and nil is returned so the caller falls back to display-name SAML
// IDs (safe — such a project has always used display-name IDs, so there is nothing to flip or
// prune). Every other failure (transient 5xx/429, network, or a non-disabled 403) is returned
// wrapped so the caller aborts rather than silently flip SAML app IDs and prune the old apps.
func handleSAMLProfileMapError(ctx context.Context, err error) error {
	if isCloudIdentityAPIDisabledError(err) {
		// Info (not Warn) to match the Debug-level missing-scope handling: a disabled API is
		// an expected, stable customer-config state, not an alert.
		ctxzap.Extract(ctx).Info("google-workspace: Cloud Identity API is not enabled for this project; "+
			"SAML app IDs will use display names. Enable the Cloud Identity API "+
			"(cloudidentity.googleapis.com) for this project to use stable SAML profile IDs.",
			zap.Error(err))
		return nil
	}
	return fmt.Errorf("google-workspace-connector: failed to load SAML profiles from Cloud Identity: %w", err)
}
