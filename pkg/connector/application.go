package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"

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
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to load SAML profiles from Cloud Identity: %w", err)
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
