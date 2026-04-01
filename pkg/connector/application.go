package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

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
	l := ctxzap.Extract(ctx)

	var samlProfileMap map[string]string
	if ar.client.CloudIdentityService != nil {
		var err error
		samlProfileMap, err = ar.client.BuildSAMLProfileMap(ctx, ar.customerID)
		if err != nil {
			l.Info("google-workspace: failed to load SAML profiles from Cloud Identity; SAML app IDs will use display names. "+
				"Grant the 'https://www.googleapis.com/auth/cloud-identity.inboundsso.readonly' scope to fix this.", zap.Error(err))
		}
	}

	apps, err := discoverOAuthApps(ctx, attrs.Session, ar.client, ar.customerID, ar.domain)
	if err != nil {
		return nil, nil, err
	}

	samlApps, err := loadLoginEvents(ctx, attrs.Session, ar.client, samlProfileMap)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace-connector: failed to load login events: %w", err)
	}
	for appID, name := range samlApps {
		apps[appID] = name
	}

	if samlProfileMap != nil {
		for appID, name := range discoverSAMLApps(samlProfileMap) {
			if _, exists := apps[appID]; !exists {
				apps[appID] = name
			}
		}
	}

	// Google Workspace itself is always an app — sign-in events from googleLoginEventFeed target this resource.
	apps[googleWorkspaceAppID] = googleWorkspaceAppDisplayName

	resources := make([]*v2.Resource, 0, len(apps))
	for appID, displayName := range apps {
		r, err := rs.NewAppResource(displayName, resourceTypeEnterpriseApplication, appID, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace-connector: failed to create application resource %s: %w", appID, err)
		}
		resources = append(resources, r)
	}

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

func (ar *applicationResource) Grant(_ context.Context, _ *v2.Resource, _ *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	return nil, nil, nil
}

func (ar *applicationResource) Revoke(_ context.Context, _ *v2.Grant) (annotations.Annotations, error) {
	return nil, nil
}
