package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
)

type userTokenResource struct {
	userService *admin.Service
}

func newUserTokenResource(userService *admin.Service) *userTokenResource {
	return &userTokenResource{userService: userService}
}

func (u *userTokenResource) ResourceType(ctx context.Context) *v2.ResourceType {
	return resourceTypeUserToken
}

func (u *userTokenResource) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if parentResourceID == nil {
		l.Info("Skipping user token resource type list, only supported as a child resource type")
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != resourceTypeUser.Id {
		return nil, "", nil, fmt.Errorf("invalid resource type: %s", parentResourceID.ResourceType)
	}

	userKey := parentResourceID.Resource

	doResponse, err := u.userService.Tokens.List(userKey).Context(ctx).Do()
	if err != nil {
		return nil, "", nil, err
	}

	rv := make([]*v2.Resource, 0, len(doResponse.Items))
	for _, token := range doResponse.Items {
		profile := map[string]any{
			"client_id":    token.ClientId,
			"display_text": token.DisplayText,
			"scopes":       strings.Join(token.Scopes, " "),
			"user_key":     token.UserKey,
		}

		opts := []sdkResource.AppTraitOption{
			sdkResource.WithAppProfile(profile),
		}

		rs, err := sdkResource.NewAppResource(
			token.DisplayText,
			resourceTypeUserToken,
			fmt.Sprintf("%s/%s", token.UserKey, token.ClientId),
			opts,
			sdkResource.WithParentResourceID(parentResourceID),
		)

		if err != nil {
			l.Error("Failed to create resource for user token", zap.Error(err))
			continue
		}

		rv = append(rv, rs)
	}

	return rv, "", nil, nil
}

func (u *userTokenResource) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{
		entitlement.NewAssignmentEntitlement(
			resource,
			"has",
			entitlement.WithDisplayName("Has Token"),
			entitlement.WithDescription("User has a token for an application"),
			entitlement.WithAnnotation(&v2.EntitlementImmutable{}),
			entitlement.WithGrantableTo(resourceTypeUser),
		),
	}, "", nil, nil
}

func (u *userTokenResource) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	idSplit := strings.Split(resource.Id.Resource, "/")
	if len(idSplit) != 2 {
		return nil, "", nil, fmt.Errorf("invalid resource id: %s", resource.Id.Resource)
	}

	userKey := idSplit[0]

	grants := []*v2.Grant{
		grant.NewGrant(resource, "has", &v2.ResourceId{
			Resource:     userKey,
			ResourceType: resourceTypeUser.Id,
		}),
	}

	return grants, "", nil, nil
}

func (u *userTokenResource) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	return nil, nil, fmt.Errorf("granting user tokens is not supported, only revoking")
}

func (u *userTokenResource) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	if grant.Principal.Id.ResourceType != resourceTypeUser.Id {
		return nil, fmt.Errorf("invalid grant type: %s", grant.Principal.Id.ResourceType)
	}

	idSplit := strings.Split(grant.Entitlement.Resource.Id.Resource, "/")
	if len(idSplit) != 2 {
		return nil, fmt.Errorf("invalid resource id: %s", grant.Entitlement.Resource.Id.Resource)
	}

	userKey := idSplit[0]
	clientID := idSplit[1]

	err := u.userService.Tokens.Delete(userKey, clientID).Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return nil, nil
}
