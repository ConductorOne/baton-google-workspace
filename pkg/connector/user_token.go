package connector

import (
	"context"
	"fmt"
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

func (u userTokenResource) ResourceType(ctx context.Context) *v2.ResourceType {
	return resourceTypeUserToken
}

func (u userTokenResource) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
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
		rs, err := sdkResource.NewResource(
			token.DisplayText,
			resourceTypeUserToken,
			fmt.Sprintf("%s/%s", token.UserKey, token.ClientId),
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

func (u userTokenResource) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (u userTokenResource) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}
