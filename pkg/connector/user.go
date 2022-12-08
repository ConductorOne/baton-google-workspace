package connector

import (
	"context"
	"encoding/json"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
)

type userResourceType struct {
	resourceType *v2.ResourceType
	userService  *admin.Service
	customerId   string
	domain       string
}

func (o *userResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return o.resourceType
}

func (o *userResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	bag := &pagination.Bag{}
	err := bag.Unmarshal(pt.Token)
	if err != nil {
		return nil, "", nil, err
	}

	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: resourceTypeUser.Id,
		})
	}

	r := o.userService.Users.List().Domain(o.domain).MaxResults(100).
		OrderBy("email").Context(ctx)
	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	users, err := r.Context(ctx).Do()
	if err != nil {
		return nil, "", nil, err
	}

	rv := make([]*v2.Resource, 0, len(users.Users))
	for _, user := range users.Users {
		if user.Id == "" {
			l.Error("google-workspace: user had no id", zap.String("email", user.PrimaryEmail))
			continue
		}
		annos := &v2.V1Identifier{
			Id: user.Id,
		}
		profile := userProfile(ctx, user)
		userResource, err := sdk.NewUserResource(user.Name.FullName, resourceTypeUser, nil, user.Id, user.PrimaryEmail, profile, annos)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, userResource)
	}

	nextPage, err := bag.NextToken(users.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func (o *userResourceType) Entitlements(_ context.Context, _ *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (o *userResourceType) Grants(_ context.Context, _ *v2.Resource, _ *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func userBuilder(userService *admin.Service, customerId string, domain string) *userResourceType {
	return &userResourceType{
		resourceType: resourceTypeUser,
		userService:  userService,
		customerId:   customerId,
		domain:       domain,
	}
}

func userProfile(ctx context.Context, user *admin.User) map[string]interface{} {
	profile := make(map[string]interface{})
	if user.Name != nil {
		profile["given_name"] = user.Name.GivenName
		profile["family_name"] = user.Name.FamilyName
		profile["full_name"] = user.Name.FullName
		profile["icon"] = user.ThumbnailPhotoUrl
		profile["manager_email"] = extractManagerEmail(user)
	}
	return profile
}

func extractManagerEmail(u *admin.User) string {
	for _, rel := range extractRelations(u) {
		if rel.Type == "manager" {
			return rel.Value
		}
	}
	return ""
}

func extractRelations(u *admin.User) []*admin.UserRelation {
	if u.Relations == nil {
		return nil
	}

	data, err := json.Marshal(u.Relations)
	if err != nil {
		return nil
	}
	rv := make([]*admin.UserRelation, 0)
	err = json.Unmarshal(data, &rv)
	if err != nil {
		return nil
	}
	return rv
}
