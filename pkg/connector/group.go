package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	sdkGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	uhttp "github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
)

const (
	groupMemberEntitlement = "member"
)

type groupResourceType struct {
	resourceType *v2.ResourceType
	client       *GoogleWorkspaceClient
	customerId   string
	domain       string
}

func (o *groupResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return o.resourceType
}

func (o *groupResourceType) List(ctx context.Context, resourceId *v2.ResourceId, attrs rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag := &pagination.Bag{}
	err := bag.Unmarshal(attrs.PageToken.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal pagination token in group List: %w", err)
	}
	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: resourceTypeGroup.Id,
		})
	}
	l := ctxzap.Extract(ctx)
	// https://developers.google.com/admin-sdk/directory/v1/limits
	// Groups and group members – A default and maximum of 200 entries per page.
	groups, err := o.client.ListGroups(ctx, o.customerId, o.domain, bag.PageToken())
	if err != nil {
		return nil, nil, err
	}

	serverResponse := groups.ServerResponse
	if len(groups.Groups) == 0 {
		l.Warn("no groups found", zap.Int("status", serverResponse.HTTPStatusCode),
			zap.Any("header", serverResponse.Header), zap.String("req", bag.PageToken()))
	} else {
		l.Debug("groups found", zap.Int("count", len(groups.Groups)),
			zap.Int("status", serverResponse.HTTPStatusCode),
			zap.Any("header", serverResponse.Header), zap.String("req", bag.PageToken()))
	}

	rv := make([]*v2.Resource, 0, len(groups.Groups))
	for _, g := range groups.Groups {
		if g.Id == "" {
			l.Error("group had no id", zap.String("name", g.Name))
			continue
		}
		groupResource, err := groupToResource(ctx, g)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create group resource in List: %w", err)
		}
		rv = append(rv, groupResource)
	}
	nextPage, err := bag.NextToken(groups.NextPageToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate next page token in group List: %w", err)
	}
	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func (o *groupResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	var annos annotations.Annotations
	annos.Update(&v2.V1Identifier{
		Id: V1MembershipEntitlementID(resource.Id.Resource),
	})
	member := sdkEntitlement.NewAssignmentEntitlement(resource, groupMemberEntitlement, sdkEntitlement.WithGrantableTo(resourceTypeUser, resourceTypeGroup))
	member.Description = fmt.Sprintf("Is member of the %s group in Google Workspace", resource.DisplayName)
	member.Annotations = annos
	member.DisplayName = fmt.Sprintf("%s Group Member", resource.DisplayName)
	return []*v2.Entitlement{member}, nil, nil
}

func (o *groupResourceType) Grants(ctx context.Context, resource *v2.Resource, attrs rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	bag := &pagination.Bag{}
	err := bag.Unmarshal(attrs.PageToken.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal pagination token in group Grants: %w", err)
	}

	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: resource.Id.ResourceType,
			ResourceID:     resource.Id.Resource,
		})
	}

	members, err := o.client.ListMembers(ctx, resource.Id.Resource, bag.PageToken())
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			// Return no grants if the group no longer exists. This might happen if the group is deleted during a sync.
			return nil, nil, uhttp.WrapErrors(codes.NotFound, fmt.Sprintf("no group found with id %s", resource.Id.Resource))
		}
		return nil, nil, err
	}

	var rv []*v2.Grant
	for _, member := range members.Members {
		opts := []sdkGrant.GrantOption{}
		v1Identifier := &v2.V1Identifier{
			Id: V1GrantID(V1MembershipEntitlementID(resource.Id.Resource), member.Id),
		}
		opts = append(opts, sdkGrant.WithAnnotation(v1Identifier))

		var gmID *v2.ResourceId
		if strings.EqualFold(member.Type, "group") {
			gmID, err = rs.NewResourceID(resourceTypeGroup, member.Id)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create group resource ID in group Grants: %w", err)
			}
			opts = append(opts, sdkGrant.WithAnnotation(&v2.GrantExpandable{
				EntitlementIds: []string{
					fmt.Sprintf("group:%s:member", member.Id),
				},
			}))
		} else {
			gmID, err = rs.NewResourceID(resourceTypeUser, member.Id)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create user resource ID in group Grants: %w", err)
			}
		}

		grant := sdkGrant.NewGrant(resource, groupMemberEntitlement, gmID, opts...)
		rv = append(rv, grant)
	}

	nextPage, err := bag.NextToken(members.NextPageToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate next page token in group Grants: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func groupBuilder(client *GoogleWorkspaceClient, customerId string, domain string) *groupResourceType {
	return &groupResourceType{
		resourceType: resourceTypeGroup,
		client:       client,
		customerId:   customerId,
		domain:       domain,
	}
}

func groupProfile(ctx context.Context, group *admin.Group) map[string]interface{} {
	profile := make(map[string]interface{})
	profile["group_id"] = group.Id
	profile["group_name"] = group.Name
	profile["group_email"] = group.Email
	return profile
}

// groupToResource converts an admin.Group to a v2.Resource.
func groupToResource(ctx context.Context, group *admin.Group) (*v2.Resource, error) {
	l := ctxzap.Extract(ctx)
	if group.Id == "" {
		l.Error("google-workspace: group has no id", zap.String("name", group.Name))
		return nil, fmt.Errorf("google-workspace: group has no id")
	}
	traitOpts := []rs.GroupTraitOption{rs.WithGroupProfile(groupProfile(ctx, group))}
	resourceOpts := []rs.ResourceOption{
		rs.WithAnnotation(&v2.V1Identifier{
			Id: group.Id,
		}),
		rs.WithAnnotation(&v2.RawId{
			Id: group.Id,
		}),
	}
	return rs.NewGroupResource(group.Name, resourceTypeGroup, group.Id, traitOpts, resourceOpts...)
}

func (o *groupResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if o.client.groupMemberProvisioningService == nil {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("unable to get service for scope %s", admin.AdminDirectoryGroupMemberScope))
	}
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "user principal is required")
	}

	assignment, err := o.client.InsertMember(ctx, entitlement.Resource.Id.Resource, &admin.Member{Id: principal.GetId().GetResource()})
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusConflict {
			// Member already exists, fetch it to return as grant (idempotency)
			assignment, err = o.client.GetMember(ctx, entitlement.Resource.Id.Resource, principal.GetId().GetResource())
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, err
		}
	}

	grant := sdkGrant.NewGrant(entitlement.Resource, roleMemberEntitlement, principal.GetId())
	grant.Id = assignment.Id
	return []*v2.Grant{grant}, nil, nil
}

func (o *groupResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	if o.client.groupMemberProvisioningService == nil {
		return nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("unable to get service for scope %s", admin.AdminDirectoryGroupMemberScope))
	}
	if grant.Principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, uhttp.WrapErrors(codes.InvalidArgument, "user principal is required")
	}
	l := ctxzap.Extract(ctx)

	err := o.client.DeleteMember(ctx, grant.Entitlement.Resource.Id.Resource, grant.Principal.GetId().GetResource())
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			// This should only hit if someone double-revokes, but I'd rather we log something about it
			l.Info("google-workspace-v2: group member is being deleted but doesn't exist",
				zap.String("group_id", grant.Entitlement.Resource.Id.Resource),
				zap.String("user_id", grant.Principal.GetId().GetResource()))
			return nil, nil
		}
		return nil, err
	}

	return nil, nil
}

func (o *groupResourceType) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	g, err := o.client.GetGroup(ctx, resourceId.Resource)
	if err != nil {
		return nil, nil, err
	}

	// TODO: If o.domainId is set, check if the group is still in the domain.
	//       There is not a straight forward way to do this when getting a single group.

	groupResource, err := groupToResource(ctx, g)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create group resource in Get: %w", err)
	}

	return groupResource, nil, nil
}

func (o *groupResourceType) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (annotations.Annotations, error) {
	if o.client.groupProvisioningService == nil {
		return nil, fmt.Errorf("google-workspace: group provisioning service not available - requires %s scope", admin.AdminDirectoryGroupScope)
	}
	if resourceId.ResourceType != resourceTypeGroup.Id {
		return nil, fmt.Errorf("google-workspace: resource type is not group")
	}

	err := o.client.DeleteGroup(ctx, resourceId.Resource)
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				// Group already deleted, return success
				return nil, nil
			}
			if gerr.Code == http.StatusForbidden {
				return nil, fmt.Errorf(
					"google-workspace: failed to delete group (403 Forbidden). "+
						"This may be due to: 1) missing OAuth scope %s, "+
						"2) insufficient admin permissions: %w",
					admin.AdminDirectoryGroupScope, err)
			}
		}
		return nil, err
	}

	return nil, nil
}
