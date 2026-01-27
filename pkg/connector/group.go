package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"

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
	resourceType                   *v2.ResourceType
	groupService                   *admin.Service
	customerId                     string
	domain                         string
	groupMemberService             *admin.Service
	groupMemberProvisioningService *admin.Service
	groupProvisioningService       *admin.Service
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
	r := o.groupService.Groups.List()

	if o.domain != "" {
		r = r.Domain(o.domain)
	} else {
		r = r.Customer(o.customerId)
	}

	// https://developers.google.com/admin-sdk/directory/v1/limits
	// Groups and group members – A default and maximum of 200 entries per page.
	r = r.MaxResults(200)

	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	groups, err := r.Context(ctx).Do()
	if err != nil {
		return nil, nil, wrapGoogleApiErrorWithContext(err, "failed to list groups")
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
	member := sdkEntitlement.NewAssignmentEntitlement(resource, groupMemberEntitlement, sdkEntitlement.WithGrantableTo(resourceTypeUser))
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

	r := o.groupMemberService.Members.List(resource.Id.Resource).MaxResults(200)
	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	members, err := r.Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			// Return no grants if the group no longer exists. This might happen if the group is deleted during a sync.
			if gerr.Code == http.StatusNotFound {
				return nil, nil, uhttp.WrapErrors(codes.NotFound, fmt.Sprintf("no group found with id %s", resource.Id.Resource))
			}
		}
		return nil, nil, wrapGoogleApiErrorWithContext(err, "failed to list group members")
	}

	var rv []*v2.Grant
	for _, member := range members.Members {
		v1Identifier := &v2.V1Identifier{
			Id: V1GrantID(V1MembershipEntitlementID(resource.Id.Resource), member.Id),
		}
		gmID, err := rs.NewResourceID(resourceTypeUser, member.Id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create user resource ID in group Grants: %w", err)
		}
		grant := sdkGrant.NewGrant(resource, groupMemberEntitlement, gmID, sdkGrant.WithAnnotation(v1Identifier))
		rv = append(rv, grant)
	}

	nextPage, err := bag.NextToken(members.NextPageToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate next page token in group Grants: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func groupBuilder(
	groupService *admin.Service,
	customerId string,
	domain string,
	groupMemberService *admin.Service,
	groupMemberProvisioningService *admin.Service,
	groupProvisioningService *admin.Service,
) *groupResourceType {
	return &groupResourceType{
		resourceType:                   resourceTypeGroup,
		groupService:                   groupService,
		customerId:                     customerId,
		domain:                         domain,
		groupMemberService:             groupMemberService,
		groupMemberProvisioningService: groupMemberProvisioningService,
		groupProvisioningService:       groupProvisioningService,
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
	if o.groupMemberProvisioningService == nil {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("unable to get service for scope %s", admin.AdminDirectoryGroupMemberScope))
	}
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "user principal is required")
	}

	r := o.groupMemberProvisioningService.Members.Insert(entitlement.Resource.Id.Resource, &admin.Member{Id: principal.GetId().GetResource()})
	assignment, err := r.Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusConflict {
			// Member already exists, fetch it to return as grant (idempotency)
			assignment, err = o.groupMemberProvisioningService.Members.Get(entitlement.Resource.Id.Resource, principal.GetId().GetResource()).Context(ctx).Do()
			if err != nil {
				return nil, nil, wrapGoogleApiErrorWithContext(err, "failed to get existing group member")
			}
		} else {
			return nil, nil, wrapGoogleApiErrorWithContext(err, "failed to add group member")
		}
	}

	grant := sdkGrant.NewGrant(entitlement.Resource, roleMemberEntitlement, principal.GetId())
	grant.Id = assignment.Id
	return []*v2.Grant{grant}, nil, nil
}

func (o *groupResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	if o.groupMemberProvisioningService == nil {
		return nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("unable to get service for scope %s", admin.AdminDirectoryGroupMemberScope))
	}
	if grant.Principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, uhttp.WrapErrors(codes.InvalidArgument, "user principal is required")
	}
	l := ctxzap.Extract(ctx)

	r := o.groupMemberProvisioningService.Members.Delete(grant.Entitlement.Resource.Id.Resource, grant.Principal.GetId().GetResource())
	err := r.Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				// This should only hit if someone double-revokes, but I'd rather we log something about it
				l.Info("google-workspace-v2: group member is being deleted but doesn't exist",
					zap.String("group_id", grant.Entitlement.Resource.Id.Resource),
					zap.String("user_id", grant.Principal.GetId().GetResource()))
				return nil, nil
			}
		}
		return nil, wrapGoogleApiErrorWithContext(err, "failed to delete group member")
	}

	return nil, nil
}

func (o *groupResourceType) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	r := o.groupService.Groups.Get(resourceId.Resource)

	g, err := r.Context(ctx).Do()
	if err != nil {
		return nil, nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to retrieve group: %s", resourceId.Resource))
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
	if o.groupProvisioningService == nil {
		return nil, fmt.Errorf("google-workspace: group provisioning service not available - requires %s scope", admin.AdminDirectoryGroupScope)
	}
	if resourceId.ResourceType != resourceTypeGroup.Id {
		return nil, fmt.Errorf("google-workspace: resource type is not group")
	}

	err := o.groupProvisioningService.Groups.Delete(resourceId.Resource).Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				// Group already deleted, return success
				return nil, nil
			}
			if gerr.Code == http.StatusForbidden {
				return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf(
					"google-workspace: failed to delete group (403 Forbidden). "+
						"This may be due to: 1) missing OAuth scope %s, "+
						"2) insufficient admin permissions",
					admin.AdminDirectoryGroupScope))
			}
		}
		return nil, wrapGoogleApiErrorWithContext(err, "google-workspace: failed to delete group")
	}

	return nil, nil
}
