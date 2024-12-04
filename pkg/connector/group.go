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
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/api/googleapi"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
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
}

func (o *groupResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return o.resourceType
}

func (o *groupResourceType) List(ctx context.Context, resourceId *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag := &pagination.Bag{}
	err := bag.Unmarshal(pt.Token)
	if err != nil {
		return nil, "", nil, err
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
		return nil, "", nil, err
	}

	rv := make([]*v2.Resource, 0, len(groups.Groups))
	for _, g := range groups.Groups {
		if g.Id == "" {
			l.Error("google-workspace: group had no id", zap.String("name", g.Name))
			continue
		}
		annos := &v2.V1Identifier{
			Id: g.Id,
		}
		traitOpts := []sdkResource.GroupTraitOption{sdkResource.WithGroupProfile(groupProfile(ctx, g))}
		groupResource, err := sdkResource.NewGroupResource(g.Name, resourceTypeGroup, g.Id, traitOpts, sdkResource.WithAnnotation(annos))
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, groupResource)
	}
	nextPage, err := bag.NextToken(groups.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}
	return rv, nextPage, nil, nil
}

func (o *groupResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var annos annotations.Annotations
	annos.Update(&v2.V1Identifier{
		Id: V1MembershipEntitlementID(resource.Id.Resource),
	})
	member := sdkEntitlement.NewAssignmentEntitlement(resource, groupMemberEntitlement, sdkEntitlement.WithGrantableTo(resourceTypeUser))
	member.Description = fmt.Sprintf("Is member of the %s group in Google Workspace", resource.DisplayName)
	member.Annotations = annos
	member.DisplayName = fmt.Sprintf("%s Group Member", resource.DisplayName)
	return []*v2.Entitlement{member}, "", nil, nil
}

func (o *groupResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag := &pagination.Bag{}
	err := bag.Unmarshal(pt.Token)
	if err != nil {
		return nil, "", nil, err
	}

	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: resource.Id.ResourceType,
			ResourceID:     resource.Id.Resource,
		})
	}

	r := o.groupMemberService.Members.List(resource.Id.Resource).MaxResults(100)
	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	members, err := r.Context(ctx).Do()
	if err != nil {
		return nil, "", nil, err
	}
	var rv []*v2.Grant
	for _, member := range members.Members {
		v1Identifier := &v2.V1Identifier{
			Id: V1GrantID(V1MembershipEntitlementID(resource.Id.Resource), member.Id),
		}
		gmID, err := sdkResource.NewResourceID(resourceTypeUser, member.Id)
		if err != nil {
			return nil, "", nil, err
		}
		grant := sdkGrant.NewGrant(resource, groupMemberEntitlement, gmID, sdkGrant.WithAnnotation(v1Identifier))
		rv = append(rv, grant)
	}

	nextPage, err := bag.NextToken(members.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func groupBuilder(
	groupService *admin.Service,
	customerId string,
	domain string,
	groupMemberService *admin.Service,
	groupMemberProvisioningService *admin.Service,
) *groupResourceType {
	return &groupResourceType{
		resourceType:                   resourceTypeGroup,
		groupService:                   groupService,
		customerId:                     customerId,
		domain:                         domain,
		groupMemberService:             groupMemberService,
		groupMemberProvisioningService: groupMemberProvisioningService,
	}
}

func groupProfile(ctx context.Context, group *admin.Group) map[string]interface{} {
	profile := make(map[string]interface{})
	profile["group_id"] = group.Id
	profile["group_name"] = group.Name
	profile["group_email"] = group.Email
	return profile
}

func (o *groupResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if o.groupMemberProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace-v2: unable to get service for scope %s", directoryAdmin.AdminDirectoryGroupMemberScope)
	}
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, nil, errors.New("google-workspace-v2: user principal is required")
	}

	r := o.groupMemberProvisioningService.Members.Insert(entitlement.Resource.Id.Resource, &admin.Member{Id: principal.GetId().GetResource()})
	assignment, err := r.Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusConflict {
				assignment, err = o.groupService.Members.Get(entitlement.Resource.Id.Resource, principal.GetId().GetResource()).Context(ctx).Do()
				if err != nil {
					return nil, nil, err
				}
			} else {
				return nil, nil, fmt.Errorf("google-workspace-v2: failed to insert group member: %w", err)
			}
		} else {
			return nil, nil, fmt.Errorf("google-workspace-v2: failed to insert group member: %w", err)
		}
	}

	grant := sdkGrant.NewGrant(entitlement.Resource, roleMemberEntitlement, principal.GetId())
	grant.Id = assignment.Id
	return []*v2.Grant{grant}, nil, nil
}

func (o *groupResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	if o.groupMemberProvisioningService == nil {
		return nil, fmt.Errorf("google-workspace-v2: unable to get service for scope %s", directoryAdmin.AdminDirectoryGroupMemberScope)
	}
	if grant.Principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, errors.New("google-workspace-v2: user principal is required")
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
		return nil, fmt.Errorf("google-workspace-v2: failed to remove group member: %w", err)
	}

	return nil, nil
}
