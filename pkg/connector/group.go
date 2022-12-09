package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
)

const (
	groupMemberEntitlement = "member"
)

type groupResourceType struct {
	resourceType       *v2.ResourceType
	groupService       *admin.Service
	customerId         string
	domain             string
	groupMemberService *admin.Service
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
	r := o.groupService.Groups.List().Domain(o.domain).MaxResults(100)

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
		profile := groupProfile(ctx, g)
		groupResource, err := sdk.NewGroupResource(g.Name, resourceTypeGroup, nil, g.Id, profile, annos)
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
	member := sdk.NewAssignmentEntitlement(resource, groupMemberEntitlement, resourceTypeUser)
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
		gmID, err := sdk.NewResourceID(resourceTypeUser, member.Id)
		if err != nil {
			return nil, "", nil, err
		}
		grant := sdk.NewGrant(resource, groupMemberEntitlement, gmID)
		annos := annotations.Annotations(grant.Annotations)
		annos.Update(v1Identifier)
		grant.Annotations = annos
		rv = append(rv, grant)
	}

	nextPage, err := bag.NextToken(members.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func groupBuilder(groupService *admin.Service, customerId string, domain string, groupMemberService *admin.Service) *groupResourceType {
	return &groupResourceType{
		resourceType:       resourceTypeGroup,
		groupService:       groupService,
		customerId:         customerId,
		domain:             domain,
		groupMemberService: groupMemberService,
	}
}

func groupProfile(ctx context.Context, group *admin.Group) map[string]interface{} {
	profile := make(map[string]interface{})
	profile["group_id"] = group.Id
	profile["group_name"] = group.Name
	profile["group_email"] = group.Email
	return profile
}
