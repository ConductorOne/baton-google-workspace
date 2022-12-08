package connector

import (
	"context"
	"fmt"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
)

const (
	roleMemberEntitlement = "member"
)

type roleResourceType struct {
	resourceType *v2.ResourceType
	roleService  *admin.Service
	customerId   string
}

func (o *roleResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return o.resourceType
}

func (o *roleResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	bag := &pagination.Bag{}
	err := bag.Unmarshal(pt.Token)
	if err != nil {
		return nil, "", nil, err
	}
	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: resourceTypeRole.Id,
		})
	}
	r := o.roleService.Roles.List(o.customerId).MaxResults(100)

	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	roles, err := r.Context(ctx).Do()
	if err != nil {
		return nil, "", nil, err
	}

	rv := make([]*v2.Resource, 0, len(roles.Items))
	for _, r := range roles.Items {
		tempRoleId := strconv.FormatInt(r.RoleId, 10)
		if tempRoleId == "" {
			l.Error("google-workspace: role had no id", zap.String("name", r.RoleName))
			continue
		}
		annos := &v2.V1Identifier{
			Id: tempRoleId,
		}
		profile := roleProfile(ctx, r)
		roleResource, err := sdk.NewRoleResource(r.RoleName, resourceTypeRole, nil, r.RoleId, profile, annos)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, roleResource)
	}
	nextPage, err := bag.NextToken(roles.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}
	return rv, nextPage, nil, nil
}

func (o *roleResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var annos annotations.Annotations
	annos.Update(&v2.V1Identifier{
		Id: MembershipEntitlementID(resource.Id),
	})
	member := sdk.NewAssignmentEntitlement(resource, roleMemberEntitlement, resourceTypeUser)
	member.Description = fmt.Sprintf("Has the %s role in Google Workspace", resource.DisplayName)
	member.Annotations = annos
	member.DisplayName = fmt.Sprintf("%s Role Member", resource.DisplayName)
	return []*v2.Entitlement{member}, "", nil, nil
}

func (o *roleResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
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

	r := o.roleService.RoleAssignments.List(o.customerId).RoleId(resource.Id.Resource).MaxResults(100)
	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	roleAssignments, err := r.Context(ctx).Do()
	if err != nil {
		return nil, "", nil, err
	}
	var rv []*v2.Grant
	for _, roleAssignment := range roleAssignments.Items {
		uID, err := sdk.NewResourceID(resourceTypeUser, roleAssignment.AssignedTo)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, sdk.NewGrant(resource, roleMemberEntitlement, uID))
	}

	nextPage, err := bag.NextToken(roleAssignments.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func roleBuilder(roleService *admin.Service, customerId string) *roleResourceType {
	return &roleResourceType{
		resourceType: resourceTypeRole,
		roleService:  roleService,
		customerId:   customerId,
	}
}

func roleProfile(ctx context.Context, role *admin.Role) map[string]interface{} {
	profile := make(map[string]interface{})
	profile["role_id"] = role.RoleId
	profile["role_name"] = role.RoleName
	return profile
}
