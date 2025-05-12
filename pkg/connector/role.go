package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	sdkGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
)

const (
	roleMemberEntitlement = "member"
)

type roleResourceType struct {
	resourceType            *v2.ResourceType
	roleService             *admin.Service
	roleProvisioningService *admin.Service
	customerId              string
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
		traitOpts := []sdkResource.RoleTraitOption{sdkResource.WithRoleProfile(roleProfile(ctx, r))}
		roleResource, err := sdkResource.NewRoleResource(r.RoleName, resourceTypeRole, tempRoleId, traitOpts, sdkResource.WithAnnotation(annos))
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
		Id: V1MembershipEntitlementID(resource.Id.Resource),
	})
	member := sdkEntitlement.NewAssignmentEntitlement(resource, roleMemberEntitlement, sdkEntitlement.WithGrantableTo(resourceTypeUser))
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
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			// Return no grants if the role no longer exists. This might happen if the role is deleted during a sync.
			if gerr.Code == http.StatusNotFound {
				return nil, "", nil, uhttp.WrapErrors(codes.NotFound, fmt.Sprintf("no role found with id %s", resource.Id.Resource))
			}
		}
		return nil, "", nil, err
	}
	var rv []*v2.Grant
	for _, roleAssignment := range roleAssignments.Items {
		tempRoleAssignmentId := strconv.FormatInt(roleAssignment.RoleAssignmentId, 10)
		v1Identifier := &v2.V1Identifier{
			Id: tempRoleAssignmentId,
		}
		uID, err := sdkResource.NewResourceID(resourceTypeUser, roleAssignment.AssignedTo)
		if err != nil {
			return nil, "", nil, err
		}
		grant := sdkGrant.NewGrant(resource, roleMemberEntitlement, uID, sdkGrant.WithAnnotation(v1Identifier))
		grant.Id = tempRoleAssignmentId
		rv = append(rv, grant)
	}

	nextPage, err := bag.NextToken(roleAssignments.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func roleBuilder(roleService *admin.Service, customerId string, roleProvisioningService *admin.Service) *roleResourceType {
	return &roleResourceType{
		resourceType:            resourceTypeRole,
		roleService:             roleService,
		customerId:              customerId,
		roleProvisioningService: roleProvisioningService,
	}
}

func roleProfile(ctx context.Context, role *admin.Role) map[string]interface{} {
	profile := make(map[string]interface{})
	profile["role_id"] = role.RoleId
	profile["role_name"] = role.RoleName
	return profile
}

func (o *roleResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if o.roleProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace-v2: unable to get service for scope %s", directoryAdmin.AdminDirectoryRolemanagementScope)
	}
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, nil, errors.New("google-workspace-v2: user principal is required")
	}

	tempRoleId, err := strconv.ParseInt(entitlement.Resource.Id.Resource, 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace-v2: failed to convert roleId to string: %w", err)
	}
	r := o.roleProvisioningService.RoleAssignments.Insert(o.customerId, &admin.RoleAssignment{
		AssignedTo: principal.GetId().GetResource(),
		RoleId:     tempRoleId,
		ScopeType:  "CUSTOMER",
	})
	assignment, err := r.Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusConflict {
				// We don't need to do anything here, the user is already a member of the role
				// We unfortunately can't get the role assignment to return as a grant, so we just return nil
				return nil, nil, nil
			}
		}
		return nil, nil, fmt.Errorf("google-workspace-v2: failed to insert role member: %w", err)
	}

	grant := sdkGrant.NewGrant(entitlement.Resource, roleMemberEntitlement, principal.GetId())
	grant.Id = strconv.FormatInt(assignment.RoleAssignmentId, 10)
	return []*v2.Grant{grant}, nil, nil
}

func (o *roleResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	if o.roleProvisioningService == nil {
		return nil, fmt.Errorf("google-workspace-v2: unable to get service for scope %s", directoryAdmin.AdminDirectoryRolemanagementScope)
	}
	if grant.Principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, errors.New("google-workspace-v2: user principal is required")
	}
	l := ctxzap.Extract(ctx)

	r := o.roleProvisioningService.RoleAssignments.Delete(o.customerId, grant.Id)
	err := r.Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				// This should only hit if someone double-revokes, but I'd rather we log something about it
				l.Info("google-workspace-v2: role member is being deleted but doesn't exist",
					zap.String("group_id", grant.Entitlement.Resource.Id.Resource),
					zap.String("user_id", grant.Principal.GetId().GetResource()))
				return nil, nil
			}
		}
		return nil, fmt.Errorf("google-workspace-v2: failed to remove role member: %w", err)
	}

	return nil, nil
}

func (o *roleResourceType) Get(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return nil, nil, nil
}
