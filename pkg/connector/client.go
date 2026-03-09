package connector

import (
	"context"
	"fmt"

	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	groupssettings "google.golang.org/api/groupssettings/v1"
)

// errServiceNotAvailable returns a standardised error for when a required Google
// API service is nil (scope not granted or initialisation failed).
func errServiceNotAvailable(service string) error {
	return fmt.Errorf("google-workspace: %s not available", service)
}

// GoogleWorkspaceClient wraps all Google API service instances and handles error
// wrapping via wrapGoogleApiErrorWithContext so callers don't need to.
//
// Fields may be nil when the corresponding OAuth scope was not granted;
// callers are responsible for nil-checking before use.
type GoogleWorkspaceClient struct {
	// Directory – users
	userService             *directoryAdmin.Service
	userProvisioningService *directoryAdmin.Service
	userSecurityService     *directoryAdmin.Service

	// Directory – groups
	groupService                   *directoryAdmin.Service
	groupMemberService             *directoryAdmin.Service
	groupMemberProvisioningService *directoryAdmin.Service
	groupProvisioningService       *directoryAdmin.Service

	// Directory – roles
	roleService             *directoryAdmin.Service
	roleProvisioningService *directoryAdmin.Service

	// Directory – domains (connector-level)
	domainService *directoryAdmin.Service

	// Other services
	groupsSettingsService *groupssettings.Service
	dataTransferService   *datatransferAdmin.Service
	reportService         *reportsAdmin.Service
}

// ---------------------------------------------------------------------------
// Domains
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListDomains(ctx context.Context, customerId string) (*directoryAdmin.Domains2, error) {
	if c.domainService == nil {
		return nil, errServiceNotAvailable("domain service")
	}
	resp, err := c.domainService.Domains.List(customerId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list domains")
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Capability checks
// ---------------------------------------------------------------------------

// RequireUserProvisioning returns an error if the user provisioning service is
// not available, keeping the capability check and scope context inside the client.
func (c *GoogleWorkspaceClient) RequireUserProvisioning() error {
	if c.userProvisioningService == nil {
		return fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", directoryAdmin.AdminDirectoryUserScope)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Users – read
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListUsers(ctx context.Context, customerId, domain, pageToken string) (*directoryAdmin.Users, error) {
	if c.userService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	r := c.userService.Users.List().OrderBy("email").Projection("full").MaxResults(500)
	if domain != "" {
		r = r.Domain(domain)
	} else {
		r = r.Customer(customerId)
	}
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list users")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetUser(ctx context.Context, userId string) (*directoryAdmin.User, error) {
	if c.userService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	resp, err := c.userService.Users.Get(userId).Projection("full").Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get user: %s", userId))
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Users – write (requires userProvisioningService)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) GetUserForProvisioning(ctx context.Context, userId string) (*directoryAdmin.User, error) {
	if c.userProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.userProvisioningService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetUserFullForProvisioning(ctx context.Context, userId string) (*directoryAdmin.User, error) {
	if c.userProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.userProvisioningService.Users.Get(userId).Projection("full").Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) InsertUser(ctx context.Context, user *directoryAdmin.User) (*directoryAdmin.User, error) {
	if c.userProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.userProvisioningService.Users.Insert(user).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to create user")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) UpdateUser(ctx context.Context, userId string, user *directoryAdmin.User) (*directoryAdmin.User, error) {
	if c.userProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.userProvisioningService.Users.Update(userId, user).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to update user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteUser(ctx context.Context, userId string) error {
	if c.userProvisioningService == nil {
		return errServiceNotAvailable("user provisioning service")
	}
	err := c.userProvisioningService.Users.Delete(userId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete user: %s", userId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Users – security (requires userSecurityService)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) SignOutUser(ctx context.Context, userId string) error {
	if c.userSecurityService == nil {
		return errServiceNotAvailable("user security service")
	}
	err := c.userSecurityService.Users.SignOut(userId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to sign out user: %s", userId))
	}
	return nil
}

func (c *GoogleWorkspaceClient) ListTokens(ctx context.Context, userId string) (*directoryAdmin.Tokens, error) {
	if c.userSecurityService == nil {
		return nil, errServiceNotAvailable("user security service")
	}
	resp, err := c.userSecurityService.Tokens.List(userId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list tokens for user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteToken(ctx context.Context, userId, clientId string) error {
	if c.userSecurityService == nil {
		return errServiceNotAvailable("user security service")
	}
	err := c.userSecurityService.Tokens.Delete(userId, clientId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete token for user: %s", userId))
	}
	return nil
}

func (c *GoogleWorkspaceClient) ListAsps(ctx context.Context, userId string) (*directoryAdmin.Asps, error) {
	if c.userSecurityService == nil {
		return nil, errServiceNotAvailable("user security service")
	}
	resp, err := c.userSecurityService.Asps.List(userId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list application passwords for user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteAsp(ctx context.Context, userId string, codeId int64) error {
	if c.userSecurityService == nil {
		return errServiceNotAvailable("user security service")
	}
	err := c.userSecurityService.Asps.Delete(userId, codeId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete application password for user: %s", userId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Groups – read
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListGroups(ctx context.Context, customerId, domain, pageToken string) (*directoryAdmin.Groups, error) {
	if c.groupService == nil {
		return nil, errServiceNotAvailable("group service")
	}
	r := c.groupService.Groups.List().MaxResults(200)
	if domain != "" {
		r = r.Domain(domain)
	} else {
		r = r.Customer(customerId)
	}
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list groups")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetGroup(ctx context.Context, groupKey string) (*directoryAdmin.Group, error) {
	if c.groupService == nil {
		return nil, errServiceNotAvailable("group service")
	}
	resp, err := c.groupService.Groups.Get(groupKey).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get group: %s", groupKey))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) ListMembers(ctx context.Context, groupId, pageToken string) (*directoryAdmin.Members, error) {
	if c.groupMemberService == nil {
		return nil, errServiceNotAvailable("group member service")
	}
	r := c.groupMemberService.Members.List(groupId).MaxResults(200)
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list members for group: %s", groupId))
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Groups – write (requires provisioning services)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) InsertGroup(ctx context.Context, group *directoryAdmin.Group) (*directoryAdmin.Group, error) {
	if c.groupProvisioningService == nil {
		return nil, errServiceNotAvailable("group provisioning service")
	}
	resp, err := c.groupProvisioningService.Groups.Insert(group).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to create group")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteGroup(ctx context.Context, groupId string) error {
	if c.groupProvisioningService == nil {
		return errServiceNotAvailable("group provisioning service")
	}
	err := c.groupProvisioningService.Groups.Delete(groupId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete group: %s", groupId))
	}
	return nil
}

func (c *GoogleWorkspaceClient) InsertMember(ctx context.Context, groupId string, member *directoryAdmin.Member) (*directoryAdmin.Member, error) {
	if c.groupMemberProvisioningService == nil {
		return nil, errServiceNotAvailable("group member provisioning service")
	}
	resp, err := c.groupMemberProvisioningService.Members.Insert(groupId, member).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to add member to group: %s", groupId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetMember(ctx context.Context, groupId, memberKey string) (*directoryAdmin.Member, error) {
	if c.groupMemberProvisioningService == nil {
		return nil, errServiceNotAvailable("group member provisioning service")
	}
	resp, err := c.groupMemberProvisioningService.Members.Get(groupId, memberKey).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get member %s in group: %s", memberKey, groupId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteMember(ctx context.Context, groupId, memberKey string) error {
	if c.groupMemberProvisioningService == nil {
		return errServiceNotAvailable("group member provisioning service")
	}
	err := c.groupMemberProvisioningService.Members.Delete(groupId, memberKey).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to remove member %s from group: %s", memberKey, groupId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Groups Settings
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) GetGroupSettings(ctx context.Context, groupEmail string) (*groupssettings.Groups, error) {
	if c.groupsSettingsService == nil {
		return nil, errServiceNotAvailable("groups settings service")
	}
	resp, err := c.groupsSettingsService.Groups.Get(groupEmail).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get settings for group: %s", groupEmail))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) PatchGroupSettings(ctx context.Context, groupEmail string, settings *groupssettings.Groups) (*groupssettings.Groups, error) {
	if c.groupsSettingsService == nil {
		return nil, errServiceNotAvailable("groups settings service")
	}
	resp, err := c.groupsSettingsService.Groups.Patch(groupEmail, settings).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to update settings for group: %s", groupEmail))
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Roles – read
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListRoles(ctx context.Context, customerId, pageToken string) (*directoryAdmin.Roles, error) {
	if c.roleService == nil {
		return nil, errServiceNotAvailable("role service")
	}
	r := c.roleService.Roles.List(customerId).MaxResults(100)
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list roles")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetRole(ctx context.Context, customerId, roleId string) (*directoryAdmin.Role, error) {
	if c.roleService == nil {
		return nil, errServiceNotAvailable("role service")
	}
	resp, err := c.roleService.Roles.Get(customerId, roleId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get role: %s", roleId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) ListRoleAssignments(ctx context.Context, customerId, roleId, pageToken string) (*directoryAdmin.RoleAssignments, error) {
	if c.roleService == nil {
		return nil, errServiceNotAvailable("role service")
	}
	r := c.roleService.RoleAssignments.List(customerId).RoleId(roleId).MaxResults(100)
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list role assignments for role: %s", roleId))
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Roles – write (requires roleProvisioningService)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) InsertRoleAssignment(ctx context.Context, customerId string, assignment *directoryAdmin.RoleAssignment) (*directoryAdmin.RoleAssignment, error) {
	if c.roleProvisioningService == nil {
		return nil, errServiceNotAvailable("role provisioning service")
	}
	resp, err := c.roleProvisioningService.RoleAssignments.Insert(customerId, assignment).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to assign role")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteRoleAssignment(ctx context.Context, customerId, assignmentId string) error {
	if c.roleProvisioningService == nil {
		return errServiceNotAvailable("role provisioning service")
	}
	err := c.roleProvisioningService.RoleAssignments.Delete(customerId, assignmentId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete role assignment: %s", assignmentId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Data Transfer
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListDataTransfers(ctx context.Context, oldOwnerUserId, newOwnerUserId, pageToken string) (*datatransferAdmin.DataTransfersListResponse, error) {
	if c.dataTransferService == nil {
		return nil, errServiceNotAvailable("data transfer service")
	}
	r := c.dataTransferService.Transfers.List().OldOwnerUserId(oldOwnerUserId).NewOwnerUserId(newOwnerUserId)
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list data transfers")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) InsertDataTransfer(ctx context.Context, transfer *datatransferAdmin.DataTransfer) (*datatransferAdmin.DataTransfer, error) {
	if c.dataTransferService == nil {
		return nil, errServiceNotAvailable("data transfer service")
	}
	resp, err := c.dataTransferService.Transfers.Insert(transfer).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to create data transfer")
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListActivities(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken string, maxResults int64) (*reportsAdmin.Activities, error) {
	if c.reportService == nil {
		return nil, errServiceNotAvailable("report service")
	}
	r := c.reportService.Activities.List(userKey, applicationName).MaxResults(maxResults)
	if eventName != "" {
		r = r.EventName(eventName)
	}
	if startTime != "" {
		r = r.StartTime(startTime)
	}
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list %s activities", applicationName))
	}
	return resp, nil
}
