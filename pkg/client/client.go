package client

import (
	"context"
	"fmt"

	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	cloudidentity "google.golang.org/api/cloudidentity/v1"
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
	UserService             *directoryAdmin.Service
	UserProvisioningService *directoryAdmin.Service
	UserSecurityService     *directoryAdmin.Service

	// Directory – groups
	GroupService                   *directoryAdmin.Service
	GroupMemberService             *directoryAdmin.Service
	GroupMemberProvisioningService *directoryAdmin.Service
	GroupProvisioningService       *directoryAdmin.Service

	// Directory – roles
	RoleService             *directoryAdmin.Service
	RoleProvisioningService *directoryAdmin.Service

	// Directory – domains (connector-level)
	DomainService *directoryAdmin.Service

	// Other services
	GroupsSettingsService *groupssettings.Service
	DataTransferService   *datatransferAdmin.Service
	ReportService         *reportsAdmin.Service

	// Cloud Identity – SAML profiles (optional; nil when scope not granted)
	CloudIdentityService *cloudidentity.Service
}

// ---------------------------------------------------------------------------
// Domains
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListDomains(ctx context.Context, customerId string) (*directoryAdmin.Domains2, error) {
	if c.DomainService == nil {
		return nil, errServiceNotAvailable("domain service")
	}
	resp, err := c.DomainService.Domains.List(customerId).Context(ctx).Do()
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
	if c.UserProvisioningService == nil {
		return fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", directoryAdmin.AdminDirectoryUserScope)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Users – read
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListUsers(ctx context.Context, customerId, domain, pageToken string) (*directoryAdmin.Users, error) {
	if c.UserService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	// Projection("full") returns extensive per-user data (orgs, custom schemas,
	// relations, etc.). With large directories this can produce payloads that
	// exceed Lambda memory or timeout limits, causing Unhandled crashes.
	// Keep MaxResults low to bound per-page payload size.
	r := c.UserService.Users.List().OrderBy("email").Projection("full").MaxResults(50)
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
	if c.UserService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	resp, err := c.UserService.Users.Get(userId).Projection("full").Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get user: %s", userId))
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Users – write (requires UserProvisioningService)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) GetUserForProvisioning(ctx context.Context, userId string) (*directoryAdmin.User, error) {
	if c.UserProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.UserProvisioningService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetUserFullForProvisioning(ctx context.Context, userId string) (*directoryAdmin.User, error) {
	if c.UserProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.UserProvisioningService.Users.Get(userId).Projection("full").Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) InsertUser(ctx context.Context, user *directoryAdmin.User) (*directoryAdmin.User, error) {
	if c.UserProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.UserProvisioningService.Users.Insert(user).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to create user")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) UpdateUser(ctx context.Context, userId string, user *directoryAdmin.User) (*directoryAdmin.User, error) {
	if c.UserProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	resp, err := c.UserProvisioningService.Users.Update(userId, user).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to update user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteUser(ctx context.Context, userId string) error {
	if c.UserProvisioningService == nil {
		return errServiceNotAvailable("user provisioning service")
	}
	err := c.UserProvisioningService.Users.Delete(userId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete user: %s", userId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Users – security (requires UserSecurityService)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) SignOutUser(ctx context.Context, userId string) error {
	if c.UserSecurityService == nil {
		return errServiceNotAvailable("user security service")
	}
	err := c.UserSecurityService.Users.SignOut(userId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to sign out user: %s", userId))
	}
	return nil
}

func (c *GoogleWorkspaceClient) ListTokens(ctx context.Context, userId string) (*directoryAdmin.Tokens, error) {
	if c.UserSecurityService == nil {
		return nil, errServiceNotAvailable("user security service")
	}
	resp, err := c.UserSecurityService.Tokens.List(userId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list tokens for user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteToken(ctx context.Context, userId, clientId string) error {
	if c.UserSecurityService == nil {
		return errServiceNotAvailable("user security service")
	}
	err := c.UserSecurityService.Tokens.Delete(userId, clientId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete token for user: %s", userId))
	}
	return nil
}

func (c *GoogleWorkspaceClient) ListAsps(ctx context.Context, userId string) (*directoryAdmin.Asps, error) {
	if c.UserSecurityService == nil {
		return nil, errServiceNotAvailable("user security service")
	}
	resp, err := c.UserSecurityService.Asps.List(userId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to list application passwords for user: %s", userId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteAsp(ctx context.Context, userId string, codeId int64) error {
	if c.UserSecurityService == nil {
		return errServiceNotAvailable("user security service")
	}
	err := c.UserSecurityService.Asps.Delete(userId, codeId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete application password for user: %s", userId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Groups – read
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListGroups(ctx context.Context, customerId, domain, pageToken string) (*directoryAdmin.Groups, error) {
	if c.GroupService == nil {
		return nil, errServiceNotAvailable("group service")
	}
	r := c.GroupService.Groups.List().MaxResults(200)
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
	if c.GroupService == nil {
		return nil, errServiceNotAvailable("group service")
	}
	resp, err := c.GroupService.Groups.Get(groupKey).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get group: %s", groupKey))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) ListMembers(ctx context.Context, groupId, pageToken string) (*directoryAdmin.Members, error) {
	if c.GroupMemberService == nil {
		return nil, errServiceNotAvailable("group member service")
	}
	r := c.GroupMemberService.Members.List(groupId).MaxResults(200)
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
	if c.GroupProvisioningService == nil {
		return nil, errServiceNotAvailable("group provisioning service")
	}
	resp, err := c.GroupProvisioningService.Groups.Insert(group).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to create group")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteGroup(ctx context.Context, groupId string) error {
	if c.GroupProvisioningService == nil {
		return errServiceNotAvailable("group provisioning service")
	}
	err := c.GroupProvisioningService.Groups.Delete(groupId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete group: %s", groupId))
	}
	return nil
}

func (c *GoogleWorkspaceClient) InsertMember(ctx context.Context, groupId string, member *directoryAdmin.Member) (*directoryAdmin.Member, error) {
	if c.GroupMemberProvisioningService == nil {
		return nil, errServiceNotAvailable("group member provisioning service")
	}
	resp, err := c.GroupMemberProvisioningService.Members.Insert(groupId, member).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to add member to group: %s", groupId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) GetMember(ctx context.Context, groupId, memberKey string) (*directoryAdmin.Member, error) {
	if c.GroupMemberProvisioningService == nil {
		return nil, errServiceNotAvailable("group member provisioning service")
	}
	resp, err := c.GroupMemberProvisioningService.Members.Get(groupId, memberKey).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get member %s in group: %s", memberKey, groupId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteMember(ctx context.Context, groupId, memberKey string) error {
	if c.GroupMemberProvisioningService == nil {
		return errServiceNotAvailable("group member provisioning service")
	}
	err := c.GroupMemberProvisioningService.Members.Delete(groupId, memberKey).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to remove member %s from group: %s", memberKey, groupId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Groups Settings
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) GetGroupSettings(ctx context.Context, groupEmail string) (*groupssettings.Groups, error) {
	if c.GroupsSettingsService == nil {
		return nil, errServiceNotAvailable("groups settings service")
	}
	resp, err := c.GroupsSettingsService.Groups.Get(groupEmail).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get settings for group: %s", groupEmail))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) PatchGroupSettings(ctx context.Context, groupEmail string, settings *groupssettings.Groups) (*groupssettings.Groups, error) {
	if c.GroupsSettingsService == nil {
		return nil, errServiceNotAvailable("groups settings service")
	}
	resp, err := c.GroupsSettingsService.Groups.Patch(groupEmail, settings).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to update settings for group: %s", groupEmail))
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Roles – read
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListRoles(ctx context.Context, customerId, pageToken string) (*directoryAdmin.Roles, error) {
	if c.RoleService == nil {
		return nil, errServiceNotAvailable("role service")
	}
	r := c.RoleService.Roles.List(customerId).MaxResults(100)
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
	if c.RoleService == nil {
		return nil, errServiceNotAvailable("role service")
	}
	resp, err := c.RoleService.Roles.Get(customerId, roleId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get role: %s", roleId))
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) ListRoleAssignments(ctx context.Context, customerId, roleId, pageToken string) (*directoryAdmin.RoleAssignments, error) {
	if c.RoleService == nil {
		return nil, errServiceNotAvailable("role service")
	}
	r := c.RoleService.RoleAssignments.List(customerId).RoleId(roleId).MaxResults(100)
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
// Roles – write (requires RoleProvisioningService)
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) InsertRoleAssignment(ctx context.Context, customerId string, assignment *directoryAdmin.RoleAssignment) (*directoryAdmin.RoleAssignment, error) {
	if c.RoleProvisioningService == nil {
		return nil, errServiceNotAvailable("role provisioning service")
	}
	resp, err := c.RoleProvisioningService.RoleAssignments.Insert(customerId, assignment).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to assign role")
	}
	return resp, nil
}

func (c *GoogleWorkspaceClient) DeleteRoleAssignment(ctx context.Context, customerId, assignmentId string) error {
	if c.RoleProvisioningService == nil {
		return errServiceNotAvailable("role provisioning service")
	}
	err := c.RoleProvisioningService.RoleAssignments.Delete(customerId, assignmentId).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete role assignment: %s", assignmentId))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Data Transfer
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListDataTransfers(ctx context.Context, oldOwnerUserId, newOwnerUserId, pageToken string) (*datatransferAdmin.DataTransfersListResponse, error) {
	if c.DataTransferService == nil {
		return nil, errServiceNotAvailable("data transfer service")
	}
	r := c.DataTransferService.Transfers.List().OldOwnerUserId(oldOwnerUserId).NewOwnerUserId(newOwnerUserId)
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
	if c.DataTransferService == nil {
		return nil, errServiceNotAvailable("data transfer service")
	}
	resp, err := c.DataTransferService.Transfers.Insert(transfer).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to create data transfer")
	}
	return resp, nil
}

// ListUserIDsPage lists users returning only id and primaryEmail fields, optimized for
// high-volume app discovery where full user profiles are not needed.
func (c *GoogleWorkspaceClient) ListUserIDsPage(ctx context.Context, customerID, domain, pageToken string) (*directoryAdmin.Users, error) {
	if c.UserService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	r := c.UserService.Users.List().
		MaxResults(500).
		Fields("nextPageToken,users(id,primaryEmail)")
	if domain != "" {
		r = r.Domain(domain)
	} else {
		r = r.Customer(customerID)
	}
	if pageToken != "" {
		r = r.PageToken(pageToken)
	}
	resp, err := r.Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list user IDs")
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

func (c *GoogleWorkspaceClient) ListActivities(ctx context.Context, userKey, applicationName, eventName, startTime, pageToken string, maxResults int64) (*reportsAdmin.Activities, error) {
	if c.ReportService == nil {
		return nil, errServiceNotAvailable("report service")
	}
	r := c.ReportService.Activities.List(userKey, applicationName).MaxResults(maxResults)
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

// ---------------------------------------------------------------------------
// Cloud Identity
// ---------------------------------------------------------------------------

// ListInboundSamlProfiles paginates InboundSamlSsoProfiles for the given customer,
// calling fn for each page. Returns errServiceNotAvailable if CloudIdentityService is nil.
func (c *GoogleWorkspaceClient) ListInboundSamlProfiles(ctx context.Context, customerID string, fn func(*cloudidentity.ListInboundSamlSsoProfilesResponse) error) error {
	if c.CloudIdentityService == nil {
		return errServiceNotAvailable("cloud identity service")
	}
	customerFilter := fmt.Sprintf(`customer=="customers/%s"`, customerID)
	return c.CloudIdentityService.InboundSamlSsoProfiles.List().
		Filter(customerFilter).
		PageSize(100).
		Pages(ctx, fn)
}

// BuildSAMLProfileMap returns a displayName → profile.Name mapping for all Cloud Identity SAML
// profiles. profile.Name is the stable server-assigned ID that survives admin renames.
// OIDC profiles are excluded. Returns errServiceNotAvailable if CloudIdentityService is nil.
func (c *GoogleWorkspaceClient) BuildSAMLProfileMap(ctx context.Context, customerID string) (map[string]string, error) {
	profileMap := map[string]string{}
	if err := c.ListInboundSamlProfiles(ctx, customerID, func(resp *cloudidentity.ListInboundSamlSsoProfilesResponse) error {
		for _, profile := range resp.InboundSamlSsoProfiles {
			if profile.DisplayName == "" || profile.Name == "" {
				continue
			}
			profileMap[profile.DisplayName] = profile.Name
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("google-workspace-connector: failed to list SAML profiles: %w", err)
	}
	return profileMap, nil
}
