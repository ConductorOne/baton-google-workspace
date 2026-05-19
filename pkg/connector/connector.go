package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	cloudidentity "google.golang.org/api/cloudidentity/v1"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	cfg "github.com/conductorone/baton-google-workspace/pkg/config"
	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

func capabilityPermissions(perms ...string) *v2.CapabilityPermissions {
	cp := &v2.CapabilityPermissions{}
	for _, p := range perms {
		cp.Permissions = append(cp.Permissions, &v2.CapabilityPermission{Permission: p})
	}
	return cp
}

func v1AnnotationsWithPermissions(resourceTypeID string, perms *v2.CapabilityPermissions) annotations.Annotations {
	annos := v1AnnotationsForResourceType(resourceTypeID)
	annos.Update(perms)
	return annos
}

var (
	resourceTypeRole = &v2.ResourceType{
		Id:          "role",
		DisplayName: "role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
		Annotations: v1AnnotationsWithPermissions("role", capabilityPermissions(
			"admin.directory.rolemanagement",
			"admin.directory.domain.readonly",
		)),
	}
	resourceTypeGroup = &v2.ResourceType{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: v1AnnotationsWithPermissions("group", capabilityPermissions(
			"admin.directory.group",
			"admin.directory.group.member",
			"admin.directory.domain.readonly",
		)),
	}
	resourceTypeUser = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_USER,
		},
		Annotations: v1AnnotationsWithPermissions("user", capabilityPermissions(
			"admin.directory.user.readonly",
			"admin.directory.user.alias.readonly",
			"admin.directory.domain.readonly",
		)),
	}
	resourceTypeEnterpriseApplication = &v2.ResourceType{
		Id:          "enterprise_application",
		DisplayName: "Enterprise Application",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
		Annotations: annotations.New(
			capabilityPermissions(
				"admin.directory.user.readonly",
				"admin.directory.user.security",
				"admin.reports.audit.readonly",
				"cloud-identity.inboundsso.readonly",
			),
		),
	}
	updateUserStatusActionSchema = &v2.BatonActionSchema{
		Name: "update_user_status",
		Arguments: []*config.Field{
			{
				Name:        "resource_id",
				DisplayName: "User Resource ID",
				Description: "ID of the user resource to update the status of",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "is_suspended",
				DisplayName: "Is Suspended",
				Description: "Update the user status to suspended or active",
				Field:       &config.Field_BoolField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user resource status was updated successfully",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	transferUserDriveFilesActionSchema = &v2.BatonActionSchema{
		Name:        "transfer_user_drive_files",
		DisplayName: "Transfer User Drive Files",
		Description: "Initiate a Google Drive ownership transfer from one user to another.",
		Arguments: []*config.Field{
			{
				Name:        "resource_id",
				DisplayName: "Source User Resource ID",
				Description: "ID of the user resource to transfer Drive ownership from.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "target_resource_id",
				DisplayName: "Target User Resource ID",
				Description: "ID of the user resource to receive Drive ownership.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "privacy_levels",
				DisplayName: "Drive Privacy Levels",
				Description: "One or more of private, shared. Defaults to both.",
				Field:       &config.Field_StringSliceField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the transfer request was created successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "transfer_id",
				DisplayName: "Transfer ID",
				Description: "ID of the Data Transfer request.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "status",
				DisplayName: "Transfer Status",
				Description: "Initial status returned by the Data Transfer API (e.g., IN_PROGRESS).",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	transferUserCalendarActionSchema = &v2.BatonActionSchema{
		Name:        "transfer_user_calendar",
		DisplayName: "Transfer User Calendar",
		Description: "Initiate a Google Calendar transfer from one user to another.",
		Arguments: []*config.Field{
			{
				Name:        "resource_id",
				DisplayName: "Source User Resource ID",
				Description: "ID of the user resource to transfer calendar data from.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "target_resource_id",
				DisplayName: "Target User Resource ID",
				Description: "ID of the user resource to receive calendar data.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "release_resources",
				DisplayName: "Release Resources",
				Description: "If true, sets RELEASE_RESOURCES=TRUE (release resources for future events).",
				Field:       &config.Field_BoolField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the transfer request was created successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "transfer_id",
				DisplayName: "Transfer ID",
				Description: "ID of the Data Transfer request.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "status",
				DisplayName: "Transfer Status",
				Description: "Initial status returned by the Data Transfer API (e.g., IN_PROGRESS).",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	changeUserPrimaryEmailActionSchema = &v2.BatonActionSchema{
		Name:        "change_user_primary_email",
		DisplayName: "Change User Primary Email",
		Description: "Update a user's primary email address.",
		Arguments: []*config.Field{
			{
				Name:        "resource_id",
				DisplayName: "User Resource ID",
				Description: "ID of the user resource to update.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "new_primary_email",
				DisplayName: "New Primary Email",
				Description: "New primary email address (must be within a verified domain).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the primary email was updated successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "previous_primary_email",
				DisplayName: "Previous Primary Email",
				Description: "User's previous primary email address.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "new_primary_email",
				DisplayName: "New Primary Email",
				Description: "User's updated primary email address.",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	disableUserActionSchema = &v2.BatonActionSchema{
		Name:        "disable_user",
		DisplayName: "Disable User",
		Description: "Suspend a user account.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User Resource ID",
				Description: "ID of the user resource to disable (suspend).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user was disabled (suspended) successfully.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT_DISABLE},
	}
	enableUserActionSchema = &v2.BatonActionSchema{
		Name:        "enable_user",
		DisplayName: "Enable User",
		Description: "Unsuspend a user account.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User Resource ID",
				Description: "ID of the user resource to enable (unsuspend).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user was enabled (unsuspended) successfully.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT_ENABLE},
	}
	moveAccountToOrgUnitActionSchema = &v2.BatonActionSchema{
		Name:        "move_account_to_org_unit",
		DisplayName: "Move Account to Organizational Unit",
		Description: "Move an account to a different organizational unit in Google Workspace.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User Resource ID",
				Description: "ID of the user resource to move.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "org_unit_path",
				DisplayName: "Organizational Unit Path",
				Description: "Full path of the organizational unit (e.g., '/Sales' or '/Engineering/Backend'). Use '/' for root.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the account was moved successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "previous_org_unit_path",
				DisplayName: "Previous Org Unit Path",
				Description: "Account's previous organizational unit path.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "new_org_unit_path",
				DisplayName: "New Org Unit Path",
				Description: "Account's new organizational unit path.",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
)

type Config struct {
	CustomerID         string
	AdministratorEmail string
	Domain             string
	Credentials        []byte
}

type GoogleWorkspace struct {
	customerID         string
	domain             string
	administratorEmail string
	credentials        []byte
	mtx          sync.Mutex
	serviceCache map[string]any

	reportService *reportsAdmin.Service

	// client is lazily initialised on first use via getClient().
	clientOnce sync.Once
	client     *gwclient.GoogleWorkspaceClient
}

type newService[T any] func(ctx context.Context, opts ...option.ClientOption) (*T, error)

func newGWSAdminServiceForScopes[T any](ctx context.Context, credentials []byte, email string, newService newService[T], scopes ...string) (*T, error) {
	l := ctxzap.Extract(ctx)
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, l))
	if err != nil {
		return nil, uhttp.WrapErrors(codes.Internal, "failed to create HTTP client", err)
	}

	config, err := google.JWTConfigFromJSON(credentials, scopes...)
	if err != nil {
		return nil, uhttp.WrapErrors(codes.InvalidArgument, "failed to parse JWT config from credentials", err)
	}
	config.Subject = email

	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	tokenSrc := config.TokenSource(ctx)
	token, err := tokenSrc.Token()
	if err != nil {
		l.Debug("google-workspace: failed fetching token", zap.Error(err))
		var oe *oauth2.RetrieveError
		if errors.As(err, &oe) {
			if oe.Response != nil && oe.Response.StatusCode == http.StatusUnauthorized {
				return nil, &GoogleWorkspaceOAuthUnauthorizedError{o: oe}
			}
		}
		return nil, err
	}

	httpClient = &http.Client{
		Transport: &oauth2.Transport{
			Base:   httpClient.Transport,
			Source: oauth2.ReuseTokenSource(token, tokenSrc),
		},
	}
	srv, err := newService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, uhttp.WrapErrors(codes.Internal, "failed to create service", err)
	}
	return srv, nil
}

func (c *GoogleWorkspace) getReportService(ctx context.Context) (*reportsAdmin.Service, error) {
	if c.reportService != nil {
		return c.reportService, nil
	}
	srv, err := newGWSAdminServiceForScopes(ctx, c.credentials, c.administratorEmail, reportsAdmin.NewService, reportsAdmin.AdminReportsAuditReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("failed to create report service: %w", err)
	}
	c.reportService = srv
	return srv, nil
}

func (c *GoogleWorkspace) getDirectoryService(ctx context.Context, scope string) (*directoryAdmin.Service, error) {
	return getService(ctx, c, scope, directoryAdmin.NewService)
}

func (c *GoogleWorkspace) getGroupsSettingsService(ctx context.Context) (*groupssettings.Service, error) {
	const groupsSettingsScope = "https://www.googleapis.com/auth/apps.groups.settings"
	return getService(ctx, c, groupsSettingsScope, groupssettings.NewService)
}

func (c *GoogleWorkspace) getDataTransferService(ctx context.Context, scope string) (*datatransferAdmin.Service, error) {
	return getService(ctx, c, scope, datatransferAdmin.NewService)
}

// New creates a new Google Workspace connector with the V2 interface.
func New(ctx context.Context, config *cfg.GoogleWorkspace, opts *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	var credentialBytes []byte
	switch {
	case config.CredentialsJson != "":
		credentialBytes = []byte(config.CredentialsJson)
	case len(config.CredentialsJsonFilePath) > 0:
		credentialBytes = config.CredentialsJsonFilePath
	default:
		return nil, nil, fmt.Errorf("credentials-json or credentials-json-file-path is required")
	}

	connector, err := NewConnector(ctx, Config{
		CustomerID:         config.CustomerId,
		AdministratorEmail: config.AdministratorEmail,
		Domain:             config.Domain,
		Credentials:        credentialBytes,
	})
	if err != nil {
		return nil, nil, err
	}

	return connector, []connectorbuilder.Opt{}, nil
}

// NewConnector creates a new Google Workspace connector instance (internal constructor).
func NewConnector(ctx context.Context, config Config) (*GoogleWorkspace, error) {
	rv := &GoogleWorkspace{
		customerID:         config.CustomerID,
		administratorEmail: config.AdministratorEmail,
		credentials:        config.Credentials,
		serviceCache:       map[string]any{},
		domain:             config.Domain,
	}
	return rv, nil
}

func (c *GoogleWorkspace) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	_, err := c.Validate(ctx)
	if err != nil {
		return nil, err
	}

	domainService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	if err != nil {
		return nil, err
	}
	client := &gwclient.GoogleWorkspaceClient{DomainService: domainService}

	resp, err := client.ListDomains(ctx, c.customerID)
	if err != nil {
		return nil, err
	}

	var primaryDomain string
	for _, d := range resp.Domains {
		if d.IsPrimary {
			primaryDomain = d.DomainName
			break
		}
	}

	var annos annotations.Annotations
	annos.Update(&v2.ExternalLink{
		Url: primaryDomain,
	})

	return &v2.ConnectorMetadata{
		DisplayName: "Google Workspace",
		Annotations: annos,
		AccountCreationSchema: &v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
				"email": {
					DisplayName: "Email",
					Required:    true,
					Description: "Email address for the new user account. Must be unique within the domain.",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "user@example.com",
					Order:       1,
				},
				"given_name": {
					DisplayName: "First Name",
					Required:    true,
					Description: "User's first name.",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "John",
					Order:       2,
				},
				"family_name": {
					DisplayName: "Last Name",
					Required:    true,
					Description: "User's last name.",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "Doe",
					Order:       3,
				},
				"changePasswordAtNextLogin": {
					DisplayName: "Change Password at Next Login",
					Required:    false,
					Description: "If true, the user will be required to change their password at next login. A random password is always generated.",
					Field: &v2.ConnectorAccountCreationSchema_Field_BoolField{
						BoolField: &v2.ConnectorAccountCreationSchema_BoolField{},
					},
					Placeholder: "false",
					Order:       4,
				},
			},
		},
	}, nil
}

func (c *GoogleWorkspace) Validate(ctx context.Context) (annotations.Annotations, error) {
	domainService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	if err != nil {
		return nil, err
	}
	client := &gwclient.GoogleWorkspaceClient{DomainService: domainService}

	resp, err := client.ListDomains(ctx, c.customerID)
	if err != nil {
		return nil, err
	}
	domains := make([]string, 0, len(resp.Domains))
	for _, d := range resp.Domains {
		domains = append(domains, d.DomainName)
	}

	if c.domain != "" {
		for _, d := range domains {
			if strings.EqualFold(c.domain, d) {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("domain '%s' is not a valid domain for customer '%s'", c.domain, c.customerID)
	}

	return nil, nil
}

func (c *GoogleWorkspace) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

// isAuthorizationError returns true if the error is an authorization error that can be safely ignored
// when initializing resource syncers. Non-authorization errors (network, context cancellation, etc.)
// should be logged with more detail as they indicate real problems.
func isAuthorizationError(err error) bool {
	var ae *GoogleWorkspaceOAuthUnauthorizedError
	return errors.As(err, &ae)
}

// logServiceInitError logs an error that occurred while initializing a service for resource syncers.
// Authorization errors are logged at debug level as they are expected when scopes are not available.
// Other errors (network, context cancellation, etc.) are logged at error level with more detail.
func logServiceInitError(l *zap.Logger, err error, scope, purpose string) {
	if isAuthorizationError(err) {
		l.Debug("google-workspace: service not available due to missing authorization scope",
			zap.String("scope", scope),
			zap.Error(err))
	} else {
		l.Error("google-workspace: failed to initialize service for resource syncer",
			zap.String("scope", scope),
			zap.String("purpose", purpose),
			zap.Error(err))
	}
}

func (c *GoogleWorkspace) newClient(ctx context.Context) *gwclient.GoogleWorkspaceClient {
	l := ctxzap.Extract(ctx)
	client := &gwclient.GoogleWorkspaceClient{}

	var err error

	client.DomainService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryDomainReadonlyScope, "domain service")
	}

	client.RoleService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope, "role resource synchronization")
	}
	client.RoleProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryRolemanagementScope, "role resource provisioning")
	}

	client.UserService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserReadonlyScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryUserReadonlyScope, "user resource synchronization")
	}
	client.UserProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryUserScope, "user resource provisioning")
	}
	client.UserSecurityService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserSecurityScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryUserSecurityScope, "user security operations")
	}

	client.GroupService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupReadonlyScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryGroupReadonlyScope, "group resource synchronization")
	}
	client.GroupMemberService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberReadonlyScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryGroupMemberReadonlyScope, "group membership synchronization")
	}
	client.GroupMemberProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryGroupMemberScope, "group membership provisioning")
	}
	client.GroupProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupScope)
	if err != nil {
		logServiceInitError(l, err, directoryAdmin.AdminDirectoryGroupScope, "group resource provisioning")
	}
	client.GroupsSettingsService, err = c.getGroupsSettingsService(ctx)
	if err != nil {
		logServiceInitError(l, err, "https://www.googleapis.com/auth/apps.groups.settings", "group settings")
	}

	client.DataTransferService, err = c.getDataTransferService(ctx, datatransferAdmin.AdminDatatransferScope)
	if err != nil {
		logServiceInitError(l, err, datatransferAdmin.AdminDatatransferScope, "data transfer service")
	}

	client.ReportService, err = c.getReportService(ctx)
	if err != nil {
		logServiceInitError(l, err, reportsAdmin.AdminReportsAuditReadonlyScope, "report service")
	}

	client.CloudIdentityService, err = getService(ctx, c, cloudidentity.CloudIdentityInboundssoReadonlyScope, cloudidentity.NewService)
	if err != nil {
		logServiceInitError(l, err, cloudidentity.CloudIdentityInboundssoReadonlyScope, "SAML/OIDC app discovery")
	}

	return client
}

func (c *GoogleWorkspace) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	client := c.getClient(ctx)
	rs := []connectorbuilder.ResourceSyncerV2{}

	if client.RoleService != nil {
		rs = append(rs, roleBuilder(client, c.customerID))
	}

	if client.UserService != nil {
		rs = append(rs, userBuilder(client, c.customerID, c.domain))
	}

	if client.GroupService != nil && client.GroupMemberService != nil {
		rs = append(rs, groupBuilder(client, c.customerID, c.domain))
	}

	if client.UserService != nil && client.UserSecurityService != nil && client.ReportService != nil {
		rs = append(rs, newApplicationResource(client, c.customerID, c.domain))
	}

	return rs
}

func getFromCache[T any](ctx context.Context, c *GoogleWorkspace, scope string) (*T, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	service, ok := c.serviceCache[scope]
	if ok {
		if service, ok := service.(*T); ok {
			return service, nil
		}
		return nil, fmt.Errorf("cache entry for scope %s exists, but is not of type %s", scope, reflect.TypeOf(service))
	}
	return nil, nil
}

// getService will return an *admin.Service for the given scope, caching the result.
// If you request a 'readonly' scope and get a 401 fetching a token, getService will attempt 'upgrade'
// the scope (strip the 'readonly') and try again.
func getService[T any](ctx context.Context, c *GoogleWorkspace, scope string, newService newService[T]) (*T, error) {
	l := ctxzap.Extract(ctx)

	service, err := getFromCache[T](ctx, c, scope)
	if err != nil {
		return nil, fmt.Errorf("failed to get service from cache: %w", err)
	}
	if service != nil {
		return service, nil
	}

	upgradedScope, upgraded := upgradeScope(ctx, scope)
	if upgraded {
		service, err := getFromCache[T](ctx, c, upgradedScope)
		if err != nil {
			return nil, fmt.Errorf("failed to get upgraded service from cache: %w", err)
		}
		if service != nil {
			return service, nil
		}
	}

	service, err = newGWSAdminServiceForScopes(ctx, c.credentials, c.administratorEmail, newService, scope)
	if err != nil {
		var ae *GoogleWorkspaceOAuthUnauthorizedError
		if errors.As(err, &ae) {
			upgradedScope, upgraded := upgradeScope(ctx, scope)
			if upgraded {
				l.Debug(
					"google-workspace: unauthorized, attempting scope upgrade",
					zap.Error(err),
					zap.String("scope", scope),
					zap.String("upgraded_scope", upgradedScope),
				)
				return getService[T](ctx, c, upgradedScope, newService)
			}
		}
		return nil, err
	}

	c.mtx.Lock()
	c.serviceCache[scope] = service
	c.mtx.Unlock()
	return service, nil
}

// upgradeScope strips '.readonly' from the given scope, if it exists.
func upgradeScope(ctx context.Context, scope string) (string, bool) {
	if strings.HasSuffix(scope, ".readonly") {
		return strings.TrimSuffix(scope, ".readonly"), true
	}
	return scope, false
}

// getClient returns the shared GoogleWorkspaceClient, initialising it once.
// It is safe to call from multiple goroutines.
func (c *GoogleWorkspace) getClient(ctx context.Context) *gwclient.GoogleWorkspaceClient {
	c.clientOnce.Do(func() {
		c.client = c.newClient(ctx)
	})
	return c.client
}

func (c *GoogleWorkspace) EventFeeds(ctx context.Context) []connectorbuilder.EventFeed {
	client := c.getClient(ctx)

	feeds := []connectorbuilder.EventFeed{
		newUsageEventFeed(client),
		newAdminEventFeed(client),
	}

	if client.ReportService != nil {
		feeds = append(feeds, newSamlEventFeed(client, c.customerID))
		feeds = append(feeds, newGoogleLoginEventFeed(client))
	}

	return feeds
}

func (c *GoogleWorkspace) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	l := ctxzap.Extract(ctx)

	if err := registry.Register(ctx, updateUserStatusActionSchema, c.updateUserStatus); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return fmt.Errorf("failed to register update_user_status action: %w", err)
	}
	if err := registry.Register(ctx, transferUserDriveFilesActionSchema, c.transferUserDriveFiles); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return fmt.Errorf("failed to register transfer_user_drive_files action: %w", err)
	}
	if err := registry.Register(ctx, changeUserPrimaryEmailActionSchema, c.changeUserPrimaryEmail); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return fmt.Errorf("failed to register change_user_primary_email action: %w", err)
	}
	if err := registry.Register(ctx, disableUserActionSchema, c.disableUserActionHandler); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return fmt.Errorf("failed to register disable_user action: %w", err)
	}
	if err := registry.Register(ctx, enableUserActionSchema, c.enableUserActionHandler); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return fmt.Errorf("failed to register enable_user action: %w", err)
	}
	if err := registry.Register(ctx, transferUserCalendarActionSchema, c.transferUserCalendar); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return fmt.Errorf("failed to register transfer_user_calendar action: %w", err)
	}
	if err := registry.Register(ctx, moveAccountToOrgUnitActionSchema, c.moveAccountToOrgUnit); err != nil {
		l.Error("failed to register action", zap.Error(err))
		return err
	}

	return nil
}

// DefaultCapabilitiesBuilder returns all resource types unconditionally so that
// the generated capabilities are always complete regardless of connector configuration.
func DefaultCapabilitiesBuilder() connectorbuilder.ConnectorBuilderV2 {
	return &defaultCapabilitiesBuilder{}
}

type defaultCapabilitiesBuilder struct{}

func (d *defaultCapabilitiesBuilder) Metadata(_ context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{DisplayName: "Google Workspace"}, nil
}

func (d *defaultCapabilitiesBuilder) Validate(_ context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (d *defaultCapabilitiesBuilder) ResourceSyncers(_ context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{
		roleBuilder(nil, ""),
		userBuilder(nil, "", ""),
		groupBuilder(nil, "", ""),
		newApplicationResource(nil, "", ""),
	}
}

func (d *defaultCapabilitiesBuilder) EventFeeds(_ context.Context) []connectorbuilder.EventFeed {
	return []connectorbuilder.EventFeed{
		newUsageEventFeed(nil),
		newAdminEventFeed(nil),
		newSamlEventFeed(nil, ""),
		newGoogleLoginEventFeed(nil),
	}
}
