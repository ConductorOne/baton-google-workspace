package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
	cfg "github.com/conductorone/baton-google-workspace/pkg/config"
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
	mtx                sync.Mutex
	serviceCache       map[string]any

	reportService *reportsAdmin.Service

	// client is lazily initialised on first use via getClient().
	clientMtx sync.Mutex
	client    *gwclient.GoogleWorkspaceClient
}

type newService[T any] func(ctx context.Context, opts ...option.ClientOption) (*T, error)

// isScopeUnauthorized reports whether an OAuth token-fetch error means the requested
// scope is not authorized for this service account, so the dependent service should be
// treated as unavailable rather than aborting the sync.
//
// Google returns two distinct shapes for an unauthorized scope:
//   - 401 Unauthorized for a service account that is not configured for domain-wide delegation.
//   - 403 Forbidden with error "access_denied" / "Requested client not authorized." when the
//     client is delegated but the specific scope has not been granted in the Admin console.
//
// Both must classify as authorization errors; only matching 401 turned a missing optional
// scope (e.g. admin.directory.rolemanagement, used only for role provisioning) into a fatal
// sync failure once service init became non-tolerant.
//
// The two-legged JWT flow (golang.org/x/oauth2/jwt) builds the RetrieveError with only
// Response and Body set — unlike the standard token exchange, it never parses the body into
// RetrieveError.ErrorCode. So the RFC 6749 'error' code must be read from Body here.
func isScopeUnauthorized(oe *oauth2.RetrieveError) bool {
	if oe.Response == nil {
		return false
	}
	switch oe.Response.StatusCode {
	case http.StatusUnauthorized:
		return true
	case http.StatusForbidden:
		return oauthErrorCode(oe) == "access_denied"
	default:
		return false
	}
}

// oauthErrorCode returns the RFC 6749 'error' code for a token-fetch failure, preferring the
// pre-parsed RetrieveError.ErrorCode and falling back to parsing the raw response Body (which is
// the only place the JWT flow records it).
func oauthErrorCode(oe *oauth2.RetrieveError) string {
	if oe.ErrorCode != "" {
		return oe.ErrorCode
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(oe.Body, &body); err != nil {
		return ""
	}
	return body.Error
}

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
		if errors.As(err, &oe) && isScopeUnauthorized(oe) {
			return nil, &GoogleWorkspaceOAuthUnauthorizedError{o: oe}
		}
		return nil, err
	}

	httpClient = &http.Client{
		Timeout: 30 * time.Second,
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

// recordServiceInit records an error that occurred while initializing a service for resource syncers.
// Authorization errors are logged at debug level as they are expected when scopes are not available.
// Other errors (network, context cancellation, etc.) are fatal so the sync cannot persist a partial c1z.
func recordServiceInit(l *zap.Logger, err error, scope, purpose string) error {
	if err == nil {
		return nil
	}
	if isAuthorizationError(err) {
		l.Debug("google-workspace: service not available due to missing authorization scope",
			zap.String("scope", scope),
			zap.Error(err))
		return nil
	}

	l.Error("google-workspace: failed to initialize service for resource syncer",
		zap.String("scope", scope),
		zap.String("purpose", purpose),
		zap.Error(err))
	return fmt.Errorf("google-workspace: failed to initialize %s (scope %s): %w", purpose, scope, err)
}

func (c *GoogleWorkspace) newClient(ctx context.Context) (*gwclient.GoogleWorkspaceClient, error) {
	l := ctxzap.Extract(ctx)
	client := &gwclient.GoogleWorkspaceClient{}

	var err error

	client.DomainService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryDomainReadonlyScope, "domain service"); err != nil {
		return nil, err
	}

	client.RoleService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope, "role resource synchronization"); err != nil {
		return nil, err
	}
	client.RoleProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryRolemanagementScope, "role resource provisioning"); err != nil {
		return nil, err
	}

	client.UserService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserReadonlyScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryUserReadonlyScope, "user resource synchronization"); err != nil {
		return nil, err
	}
	client.UserProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryUserScope, "user resource provisioning"); err != nil {
		return nil, err
	}
	client.UserSecurityService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserSecurityScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryUserSecurityScope, "user security operations"); err != nil {
		return nil, err
	}

	client.GroupService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupReadonlyScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryGroupReadonlyScope, "group resource synchronization"); err != nil {
		return nil, err
	}
	client.GroupMemberService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberReadonlyScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryGroupMemberReadonlyScope, "group membership synchronization"); err != nil {
		return nil, err
	}
	client.GroupMemberProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryGroupMemberScope, "group membership provisioning"); err != nil {
		return nil, err
	}
	client.GroupProvisioningService, err = c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupScope)
	if err := recordServiceInit(l, err, directoryAdmin.AdminDirectoryGroupScope, "group resource provisioning"); err != nil {
		return nil, err
	}
	client.GroupsSettingsService, err = c.getGroupsSettingsService(ctx)
	if err := recordServiceInit(l, err, "https://www.googleapis.com/auth/apps.groups.settings", "group settings"); err != nil {
		return nil, err
	}

	client.DataTransferService, err = c.getDataTransferService(ctx, datatransferAdmin.AdminDatatransferScope)
	if err := recordServiceInit(l, err, datatransferAdmin.AdminDatatransferScope, "data transfer service"); err != nil {
		return nil, err
	}

	client.ReportService, err = c.getReportService(ctx)
	if err := recordServiceInit(l, err, reportsAdmin.AdminReportsAuditReadonlyScope, "report service"); err != nil {
		return nil, err
	}

	client.CloudIdentityService, err = getService(ctx, c, cloudidentity.CloudIdentityInboundssoReadonlyScope, cloudidentity.NewService)
	if err := recordServiceInit(l, err, cloudidentity.CloudIdentityInboundssoReadonlyScope, "SAML/OIDC app discovery"); err != nil {
		return nil, err
	}

	return client, nil
}

func (c *GoogleWorkspace) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	client, err := c.getClient(ctx)
	if err != nil {
		return failedResourceSyncers(err)
	}
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

type failedResourceSyncer struct {
	resourceType *v2.ResourceType
	err          error
}

func failedResourceSyncers(err error) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{
		&failedResourceSyncer{resourceType: resourceTypeRole, err: err},
		&failedResourceSyncer{resourceType: resourceTypeUser, err: err},
		&failedResourceSyncer{resourceType: resourceTypeGroup, err: err},
		&failedResourceSyncer{resourceType: resourceTypeEnterpriseApplication, err: err},
	}
}

func (f *failedResourceSyncer) ResourceType(_ context.Context) *v2.ResourceType {
	return f.resourceType
}

func (f *failedResourceSyncer) List(_ context.Context, _ *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	return nil, nil, f.err
}

func (f *failedResourceSyncer) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, f.err
}

func (f *failedResourceSyncer) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, f.err
}

func getFromCache[T any](c *GoogleWorkspace, scope string) (*T, error) {
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

	service, err := getFromCache[T](c, scope)
	if err != nil {
		return nil, fmt.Errorf("failed to get service from cache: %w", err)
	}
	if service != nil {
		return service, nil
	}

	upgradedScope, upgraded := upgradeScope(scope)
	if upgraded {
		service, err := getFromCache[T](c, upgradedScope)
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
			upgradedScope, upgraded := upgradeScope(scope)
			if upgraded {
				l.Debug(
					"google-workspace: unauthorized, attempting scope upgrade",
					zap.Error(err),
					zap.String("scope", scope),
					zap.String("upgraded_scope", upgradedScope),
				)
				return getService(ctx, c, upgradedScope, newService)
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
func upgradeScope(scope string) (string, bool) {
	if strings.HasSuffix(scope, ".readonly") {
		return strings.TrimSuffix(scope, ".readonly"), true
	}
	return scope, false
}

// getClient returns the shared GoogleWorkspaceClient, initialising it once after a successful init.
// It is safe to call from multiple goroutines.
func (c *GoogleWorkspace) getClient(ctx context.Context) (*gwclient.GoogleWorkspaceClient, error) {
	c.clientMtx.Lock()
	defer c.clientMtx.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	client, err := c.newClient(ctx)
	if err != nil {
		return nil, err
	}
	c.client = client
	return c.client, nil
}

func (c *GoogleWorkspace) EventFeeds(ctx context.Context) []connectorbuilder.EventFeed {
	client, err := c.getClient(ctx)
	if err != nil {
		return failedEventFeeds(err)
	}

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

type failedEventFeed struct {
	metadata *v2.EventFeedMetadata
	err      error
}

func failedEventFeeds(err error) []connectorbuilder.EventFeed {
	return []connectorbuilder.EventFeed{
		&failedEventFeed{metadata: newUsageEventFeed(nil).EventFeedMetadata(context.Background()), err: err},
		&failedEventFeed{metadata: newAdminEventFeed(nil).EventFeedMetadata(context.Background()), err: err},
		&failedEventFeed{metadata: newSamlEventFeed(nil, "").EventFeedMetadata(context.Background()), err: err},
		&failedEventFeed{metadata: newGoogleLoginEventFeed(nil).EventFeedMetadata(context.Background()), err: err},
	}
}

func (f *failedEventFeed) EventFeedMetadata(_ context.Context) *v2.EventFeedMetadata {
	return f.metadata
}

func (f *failedEventFeed) ListEvents(_ context.Context, _ *timestamppb.Timestamp, _ *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return nil, nil, nil, f.err
}

var _ connectorbuilder.GlobalActionProvider = (*GoogleWorkspace)(nil)

func (c *GoogleWorkspace) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := registry.Register(ctx, updateUserStatusActionSchema, c.updateUserStatus); err != nil {
		return fmt.Errorf("google-workspace: failed to register update_user_status action: %w", err)
	}
	if err := registry.Register(ctx, transferUserDriveFilesActionSchema, c.transferUserDriveFiles); err != nil {
		return fmt.Errorf("google-workspace: failed to register transfer_user_drive_files action: %w", err)
	}
	if err := registry.Register(ctx, changeUserPrimaryEmailActionSchema, c.changeUserPrimaryEmail); err != nil {
		return fmt.Errorf("google-workspace: failed to register change_user_primary_email action: %w", err)
	}
	if err := registry.Register(ctx, disableUserActionSchema, c.disableUserActionHandler); err != nil {
		return fmt.Errorf("google-workspace: failed to register disable_user action: %w", err)
	}
	if err := registry.Register(ctx, enableUserActionSchema, c.enableUserActionHandler); err != nil {
		return fmt.Errorf("google-workspace: failed to register enable_user action: %w", err)
	}
	if err := registry.Register(ctx, transferUserCalendarActionSchema, c.transferUserCalendar); err != nil {
		return fmt.Errorf("google-workspace: failed to register transfer_user_calendar action: %w", err)
	}
	if err := registry.Register(ctx, updateUserGlobalActionSchema, c.updateUserActionHandler); err != nil {
		return fmt.Errorf("google-workspace: failed to register update_user action: %w", err)
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
