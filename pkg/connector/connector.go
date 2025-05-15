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

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/option"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"go.uber.org/zap"
)

var (
	resourceTypeRole = &v2.ResourceType{
		Id:          "role",
		DisplayName: "role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
		Annotations: v1AnnotationsForResourceType("role"),
	}
	resourceTypeGroup = &v2.ResourceType{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: v1AnnotationsForResourceType("group"),
	}
	resourceTypeUser = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_USER,
		},
		Annotations: v1AnnotationsForResourceType("user"),
	}
	resourceTypeEnterpriseApplication = &v2.ResourceType{
		Id:          "enterprise_application",
		DisplayName: "Enterprise Application",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
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

	domainMtx sync.Mutex

	primaryDomain string
	domainsCache  []string
	reportService *reportsAdmin.Service
}

type newService[T any] func(ctx context.Context, opts ...option.ClientOption) (*T, error)

func newGWSAdminServiceForScopes[T any](ctx context.Context, credentials []byte, email string, newService newService[T], scopes ...string) (*T, error) {
	l := ctxzap.Extract(ctx)
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, l))
	if err != nil {
		return nil, err
	}

	config, err := google.JWTConfigFromJSON(credentials, scopes...)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return srv, nil
}

func (c *GoogleWorkspace) getReportService(ctx context.Context) (*reportsAdmin.Service, error) {
	if c.reportService != nil {
		return c.reportService, nil
	}
	srv, err := newGWSAdminServiceForScopes(ctx, c.credentials, c.administratorEmail, reportsAdmin.NewService, reportsAdmin.AdminReportsAuditReadonlyScope)
	if err != nil {
		return nil, err
	}
	c.reportService = srv
	return srv, nil
}

func (c *GoogleWorkspace) getDirectoryService(ctx context.Context, scope string) (*directoryAdmin.Service, error) {
	return getService(ctx, c, scope, directoryAdmin.NewService)
}

func New(ctx context.Context, config Config) (*GoogleWorkspace, error) {
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
	primaryDomain, err := c.getPrimaryDomain(ctx)
	if err != nil {
		return nil, err
	}
	var annos annotations.Annotations
	annos.Update(&v2.ExternalLink{
		Url: primaryDomain,
	})

	return &v2.ConnectorMetadata{
		DisplayName: "Google Workspace",
		Annotations: annos,
	}, nil
}

func (c *GoogleWorkspace) getPrimaryDomain(ctx context.Context) (string, error) {
	c.domainMtx.Lock()
	defer c.domainMtx.Unlock()

	if c.primaryDomain != "" {
		return c.primaryDomain, nil
	}
	err := c.fetchDomains(ctx)
	if err != nil {
		return "", err
	}
	return c.primaryDomain, nil
}

func (c *GoogleWorkspace) fetchDomains(ctx context.Context) error {
	service, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	if err != nil {
		return fmt.Errorf("google-workspace: failed to initialize service for scope %s: %w", directoryAdmin.AdminDirectoryDomainReadonlyScope, err)
	}

	resp, err := service.Domains.List(c.customerID).Context(ctx).Do()
	if err != nil {
		return err
	}

	domains := make([]string, 0, len(resp.Domains))
	for _, d := range resp.Domains {
		domains = append(domains, d.DomainName)
		if d.IsPrimary {
			c.primaryDomain = d.DomainName
		}
	}
	c.domainsCache = domains
	return nil
}

func (c *GoogleWorkspace) getDomains(ctx context.Context) ([]string, error) {
	c.domainMtx.Lock()
	defer c.domainMtx.Unlock()

	if c.domainsCache != nil {
		return c.domainsCache, nil
	}

	err := c.fetchDomains(ctx)
	if err != nil {
		return nil, err
	}

	return c.domainsCache, nil
}

func (c *GoogleWorkspace) Validate(ctx context.Context) (annotations.Annotations, error) {
	domains, err := c.getDomains(ctx)
	if err != nil {
		return nil, err
	}

	if c.domain != "" {
		for _, d := range domains {
			if strings.EqualFold(c.domain, d) {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("google-workspace: domain '%s' is not a valid domain for customer '%s'", c.domain, c.customerID)
	}

	return nil, nil
}

func (c *GoogleWorkspace) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

func (c *GoogleWorkspace) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	rs := []connectorbuilder.ResourceSyncer{}
	// We don't care about the error here, as we handle the case where the service is nil in the syncer
	roleProvisioningService, _ := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementScope)
	roleService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope)
	if err == nil {
		rs = append(rs, roleBuilder(roleService, c.customerID, roleProvisioningService))
	}

	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserReadonlyScope)
	if err == nil {
		rs = append(rs, userBuilder(userService, c.customerID, c.domain))
	}

	// We don't care about the error here, as we handle the case where the service is nil in the syncer
	groupProvisioningService, _ := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberScope)
	groupService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupReadonlyScope)
	if err == nil {
		groupMemberService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberReadonlyScope)
		if err == nil {
			rs = append(rs, groupBuilder(
				groupService,
				c.customerID,
				c.domain,
				groupMemberService,
				groupProvisioningService,
			))
		}
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
		return nil, fmt.Errorf("google-workspace: cache entry for scope %s exists, but is not of type %s", scope, reflect.TypeOf(service))
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
		return nil, err
	}
	if service != nil {
		return service, nil
	}

	upgradedScope, upgraded := upgradeScope(ctx, scope)
	if upgraded {
		service, err := getFromCache[T](ctx, c, upgradedScope)
		if err != nil {
			return nil, err
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
func upgradeScope(ctx context.Context, scope string) (string, bool) {
	if strings.HasSuffix(scope, ".readonly") {
		return strings.TrimSuffix(scope, ".readonly"), true
	}
	return scope, false
}

func (c *GoogleWorkspace) EventFeeds(ctx context.Context) []connectorbuilder.EventFeed {
	usageEventFeed := &usageEventFeed{
		connector: c,
	}

	adminEventFeed := &adminEventFeed{
		connector: c,
	}

	return []connectorbuilder.EventFeed{
		usageEventFeed,
		adminEventFeed,
	}
}
