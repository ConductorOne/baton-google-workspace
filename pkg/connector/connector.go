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
)

type Config struct {
	CustomerID         string
	Domain             string
	AdministratorEmail string
	Credentials        []byte
}

type GoogleWorkspace struct {
	customerID         string
	domain             string
	administratorEmail string
	credentials        []byte

	mtx          sync.Mutex
	serviceCache map[string]any
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

func (c *GoogleWorkspace) getRoleService(ctx context.Context, scope string) (*reportsAdmin.Service, error) {
	return getService(ctx, c, scope, reportsAdmin.NewService)
}

func (c *GoogleWorkspace) getDirectoryService(ctx context.Context, scope string) (*directoryAdmin.Service, error) {
	return getService(ctx, c, scope, directoryAdmin.NewService)
}

func New(ctx context.Context, config Config) (*GoogleWorkspace, error) {
	rv := &GoogleWorkspace{
		customerID:         config.CustomerID,
		domain:             config.Domain,
		administratorEmail: config.AdministratorEmail,
		credentials:        config.Credentials,
		serviceCache:       map[string]any{},
	}
	return rv, nil
}

func (c *GoogleWorkspace) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	_, err := c.Validate(ctx)
	if err != nil {
		return nil, err
	}

	var annos annotations.Annotations
	annos.Update(&v2.ExternalLink{
		Url: c.domain,
	})

	return &v2.ConnectorMetadata{
		DisplayName: "Google Workspace",
		Annotations: annos,
	}, nil
}

func (c *GoogleWorkspace) Validate(ctx context.Context) (annotations.Annotations, error) {
	service, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("google-workspace: failed to initialize service for scope %s: %w", directoryAdmin.AdminDirectoryDomainReadonlyScope, err)
	}

	_, err = service.Domains.Get(c.customerID, c.domain).Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (c *GoogleWorkspace) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

func (c *GoogleWorkspace) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	rs := []connectorbuilder.ResourceSyncer{}
	roleService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope)
	if err == nil {
		rs = append(rs, roleBuilder(roleService, c.customerID))
	}
	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserReadonlyScope)
	if err == nil {
		rs = append(rs, userBuilder(userService, c.customerID, c.domain))
	}
	groupService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupReadonlyScope)
	if err == nil {
		groupMemberService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberReadonlyScope)
		if err == nil {
			rs = append(rs, groupBuilder(groupService, c.customerID, c.domain, groupMemberService))
		}
	}
	return rs
}

func updateCache[T any](ctx context.Context, c *GoogleWorkspace, scope string) (*T, error) {
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

	service, err := updateCache[T](ctx, c, scope)
	if err != nil {
		return nil, err
	}
	if service != nil {
		return service, nil
	}

	upgradedScope, upgraded := upgradeScope(ctx, scope)
	if upgraded {
		service, err := updateCache[T](ctx, c, upgradedScope)
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
