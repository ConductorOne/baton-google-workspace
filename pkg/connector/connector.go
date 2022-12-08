package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/directory/v1"
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
	serviceCache map[string]*admin.Service
}

func (o *GoogleWorkspace) newGoogleWorkspaceAdminServiceForScopes(ctx context.Context, scopes ...string) (*admin.Service, error) {
	l := ctxzap.Extract(ctx)
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, l))
	if err != nil {
		return nil, err
	}

	config, err := google.JWTConfigFromJSON(o.credentials, scopes...)
	if err != nil {
		return nil, err
	}
	config.Subject = o.administratorEmail

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
	srv, err := admin.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func New(ctx context.Context, config Config) (*GoogleWorkspace, error) {
	rv := &GoogleWorkspace{
		customerID:         config.CustomerID,
		domain:             config.Domain,
		administratorEmail: config.AdministratorEmail,
		credentials:        []byte(config.Credentials),
		serviceCache:       map[string]*admin.Service{},
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
	service, err := c.getService(ctx, admin.AdminDirectoryDomainReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("google-workspace: failed to initialize service for scope %s: %w", admin.AdminDirectoryDomainReadonlyScope, err)
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
	roleService, err := c.getService(ctx, admin.AdminDirectoryRolemanagementReadonlyScope)
	if err == nil {
		rs = append(rs, roleBuilder(roleService, c.customerID))
	}
	userService, err := c.getService(ctx, admin.AdminDirectoryUserReadonlyScope)
	if err == nil {
		rs = append(rs, userBuilder(userService, c.customerID, c.domain))
	}
	groupService, err := c.getService(ctx, admin.AdminDirectoryGroupReadonlyScope)
	if err == nil {
		groupMemberService, err := c.getService(ctx, admin.AdminDirectoryGroupMemberReadonlyScope)
		if err == nil {
			rs = append(rs, groupBuilder(groupService, c.customerID, c.domain, groupMemberService))
		}
	}
	return rs
}

// getService will return an *admin.Service for the given scope, caching the result.
// If you request a 'readonly' scope and get a 401 fetching a token, getService will attempt 'upgrade'
// the scope (strip the 'readonly') and try again.
func (c *GoogleWorkspace) getService(ctx context.Context, scope string) (*admin.Service, error) {
	l := ctxzap.Extract(ctx)
	c.mtx.Lock()
	service, ok := c.serviceCache[scope]
	if ok {
		c.mtx.Unlock()
		return service, nil
	}

	upgradedScope, upgraded := upgradeScope(ctx, scope)
	if upgraded {
		service, ok := c.serviceCache[upgradedScope]
		if ok {
			c.mtx.Unlock()
			return service, nil
		}
	}
	c.mtx.Unlock()

	service, err := c.newGoogleWorkspaceAdminServiceForScopes(ctx, scope)
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
				return c.getService(ctx, upgradedScope)
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
