package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TODO: implement your connector here
type connectorImpl struct {
}

func (c *connectorImpl) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *connectorImpl) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *connectorImpl) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *connectorImpl) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *connectorImpl) GetMetadata(ctx context.Context, req *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *connectorImpl) Validate(ctx context.Context, req *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *connectorImpl) GetAsset(req *v2.AssetServiceGetAssetRequest, server v2.AssetService_GetAssetServer) error {
	return fmt.Errorf("not implemented")
}
