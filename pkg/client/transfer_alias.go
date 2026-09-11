package client

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/grpc/codes"
)

// GetDataTransfer reads a single data-transfer record by its provider-assigned
// ID via Transfers.Get. Read-only: it never inserts, updates or restarts a
// transfer. Returns errServiceNotAvailable when the data transfer service is
// nil (scope not granted), and preserves Google status codes (404 NotFound,
// 403 PermissionDenied etc.) via wrapGoogleApiErrorWithContext.
func (c *GoogleWorkspaceClient) GetDataTransfer(ctx context.Context, transferId string) (*datatransferAdmin.DataTransfer, error) {
	if c.DataTransferService == nil {
		return nil, errServiceNotAvailable("data transfer service")
	}
	resp, err := c.DataTransferService.Transfers.Get(transferId).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to get data transfer: %s", transferId))
	}
	return resp, nil
}

// DeleteUserAlias removes an email alias from a user via
// Users.Aliases.Delete on the EXISTING user provisioning service. No new
// service or scope is introduced: the broader admin.directory.user scope that
// already authorizes user provisioning covers alias deletion. Callers must
// have verified the alias exists on this exact user (readback) before calling,
// because a 404 here is ambiguous between "user missing" and "alias missing".
func (c *GoogleWorkspaceClient) DeleteUserAlias(ctx context.Context, userKey, alias string) error {
	if c.UserProvisioningService == nil {
		return errServiceNotAvailable("user provisioning service")
	}
	err := c.UserProvisioningService.Users.Aliases.Delete(userKey, alias).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete alias %s for user: %s", alias, userKey))
	}
	return nil
}

// InsertUserAlias adds an email alias to a user via Users.Aliases.Insert on
// the EXISTING user provisioning service. No new service or scope is
// introduced: the broader admin.directory.user scope that already authorizes
// user provisioning covers alias insertion. A 409 from the provider is the
// authoritative signal that the address is already owned by another user or
// namespace (e.g. a group); callers qualify it with a bounded readback rather
// than retrying.
func (c *GoogleWorkspaceClient) InsertUserAlias(ctx context.Context, userKey, alias string) error {
	if c.UserProvisioningService == nil {
		return errServiceNotAvailable("user provisioning service")
	}
	_, err := c.UserProvisioningService.Users.Aliases.Insert(userKey, &directoryAdmin.Alias{Alias: alias}).Context(ctx).Do()
	if err != nil {
		return wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to insert alias %s for user: %s", alias, userKey))
	}
	return nil
}

// ListUserAliases reads the dedicated, complete alias collection for one user.
func (c *GoogleWorkspaceClient) ListUserAliases(ctx context.Context, userID string) ([]string, error) {
	if c.UserProvisioningService == nil {
		return nil, errServiceNotAvailable("user provisioning service")
	}
	var result *directoryAdmin.Aliases
	result, err := c.UserProvisioningService.Users.Aliases.List(userID).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list user aliases")
	}
	if result == nil {
		return nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias collection response is missing")
	}
	aliases := make([]string, 0, len(result.Aliases))
	for _, item := range result.Aliases {
		fields, ok := item.(map[string]interface{})
		if !ok {
			return nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias collection contains an invalid entry")
		}
		value, ok := fields["alias"].(string)
		if !ok || value == "" {
			return nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias collection contains an entry without an address")
		}
		aliases = append(aliases, value)
	}
	return aliases, nil
}
