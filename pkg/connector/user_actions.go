package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	changeUserOrgUnitActionSchema = &v2.BatonActionSchema{
		Name:        "change_user_org_unit",
		DisplayName: "Change User Organizational Unit",
		Description: "Moves a user to a different organizational unit in Google Workspace.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User ID",
				Description: "The resource ID of the user whose organizational unit should be changed.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "org_unit_path",
				DisplayName: "Organizational Unit Path",
				Description: "The full path to the organizational unit (e.g., '/corp/sales' or '/engineering'). Must start with '/'.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user's organizational unit was changed successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "resource",
				DisplayName: "Updated User",
				Description: "The updated user resource with the new organizational unit.",
				Field:       &config.Field_ResourceField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}

	signOutUserActionSchema = &v2.BatonActionSchema{
		Name:        "sign_out_user",
		DisplayName: "Sign Out User",
		Description: "Signs a user out of all web and device sessions and resets their sign-in cookies. The user will have to sign in by authenticating again.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User ID",
				Description: "The resource ID of the user to sign out.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user was signed out successfully.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_UNSPECIFIED},
	}

	deleteAllOAuthTokensActionSchema = &v2.BatonActionSchema{
		Name:        "delete_all_oauth_tokens",
		DisplayName: "Delete All OAuth Tokens",
		Description: "Deletes all OAuth access tokens issued by a user for third-party applications. This revokes access for all applications the user has authorized.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User ID",
				Description: "The resource ID of the user whose OAuth tokens should be deleted.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether all OAuth tokens were deleted successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "tokens_deleted",
				DisplayName: "Tokens Deleted",
				Description: "The number of OAuth tokens that were deleted.",
				Field:       &config.Field_IntField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_UNSPECIFIED},
	}

	offboardingProfileUpdateActionSchema = &v2.BatonActionSchema{
		Name:        "offboarding_profile_update",
		DisplayName: "Offboarding Profile Update",
		Description: "Performs offboarding profile updates for a user: removes from Global Address List (GAL), " +
			"clears recovery details (email and phone), deletes account addresses and phone numbers, " +
			"and optionally archives the account.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User ID",
				Description: "The resource ID of the user to perform offboarding profile updates on.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "archive_account",
				DisplayName: "Archive Account",
				Description: "Whether to archive the user account. Archiving requires an archived user license. Defaults to false.",
				Field:       &config.Field_BoolField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the offboarding profile updates were successfully applied.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_UNSPECIFIED},
	}
)

// ResourceActions implements the ResourceActionProvider interface for user resource actions.
func (o *userResourceType) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := o.registerChangeUserOrgUnitAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerSignOutUserAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerDeleteAllOAuthTokensAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerOffboardingProfileUpdateAction(ctx, registry); err != nil {
		return err
	}
	return nil
}

func (o *userResourceType) registerChangeUserOrgUnitAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, changeUserOrgUnitActionSchema, o.changeUserOrgUnitActionHandler)
}

// extractUserId extracts and validates the user_id argument from action args.
func extractUserId(args *structpb.Struct, l *zap.Logger, actionName string) (string, error) {
	userIdValue, ok := args.Fields["user_id"]
	if !ok || userIdValue == nil {
		l.Debug("google-workspace: user action handler: missing user_id argument", zap.String("action", actionName), zap.Any("args", args))
		return "", fmt.Errorf("missing user_id argument")
	}
	userIdField, ok := userIdValue.GetKind().(*structpb.Value_StringValue)
	if !ok || userIdField.StringValue == "" {
		return "", fmt.Errorf("invalid user_id argument")
	}
	return userIdField.StringValue, nil
}

func (o *userResourceType) changeUserOrgUnitActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.userProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "change_user_org_unit")
	if err != nil {
		return nil, nil, err
	}

	// Extract org_unit_path argument
	orgUnitPathValue, ok := args.Fields["org_unit_path"]
	if !ok || orgUnitPathValue == nil {
		l.Debug("google-workspace: user action handler: missing org_unit_path argument", zap.Any("args", args))
		return nil, nil, fmt.Errorf("missing org_unit_path argument")
	}
	orgUnitPathField, ok := orgUnitPathValue.GetKind().(*structpb.Value_StringValue)
	if !ok || orgUnitPathField.StringValue == "" {
		return nil, nil, fmt.Errorf("invalid org_unit_path argument")
	}
	orgUnitPath := orgUnitPathField.StringValue

	// Validate org_unit_path starts with '/'
	if len(orgUnitPath) == 0 || orgUnitPath[0] != '/' {
		return nil, nil, fmt.Errorf("org_unit_path must start with '/' (e.g., '/corp/sales')")
	}

	// Get current user to check current org unit
	currentUser, err := o.userProvisioningService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("google-workspace: failed to retrieve user: %s", userId))
	}

	// Check if already in the target org unit
	if currentUser.OrgUnitPath == orgUnitPath {
		// Already in the target org unit, return success
		userResource, err := o.userResource(ctx, currentUser)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to create user resource: %w", err)
		}

		resourceRv, err := actions.NewResourceReturnField("resource", userResource)
		if err != nil {
			l.Error("failed to build resource return field", zap.Error(err))
			return nil, nil, err
		}

		return actions.NewReturnValues(true, resourceRv), nil, nil
	}

	// Update the user's organizational unit
	updatedUser, err := o.userProvisioningService.Users.Update(userId, &admin.User{
		OrgUnitPath:     orgUnitPath,
		ForceSendFields: []string{"OrgUnitPath"},
	}).Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			// Check if it's a 400 Bad Request error (INVALID_OU_ID)
			if gerr.Code == http.StatusBadRequest {
				return nil, nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf(
					"google-workspace: failed to change user org unit (400 Bad Request). "+
						"Invalid org_unit_path '%s'. "+
						"Note: Org unit paths should NOT include the domain name. "+
						"They start from '/' and list only the OU hierarchy (e.g., '/test_unit_02/child-test-ou-01' not '/batonc1/test_unit_02/child-test-ou-01'). "+
						"Please verify the path exists and try again",
					orgUnitPath))
			}
		}
		return nil, nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("google-workspace: failed to change user org unit: %s", userId))
	}

	l.Debug("google-workspace: user action handler: changed org unit",
		zap.String("user_id", userId),
		zap.String("old_org_unit", currentUser.OrgUnitPath),
		zap.String("new_org_unit", orgUnitPath))

	// Create the user resource
	userResource, err := o.userResource(ctx, updatedUser)
	if err != nil {
		l.Error("failed to create user resource", zap.Error(err))
		return nil, nil, fmt.Errorf("google-workspace: failed to create user resource: %w", err)
	}

	resourceRv, err := actions.NewResourceReturnField("resource", userResource)
	if err != nil {
		l.Error("failed to build resource return field", zap.Error(err))
		return nil, nil, err
	}

	return actions.NewReturnValues(true, resourceRv), nil, nil
}

func (o *userResourceType) registerSignOutUserAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, signOutUserActionSchema, o.signOutUserActionHandler)
}

func (o *userResourceType) registerDeleteAllOAuthTokensAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, deleteAllOAuthTokensActionSchema, o.deleteAllOAuthTokensActionHandler)
}

func (o *userResourceType) registerOffboardingProfileUpdateAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, offboardingProfileUpdateActionSchema, o.offboardingProfileUpdateActionHandler)
}

func (o *userResourceType) signOutUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.userSecurityService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user security service not available - requires %s scope", admin.AdminDirectoryUserSecurityScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "sign_out_user")
	if err != nil {
		return nil, nil, err
	}

	// Sign out the user
	err = o.userSecurityService.Users.SignOut(userId).Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusForbidden {
				return nil, nil, fmt.Errorf(
					"google-workspace: failed to sign out user (403 Forbidden). "+
						"This may be due to: 1) missing OAuth scope %s, "+
						"2) insufficient admin permissions: %w",
					admin.AdminDirectoryUserSecurityScope, err)
			}
			if gerr.Code == http.StatusNotFound {
				return nil, nil, wrapGoogleApiErrorWithContext(err, "google-workspace: user not found")
			}
		}
		return nil, nil, wrapGoogleApiErrorWithContext(err, "google-workspace: failed to sign out user")
	}

	l.Debug("google-workspace: user action handler: signed out user",
		zap.String("user_id", userId))

	return actions.NewReturnValues(true), nil, nil
}

func (o *userResourceType) deleteAllOAuthTokensActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.userSecurityService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user security service not available - requires %s scope", admin.AdminDirectoryUserSecurityScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "delete_all_oauth_tokens")
	if err != nil {
		return nil, nil, err
	}

	// List all tokens for the user
	tokens, err := o.userSecurityService.Tokens.List(userId).Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusForbidden {
				return nil, nil, fmt.Errorf(
					"google-workspace: failed to list OAuth tokens (403 Forbidden). "+
						"This may be due to: 1) missing OAuth scope %s, "+
						"2) insufficient admin permissions: %w",
					admin.AdminDirectoryUserSecurityScope, err)
			}
			if gerr.Code == http.StatusNotFound {
				return nil, nil, wrapGoogleApiErrorWithContext(err, "google-workspace: user not found")
			}
		}
		return nil, nil, wrapGoogleApiErrorWithContext(err, "google-workspace: failed to list OAuth tokens")
	}

	// If no tokens, return success with 0 deleted
	if len(tokens.Items) == 0 {
		tokensDeletedRv := actions.NewNumberReturnField("tokens_deleted", 0)
		return actions.NewReturnValues(true, tokensDeletedRv), nil, nil
	}

	// Delete each token
	tokensDeleted := 0
	var lastErr error
	for _, token := range tokens.Items {
		if token.ClientId == "" {
			l.Warn("google-workspace: skipping token with empty client ID",
				zap.String("user_id", userId),
				zap.String("display_text", token.DisplayText))
			continue
		}

		err := o.userSecurityService.Tokens.Delete(userId, token.ClientId).Context(ctx).Do()
		if err != nil {
			gerr := &googleapi.Error{}
			if errors.As(err, &gerr) {
				// If token was already deleted (404), continue
				if gerr.Code == http.StatusNotFound {
					l.Debug("google-workspace: token already deleted",
						zap.String("user_id", userId),
						zap.String("client_id", token.ClientId))
					tokensDeleted++
					continue
				}
			}
			l.Error("google-workspace: failed to delete token",
				zap.String("user_id", userId),
				zap.String("client_id", token.ClientId),
				zap.Error(err))
			lastErr = err
			continue
		}
		tokensDeleted++
	}

	// If we failed to delete some tokens, return an error
	if lastErr != nil {
		return nil, nil, wrapGoogleApiErrorWithContext(lastErr, fmt.Sprintf("google-workspace: failed to delete some OAuth tokens (deleted %d of %d)",
			tokensDeleted, len(tokens.Items)))
	}

	l.Debug("google-workspace: user action handler: deleted all OAuth tokens",
		zap.String("user_id", userId),
		zap.Int("tokens_deleted", tokensDeleted))

	tokensDeletedRv := actions.NewNumberReturnField("tokens_deleted", float64(tokensDeleted))

	return actions.NewReturnValues(true, tokensDeletedRv), nil, nil
}

func (o *userResourceType) offboardingProfileUpdateActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.userProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "offboarding_profile_update")
	if err != nil {
		return nil, nil, err
	}

	// Extract archive_account argument (defaults to false if not provided)
	archiveAccount := false
	if archiveAccountValue, ok := args.Fields["archive_account"]; ok && archiveAccountValue != nil {
		if archiveAccountField, ok := archiveAccountValue.GetKind().(*structpb.Value_BoolValue); ok {
			archiveAccount = archiveAccountField.BoolValue
		}
	}

	// Build the update request
	updateUser := &admin.User{
		IncludeInGlobalAddressList: false,
		RecoveryEmail:              "",
		RecoveryPhone:              "",
		ForceSendFields:            []string{"IncludeInGlobalAddressList", "RecoveryEmail", "RecoveryPhone"},
		NullFields:                 []string{"Addresses", "Phones", "Emails"},
	}

	// Only archive if explicitly requested
	if archiveAccount {
		updateUser.Archived = true
		updateUser.ForceSendFields = append(updateUser.ForceSendFields, "Archived")
	}

	// Update the user with all offboarding profile changes in a single call:
	// 1. Remove from GAL (Global Address List)
	// 2. Clear recovery email and phone
	// 3. Delete addresses, phone numbers, and additional email addresses (using NullFields)
	//    Note: The primary email cannot be removed and will remain
	// 4. Optionally archive the account
	_, err = o.userProvisioningService.Users.Update(userId, updateUser).Context(ctx).Do()
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusForbidden {
				return nil, nil, wrapGoogleApiErrorWithContext(err,
					fmt.Sprintf("google-workspace: failed to update offboarding profile (403 Forbidden). "+
						"This may be due to: 1) missing OAuth scope %s, "+
						"2) insufficient admin permissions", admin.AdminDirectoryUserScope))
			}
			if gerr.Code == http.StatusNotFound {
				return nil, nil, wrapGoogleApiErrorWithContext(err, "google-workspace: user not found")
			}
			if gerr.Code == http.StatusPreconditionFailed {
				return nil, nil, wrapGoogleApiErrorWithContext(err,
					"google-workspace: failed to archive user account (412 Precondition Failed). "+
						"Insufficient archived user licenses. "+
						"Archiving a user requires an archived user license. "+
						"Please ensure you have available archived user licenses in your Google Workspace subscription.")
			}
		}
		return nil, nil, wrapGoogleApiErrorWithContext(err, "google-workspace: failed to update offboarding profile")
	}

	actionsList := "removed from GAL, cleared recovery details, deleted addresses/phones/emails"
	if archiveAccount {
		actionsList += ", archived"
	}

	l.Debug("google-workspace: user action handler: updated offboarding profile",
		zap.String("user_id", userId),
		zap.String("actions", actionsList))

	return actions.NewReturnValues(true), nil, nil
}
