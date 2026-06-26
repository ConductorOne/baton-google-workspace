package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// Compile-time assertion: user actions are registered as resource-scoped actions.
var _ connectorbuilder.ResourceActionProvider = (*userResourceType)(nil)

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
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
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
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
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
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
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
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
	}

	updateUserManagerActionSchema = &v2.BatonActionSchema{
		Name:        "update_user_manager",
		DisplayName: "Update User Manager",
		Description: "Updates the manager relation for a user in Google Workspace. Updates the 'manager' entry in the user's Relations field.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User ID",
				Description: "The resource ID of the user whose manager should be changed.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "manager_email",
				DisplayName: "Manager Email",
				Description: "The email address of the new manager.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user's manager was changed successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "resource",
				DisplayName: "Updated User",
				Description: "The updated user resource with the new manager.",
				Field:       &config.Field_ResourceField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
	}

	deleteAllApplicationPasswordsActionSchema = &v2.BatonActionSchema{
		Name:        "delete_all_application_passwords",
		DisplayName: "Delete All Application Passwords",
		Description: "Deletes all application-specific passwords (ASPs) issued by a user." +
			" Application-specific passwords are used with applications that do not accept a verification code when logging in." +
			" This action deletes all ASPs for the user, including those created by the user themselves.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: "User ID",
				Description: "The resource ID of the user whose application passwords should be deleted.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether all application passwords were deleted successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "passwords_deleted",
				DisplayName: "Passwords Deleted",
				Description: "The number of application passwords that were deleted.",
				Field:       &config.Field_IntField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
	}

	updateUserProfileActionSchema = &v2.BatonActionSchema{
		Name:        "update_user_profile",
		DisplayName: "Update User Profile",
		Description: "Applies a partial update to a user's profile using patch semantics " +
			"(only the provided fields are modified, so unrelated server-side state is preserved). " +
			"Supports name fields, recovery details, and custom-schema attribute values. " +
			"At least one updatable field must be provided.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: displayUser,
				Description: "The user to update.",
				IsRequired:  true,
				Field: &config.Field_ResourceIdField{
					ResourceIdField: &config.ResourceIdField{
						Rules: &config.ResourceIDRules{
							AllowedResourceTypeIds: []string{resourceTypeUser.Id},
						},
					},
				},
			},
			{
				Name:        argGivenName,
				DisplayName: "Given Name",
				Description: "New first/given name for the user.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argFamilyName,
				DisplayName: "Family Name",
				Description: "New last/family name for the user.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argRecoveryEmail,
				DisplayName: "Recovery Email",
				Description: "New recovery email address. Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        "recovery_phone",
				DisplayName: "Recovery Phone",
				Description: "New recovery phone (E.164, e.g. +14155550100). Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argCustomSchemas,
				DisplayName: "Custom Schemas",
				Description: "JSON object mapping schema name to its field values, e.g. " +
					`{"MySchema":{"region":"emea"}}. Sent verbatim to the Directory API customSchemas field. ` +
					"Schema definitions must already exist (managed outside the connector).",
				Field:      &config.Field_StringField{},
				IsRequired: false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user's profile was updated successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "resource",
				DisplayName: "Updated User",
				Description: "The updated user resource.",
				Field:       &config.Field_ResourceField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT_UPDATE_PROFILE},
	}

	makeUserAdminActionSchema = &v2.BatonActionSchema{
		Name:        "make_admin",
		DisplayName: "Make User Super Admin",
		Description: "Promotes (status=true) or demotes (status=false) a user to/from super administrator in Google Workspace.",
		Arguments: []*config.Field{
			{
				Name:        "user_id",
				DisplayName: displayUser,
				Description: "The user whose super-admin status should be changed.",
				IsRequired:  true,
				Field: &config.Field_ResourceIdField{
					ResourceIdField: &config.ResourceIdField{
						Rules: &config.ResourceIDRules{
							AllowedResourceTypeIds: []string{resourceTypeUser.Id},
						},
					},
				},
			},
			{
				Name:        fieldStatus,
				DisplayName: "Admin Status",
				Description: "true to grant super-admin, false to revoke it.",
				Field:       &config.Field_BoolField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the user's super-admin status was updated successfully.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
	}
)

// ResourceActions implements the ResourceActionProvider interface for user resource actions.
func (o *userResourceType) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := o.registerChangeUserOrgUnitAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerOffboardingProfileUpdateAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerSignOutUserAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerDeleteAllOAuthTokensAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerDeleteAllApplicationPasswordsAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerUpdateUserManagerAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerUpdateUserProfileAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerMakeAdminAction(ctx, registry); err != nil {
		return err
	}
	return nil
}

func (o *userResourceType) registerChangeUserOrgUnitAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, changeUserOrgUnitActionSchema, o.changeUserOrgUnitActionHandler)
}

func (o *userResourceType) changeUserOrgUnitActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserProvisioningService == nil {
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
	currentUser, err := o.client.GetUserForProvisioning(ctx, userId)
	if err != nil {
		return nil, nil, err
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
			return nil, nil, fmt.Errorf("google-workspace: failed to build resource return field: %w", err)
		}

		return actions.NewReturnValues(true, resourceRv), nil, nil
	}

	// Update the user's organizational unit
	updatedUser, err := o.client.UpdateUser(ctx, userId, &admin.User{
		OrgUnitPath:     orgUnitPath,
		ForceSendFields: []string{"OrgUnitPath"},
	})
	if err != nil {
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) {
			// Check if it's a 400 Bad Request error (INVALID_OU_ID)
			if gerr.Code == http.StatusBadRequest {
				return nil, nil, fmt.Errorf(
					"google-workspace: failed to change user org unit (400 Bad Request). "+
						"Invalid org_unit_path '%s'. "+
						"Note: Org unit paths should NOT include the domain name. "+
						"They start from '/' and list only the OU hierarchy (e.g., '/test_unit_02/child-test-ou-01' not '/batonc1/test_unit_02/child-test-ou-01'). "+
						"Please verify the path exists and try again: %w",
					orgUnitPath, err)
			}
		}
		return nil, nil, err
	}

	l.Debug("google-workspace: user action handler: changed org unit",
		zap.String("user_id", userId),
		zap.String("old_org_unit", currentUser.OrgUnitPath),
		zap.String("new_org_unit", orgUnitPath))

	// Create the user resource
	userResource, err := o.userResource(ctx, updatedUser)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to create user resource: %w", err)
	}

	resourceRv, err := actions.NewResourceReturnField("resource", userResource)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to build resource return field: %w", err)
	}

	return actions.NewReturnValues(true, resourceRv), nil, nil
}

func (o *userResourceType) registerOffboardingProfileUpdateAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, offboardingProfileUpdateActionSchema, o.offboardingProfileUpdateActionHandler)
}

func (o *userResourceType) offboardingProfileUpdateActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserProvisioningService == nil {
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
	// The client wraps the Google API error (gRPC code + context + rate-limit
	// info) via wrapGoogleApiErrorWithContext, so pass it through unchanged.
	_, err = o.client.UpdateUser(ctx, userId, updateUser)
	if err != nil {
		// Non-obvious cause worth surfacing: archiving requires an available
		// archived-user license, which Google reports as a bare 412.
		var gerr *googleapi.Error
		if archiveAccount && errors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed {
			return nil, nil, fmt.Errorf("google-workspace: archiving user %s requires an available archived user license: %w", userId, err)
		}
		return nil, nil, err
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

func (o *userResourceType) registerSignOutUserAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, signOutUserActionSchema, o.signOutUserActionHandler)
}

func (o *userResourceType) registerDeleteAllOAuthTokensAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, deleteAllOAuthTokensActionSchema, o.deleteAllOAuthTokensActionHandler)
}

func (o *userResourceType) registerDeleteAllApplicationPasswordsAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, deleteAllApplicationPasswordsActionSchema, o.deleteAllApplicationPasswordsActionHandler)
}

func (o *userResourceType) signOutUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserSecurityService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user security service not available - requires %s scope", admin.AdminDirectoryUserSecurityScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "sign_out_user")
	if err != nil {
		return nil, nil, err
	}

	// Sign out the user
	err = o.client.SignOutUser(ctx, userId)
	if err != nil {
		return nil, nil, err
	}

	l.Debug("google-workspace: user action handler: signed out user",
		zap.String("user_id", userId))

	return actions.NewReturnValues(true), nil, nil
}

func (o *userResourceType) deleteAllOAuthTokensActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserSecurityService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user security service not available - requires %s scope", admin.AdminDirectoryUserSecurityScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "delete_all_oauth_tokens")
	if err != nil {
		return nil, nil, err
	}

	// List all tokens for the user
	tokens, err := o.client.ListTokens(ctx, userId)
	if err != nil {
		return nil, nil, err
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
			l.Debug("google-workspace: skipping token with empty client ID",
				zap.String("user_id", userId),
				zap.String("display_text", token.DisplayText))
			continue
		}

		err := o.client.DeleteToken(ctx, userId, token.ClientId)
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
		return nil, nil, fmt.Errorf("google-workspace: failed to delete some OAuth tokens (deleted %d of %d): %w",
			tokensDeleted, len(tokens.Items), lastErr)
	}

	l.Debug("google-workspace: user action handler: deleted all OAuth tokens",
		zap.String("user_id", userId),
		zap.Int("tokens_deleted", tokensDeleted))

	tokensDeletedRv := actions.NewNumberReturnField("tokens_deleted", float64(tokensDeleted))

	return actions.NewReturnValues(true, tokensDeletedRv), nil, nil
}

func (o *userResourceType) deleteAllApplicationPasswordsActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserSecurityService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user security service not available - requires %s scope", admin.AdminDirectoryUserSecurityScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "delete_all_application_passwords")
	if err != nil {
		return nil, nil, err
	}

	// List all application-specific passwords (ASPs) for the user
	asps, err := o.client.ListAsps(ctx, userId)
	if err != nil {
		return nil, nil, err
	}

	// If no application passwords, return success with 0 deleted
	if len(asps.Items) == 0 {
		passwordsDeletedRv := actions.NewNumberReturnField("passwords_deleted", 0)
		return actions.NewReturnValues(true, passwordsDeletedRv), nil, nil
	}

	// Delete each application password
	passwordsDeleted := 0
	var lastErr error
	for _, asp := range asps.Items {
		err := o.client.DeleteAsp(ctx, userId, asp.CodeId)
		if err != nil {
			gerr := &googleapi.Error{}
			if errors.As(err, &gerr) {
				// If ASP was already deleted (404), continue
				if gerr.Code == http.StatusNotFound {
					l.Debug("google-workspace: application password already deleted",
						zap.String("user_id", userId),
						zap.Int64("code_id", asp.CodeId))
					passwordsDeleted++
					continue
				}
			}
			l.Error("google-workspace: failed to delete application password",
				zap.String("user_id", userId),
				zap.Int64("code_id", asp.CodeId),
				zap.String("name", asp.Name),
				zap.Error(err))
			lastErr = err
			continue
		}
		passwordsDeleted++
	}

	// If we failed to delete some application passwords, return an error
	if lastErr != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to delete some application passwords (deleted %d of %d): %w",
			passwordsDeleted, len(asps.Items), lastErr)
	}

	l.Debug("google-workspace: user action handler: deleted all application passwords",
		zap.String("user_id", userId),
		zap.Int("passwords_deleted", passwordsDeleted))

	passwordsDeletedRv := actions.NewNumberReturnField("passwords_deleted", float64(passwordsDeleted))

	return actions.NewReturnValues(true, passwordsDeletedRv), nil, nil
}

func (o *userResourceType) registerUpdateUserManagerAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, updateUserManagerActionSchema, o.updateUserManagerActionHandler)
}

func (o *userResourceType) updateUserManagerActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	// Extract user_id argument
	userId, err := extractUserId(args, l, "update_user_manager")
	if err != nil {
		return nil, nil, err
	}

	// Extract manager_email argument
	managerEmailValue, ok := args.Fields["manager_email"]
	if !ok || managerEmailValue == nil {
		l.Debug("google-workspace: user action handler: missing manager_email argument", zap.Any("args", args))
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing manager_email argument")
	}
	managerEmailField, ok := managerEmailValue.GetKind().(*structpb.Value_StringValue)
	if !ok || managerEmailField.StringValue == "" {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "invalid manager_email argument")
	}
	managerEmail := managerEmailField.StringValue

	// Validate that managerEmail is a valid email address
	if _, err := mail.ParseAddress(managerEmail); err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("invalid email address: %s", managerEmail), err)
	}

	// Get current user to check current manager
	currentUser, err := o.client.GetUserFullForProvisioning(ctx, userId)
	if err != nil {
		return nil, nil, err
	}

	// Check if already set to the target manager (idempotency)
	currentManagerEmail := extractManagerEmail(currentUser)
	if emailsEqual(currentManagerEmail, managerEmail) {
		userResource, err := o.userResource(ctx, currentUser)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to create user resource: %w", err)
		}

		resourceRv, err := actions.NewResourceReturnField("resource", userResource)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to build resource return field: %w", err)
		}

		return actions.NewReturnValues(true, resourceRv), nil, nil
	}

	// Build updated relations: keep all non-manager relations, replace/add manager
	currentRelations := extractRelations(currentUser)
	updatedRelations := make([]admin.UserRelation, 0, len(currentRelations)+1)
	for _, rel := range currentRelations {
		if rel.Type != relTypeManager {
			updatedRelations = append(updatedRelations, *rel)
		}
	}
	// Add the new manager relation
	updatedRelations = append(updatedRelations, admin.UserRelation{
		Type:  relTypeManager,
		Value: managerEmail,
	})

	// Update the user's relations
	updatedUser, err := o.client.UpdateUser(ctx, userId, &admin.User{
		Relations:       updatedRelations,
		ForceSendFields: []string{"Relations"},
	})
	if err != nil {
		return nil, nil, err
	}

	l.Debug("google-workspace: user action handler: changed manager",
		zap.String("user_id", userId),
		zap.String("old_manager", currentManagerEmail),
		zap.String("new_manager", managerEmail))

	// Create the user resource
	userResource, err := o.userResource(ctx, updatedUser)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to create user resource: %w", err)
	}

	resourceRv, err := actions.NewResourceReturnField("resource", userResource)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to build resource return field: %w", err)
	}

	return actions.NewReturnValues(true, resourceRv), nil, nil
}

func (o *userResourceType) registerUpdateUserProfileAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, updateUserProfileActionSchema, o.updateUserProfileActionHandler)
}

func (o *userResourceType) updateUserProfileActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	userId, err := extractUserId(args, l, "update_user_profile")
	if err != nil {
		return nil, nil, err
	}

	patch := userProfilePatch{}
	if _, ok := args.Fields[argGivenName]; ok {
		v := getStringField(args, argGivenName)
		patch.givenName = &v
	}
	if _, ok := args.Fields[argFamilyName]; ok {
		v := getStringField(args, argFamilyName)
		patch.familyName = &v
	}
	if _, ok := args.Fields[argRecoveryEmail]; ok {
		v := getStringField(args, argRecoveryEmail)
		patch.recoveryEmail = &v
	}
	if _, ok := args.Fields["recovery_phone"]; ok {
		v := getStringField(args, "recovery_phone")
		patch.recoveryPhone = &v
	}

	// Custom schemas: raw JSON object mapping schemaName -> { fieldName: value },
	// passed verbatim to the Directory API. Schema definitions are managed outside
	// the connector (see ticket scope).
	if raw := getStringField(args, argCustomSchemas); raw != "" {
		var schemas map[string]googleapi.RawMessage
		if err := json.Unmarshal([]byte(raw), &schemas); err != nil {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("invalid custom_schemas JSON: %v", err))
		}
		patch.customSchemas = schemas
	}

	updatedUser, updatedFields, err := applyUserProfilePatch(ctx, o.client, userId, patch)
	if err != nil {
		return nil, nil, err
	}

	l.Debug("google-workspace: user action handler: updated user profile",
		zap.String("user_id", userId),
		zap.Strings("fields", updatedFields))

	userResource, err := o.userResource(ctx, updatedUser)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to create user resource: %w", err)
	}

	resourceRv, err := actions.NewResourceReturnField("resource", userResource)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to build resource return field: %w", err)
	}

	return actions.NewReturnValues(true, resourceRv), nil, nil
}

func (o *userResourceType) registerMakeAdminAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, makeUserAdminActionSchema, o.makeAdminActionHandler)
}

func (o *userResourceType) makeAdminActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	userId, err := extractUserId(args, l, "make_admin")
	if err != nil {
		return nil, nil, err
	}

	status, ok := getBoolField(args, fieldStatus)
	if !ok {
		l.Debug("google-workspace: user action handler: missing status argument", zap.Any("args", args))
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing status argument")
	}

	err = o.client.MakeAdmin(ctx, userId, status)
	if err != nil {
		return nil, nil, err
	}

	l.Debug("google-workspace: user action handler: updated admin status",
		zap.String("user_id", userId),
		zap.Bool(fieldStatus, status))

	return actions.NewReturnValues(true), nil, nil
}

// userProfilePatch holds the optional profile fields to apply with patch
// semantics. A nil pointer leaves the field untouched; a non-nil pointer
// (including a pointer to the empty string) is sent to the API so callers can
// clear a value.
type userProfilePatch struct {
	givenName     *string
	familyName    *string
	recoveryEmail *string
	recoveryPhone *string
	customSchemas map[string]googleapi.RawMessage
}

// applyUserProfilePatch applies a partial profile update with patch semantics and
// returns the updated user plus the list of changed fields. Shared by the
// resource-scoped update_user_profile action and the global update_user action
// consumed by ConductorOne push rules.
func applyUserProfilePatch(
	ctx context.Context,
	client *gwclient.GoogleWorkspaceClient,
	userId string,
	patch userProfilePatch,
) (*admin.User, []string, error) {
	update := &admin.User{}
	forceSend := make([]string, 0)

	// Name fields. A patch replaces the whole "name" object, so read-modify-write
	// to avoid clearing the sibling field the caller did not set.
	if patch.givenName != nil || patch.familyName != nil {
		current, err := client.GetUserFullForProvisioning(ctx, userId)
		if err != nil {
			return nil, nil, err
		}
		name := &admin.UserName{}
		if current.Name != nil {
			*name = *current.Name
		}
		if patch.givenName != nil {
			name.GivenName = *patch.givenName
		}
		if patch.familyName != nil {
			name.FamilyName = *patch.familyName
		}
		// FullName is server-derived from given/family; clear it so it is recomputed.
		name.FullName = ""
		update.Name = name
		forceSend = append(forceSend, "Name")
	}

	if patch.recoveryEmail != nil {
		update.RecoveryEmail = *patch.recoveryEmail
		forceSend = append(forceSend, "RecoveryEmail")
	}
	if patch.recoveryPhone != nil {
		update.RecoveryPhone = *patch.recoveryPhone
		forceSend = append(forceSend, "RecoveryPhone")
	}
	if patch.customSchemas != nil {
		update.CustomSchemas = patch.customSchemas
	}

	if len(forceSend) == 0 && update.CustomSchemas == nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: profile update requires at least one updatable field")
	}

	update.ForceSendFields = forceSend

	updatedUser, err := client.PatchUser(ctx, userId, update)
	if err != nil {
		return nil, nil, err
	}

	updatedFields := append([]string{}, forceSend...)
	if update.CustomSchemas != nil {
		updatedFields = append(updatedFields, "CustomSchemas")
	}
	return updatedUser, updatedFields, nil
}

const (
	actionUpdateUser = "update_user"
	argUserProfile   = "user_profile"
	argUserID        = "user_id"

	argGivenName     = "given_name"
	argFamilyName    = "family_name"
	argRecoveryEmail = "recovery_email"
	argCustomSchemas = "custom_schemas"
	displayUser      = "User"
)

// updateUserGlobalActionSchema is the global (account-level) profile-update
// action consumed by ConductorOne push rules. The push-rule system discovers it
// by ActionType ACCOUNT_UPDATE_PROFILE plus the user_profile argument; the
// resource-scoped update_user_profile (typed fields) does not satisfy that
// lookup, so this global shape must exist for automated profile sync.
var updateUserGlobalActionSchema = &v2.BatonActionSchema{
	Name:        actionUpdateUser,
	DisplayName: "Update User",
	Description: "Updates a user's profile from a user_profile JSON object. " +
		"Consumed by ConductorOne push rules for automated profile sync. " +
		"Supported keys: given_name, family_name, recovery_email, recovery_phone, custom_schemas.",
	Arguments: []*config.Field{
		{
			Name:        argUserID,
			DisplayName: displayUser,
			Description: "The user to update.",
			IsRequired:  true,
			Field: &config.Field_ResourceIdField{
				ResourceIdField: &config.ResourceIdField{
					Rules: &config.ResourceIDRules{
						AllowedResourceTypeIds: []string{resourceTypeUser.Id},
					},
				},
			},
		},
		{
			Name:        argUserProfile,
			DisplayName: "User Profile Data",
			Description: "A JSON object with any of: given_name, family_name, recovery_email, recovery_phone, custom_schemas.",
			IsRequired:  true,
			Field:       &config.Field_StringField{},
		},
	},
	ReturnTypes: []*config.Field{
		{
			Name:        "success",
			DisplayName: "Success",
			Description: "Whether the user's profile was updated successfully.",
			Field:       &config.Field_BoolField{},
		},
		{
			Name:        "updated_fields",
			DisplayName: "Updated Fields",
			Description: "Comma-separated list of profile fields that were changed.",
			Field:       &config.Field_StringField{},
		},
	},
	ActionType: []v2.ActionType{
		v2.ActionType_ACTION_TYPE_ACCOUNT,
		v2.ActionType_ACTION_TYPE_ACCOUNT_UPDATE_PROFILE,
	},
}

func (c *GoogleWorkspace) updateUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	userRef, ok := actions.GetResourceIDArg(args, argUserID)
	if !ok || userRef.GetResource() == "" {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: update_user: user_id is required")
	}
	userId := userRef.GetResource()

	profileJSON, err := actions.RequireStringArg(args, argUserProfile)
	if err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("google-workspace: update_user: %v", err))
	}

	var profile map[string]any
	if err := json.Unmarshal([]byte(profileJSON), &profile); err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("google-workspace: update_user: invalid user_profile JSON: %v", err))
	}

	patch, err := profileFromJSON(profile)
	if err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("google-workspace: update_user: %v", err))
	}

	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	_, updatedFields, err := applyUserProfilePatch(ctx, client, userId, patch)
	if err != nil {
		return nil, nil, err
	}

	l.Debug("google-workspace: update_user: updated user profile",
		zap.String("user_id", userId),
		zap.Strings("fields", updatedFields))

	result, err := structpb.NewStruct(map[string]any{
		"success":        true,
		"updated_fields": strings.Join(updatedFields, ", "),
	})
	if err != nil {
		return nil, nil, uhttp.WrapErrors(codes.Internal, "google-workspace: update_user: failed to build result")
	}

	return result, nil, nil
}

// profileFromJSON maps a user_profile JSON object (snake_case or camelCase keys)
// to a userProfilePatch. Only keys present in the object are applied.
func profileFromJSON(profile map[string]any) (userProfilePatch, error) {
	patch := userProfilePatch{}
	if v, ok := stringFromJSON(profile, argGivenName, "givenName"); ok {
		patch.givenName = &v
	}
	if v, ok := stringFromJSON(profile, argFamilyName, "familyName"); ok {
		patch.familyName = &v
	}
	if v, ok := stringFromJSON(profile, argRecoveryEmail, "recoveryEmail"); ok {
		patch.recoveryEmail = &v
	}
	if v, ok := stringFromJSON(profile, "recovery_phone", "recoveryPhone"); ok {
		patch.recoveryPhone = &v
	}
	if raw, ok := profile[argCustomSchemas]; ok {
		m, ok := raw.(map[string]any)
		if !ok {
			return patch, fmt.Errorf("custom_schemas must be a JSON object")
		}
		schemas := make(map[string]googleapi.RawMessage, len(m))
		for k, val := range m {
			b, err := json.Marshal(val)
			if err != nil {
				return patch, fmt.Errorf("invalid custom_schemas value for %q: %w", k, err)
			}
			schemas[k] = b
		}
		if len(schemas) > 0 {
			patch.customSchemas = schemas
		}
	}
	return patch, nil
}

// stringFromJSON returns the first key present in profile whose value is a string.
func stringFromJSON(profile map[string]any, keys ...string) (string, bool) {
	for _, k := range keys {
		if v, ok := profile[k]; ok {
			if s, ok := v.(string); ok {
				return s, true
			}
		}
	}
	return "", false
}
