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
				Name:        argManagerEmail,
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
			"Supports name fields, recovery details, Employee Information attributes " +
			"(department, job title, cost center, employee ID, employee type), the manager relation, " +
			"and custom-schema attribute values. At least one updatable field must be provided.",
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
				Name:        argRecoveryPhone,
				DisplayName: "Recovery Phone",
				Description: "New recovery phone (E.164, e.g. +14155550100). Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argDepartment,
				DisplayName: "Department",
				Description: "New department. Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argJobTitle,
				DisplayName: "Job Title",
				Description: "New job title. Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argCostCenter,
				DisplayName: "Cost Center",
				Description: "New cost center. Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argEmployeeType,
				DisplayName: "Employee Type",
				Description: "New employee type. Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argEmployeeID,
				DisplayName: "Employee ID",
				Description: "New employee ID. Send an empty string to clear it.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
			{
				Name:        argManagerEmail,
				DisplayName: "Manager Email",
				Description: "The email address of the new manager. An empty value is ignored (not applied); " +
					"use update_user_manager to remove the manager relation entirely.",
				Field:      &config.Field_StringField{},
				IsRequired: false,
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
	managerEmailValue, ok := args.Fields[argManagerEmail]
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
	updatedRelations := buildManagerRelations(currentRelations, managerEmail)

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
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope))
	}

	userId, err := extractUserId(args, l, "update_user_profile")
	if err != nil {
		return nil, nil, err
	}

	patch := userProfilePatch{
		givenName:     optionalStringField(args, argGivenName),
		familyName:    optionalStringField(args, argFamilyName),
		recoveryEmail: optionalStringField(args, argRecoveryEmail),
		recoveryPhone: optionalStringField(args, argRecoveryPhone),
		department:    optionalStringField(args, argDepartment),
		jobTitle:      optionalStringField(args, argJobTitle),
		costCenter:    optionalStringField(args, argCostCenter),
		employeeType:  optionalStringField(args, argEmployeeType),
		employeeID:    optionalStringField(args, argEmployeeID),
		managerEmail:  optionalStringField(args, argManagerEmail),
	}

	// Custom schemas: raw JSON object mapping schemaName -> { fieldName: value },
	// passed verbatim to the Directory API. Schema definitions are managed outside
	// the connector (see ticket scope).
	if raw := getStringField(args, argCustomSchemas); raw != "" {
		var schemas map[string]googleapi.RawMessage
		if err := json.Unmarshal([]byte(raw), &schemas); err != nil {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: update_user_profile: invalid custom_schemas JSON", err)
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

// makeAdminActionHandler promotes (status=true) or demotes (status=false) a user
// to/from super admin. It is idempotent without a pre-read: Google's
// users.makeAdmin is a state-set (not a toggle) and returns 2xx when the user is
// already in the target admin state (verified against a live tenant). Skipping
// the GET that enable_user/disable_user perform avoids an extra API call and a
// TOCTOU window on every invocation.
func (o *userResourceType) makeAdminActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.client.UserProvisioningService == nil {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope))
	}

	userId, err := extractUserId(args, l, "make_admin")
	if err != nil {
		return nil, nil, err
	}

	status, ok := getBoolField(args, fieldStatus)
	if !ok {
		l.Debug("google-workspace: user action handler: missing status argument", zap.Any("args", args))
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: make_admin: missing status argument")
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
// clear a value. Exceptions: the name fields (givenName/familyName) and
// managerEmail ignore empty strings and are treated as "not provided" —
// Google rejects empty names, and this patch path has no way to clear the
// manager relation, so an empty value must not fail the whole request.
type userProfilePatch struct {
	givenName     *string
	familyName    *string
	recoveryEmail *string
	recoveryPhone *string
	department    *string
	jobTitle      *string
	costCenter    *string
	employeeType  *string
	employeeID    *string
	managerEmail  *string
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
	// to avoid clearing the sibling field the caller did not set. Unlike the
	// recovery fields (where empty means "clear"), an empty name value is treated
	// as "not provided": Google rejects empty given/family names and blanking a
	// name is never an intended outcome, so empty values are ignored.
	setGiven := patch.givenName != nil && *patch.givenName != ""
	setFamily := patch.familyName != nil && *patch.familyName != ""
	setOrg := patch.department != nil || patch.jobTitle != nil || patch.costCenter != nil || patch.employeeType != nil
	// Unlike recovery fields (empty means "clear"), manager_email has no clear
	// path through this action - see the Relations block below - so an empty
	// value is treated the same as "not provided", matching the name fields'
	// convention, rather than failing the whole multi-field patch.
	setManagerEmail := patch.managerEmail != nil && *patch.managerEmail != ""
	// Validate before the read-modify-write GET below so a malformed address
	// fails fast without burning an API call.
	if setManagerEmail {
		if _, err := mail.ParseAddress(*patch.managerEmail); err != nil {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument,
				fmt.Sprintf("google-workspace: invalid manager_email: %s", *patch.managerEmail), err)
		}
	}

	// Organizations, ExternalIds, and Relations are array fields: a patch
	// replaces the whole array, so a GET is required first to preserve sibling
	// entries (other organizations, other external-ID types, other relation
	// types) the caller did not set. Fetch once and reuse across all three
	// blocks below, plus the Name block, instead of issuing a GET per field.
	needCurrent := (setGiven != setFamily) || setOrg || patch.employeeID != nil || setManagerEmail
	var current *admin.User
	if needCurrent {
		var err error
		current, err = client.GetUserFullForProvisioning(ctx, userId)
		if err != nil {
			return nil, nil, err
		}
	}

	if setGiven || setFamily {
		name := &admin.UserName{}
		// Read-modify-write only when exactly one name field is provided, to
		// preserve the sibling field the caller did not set. When both are
		// supplied the whole name object is overwritten.
		if setGiven != setFamily && current.Name != nil {
			*name = *current.Name
		}
		if setGiven {
			name.GivenName = *patch.givenName
		}
		if setFamily {
			name.FamilyName = *patch.familyName
		}
		// FullName is server-derived from given/family; clear it so it is recomputed.
		name.FullName = ""
		update.Name = name
		forceSend = append(forceSend, "Name")
	}

	if patch.recoveryEmail != nil {
		// Empty string is a legitimate "clear" request; only validate non-empty values.
		if *patch.recoveryEmail != "" {
			if _, err := mail.ParseAddress(*patch.recoveryEmail); err != nil {
				return nil, nil, uhttp.WrapErrors(codes.InvalidArgument,
					fmt.Sprintf("google-workspace: invalid recovery_email: %s", *patch.recoveryEmail), err)
			}
		}
		update.RecoveryEmail = *patch.recoveryEmail
		forceSend = append(forceSend, "RecoveryEmail")
	}
	if patch.recoveryPhone != nil {
		update.RecoveryPhone = *patch.recoveryPhone
		forceSend = append(forceSend, "RecoveryPhone")
	}

	// Employee Information: Department, Job title, and Cost center live on the
	// primary entry of the Organizations array; Employee type maps to that same
	// entry's Description field (per Admin console mapping). Preserve secondary
	// organizations and any sibling fields on the primary entry the caller did
	// not set.
	if setOrg {
		orgs, err := extractFromInterface[*admin.UserOrganization](current.Organizations)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to parse organizations: %w", err)
		}
		update.Organizations = buildUpdatedOrganizations(orgs, patch)
		forceSend = append(forceSend, "Organizations")
	}

	// Employee ID: the Admin console's "Employee ID" is the ExternalIds entry
	// with Type "organization" (an oddly-named but stable API mapping). Preserve
	// any other ExternalIds entries (account/login_id/network, etc.).
	if patch.employeeID != nil {
		currentExtIDs, err := extractFromInterface[*admin.UserExternalId](current.ExternalIds)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to parse external ids: %w", err)
		}
		update.ExternalIds = buildUpdatedExternalIDs(currentExtIDs, *patch.employeeID)
		forceSend = append(forceSend, "ExternalIds")
	}

	// Manager: same "manager" Relations entry the standalone update_user_manager
	// action writes - buildManagerRelations is shared with that handler so the
	// two stay behaviorally identical. Unlike the recovery fields, an empty
	// value means "not provided" rather than "clear" - clearing the manager
	// relation is out of scope for this bulk patch path, so it is skipped (like
	// the name fields) rather than failing the request.
	if setManagerEmail {
		currentRelations, err := extractFromInterface[*admin.UserRelation](current.Relations)
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to parse relations: %w", err)
		}
		update.Relations = buildManagerRelations(currentRelations, *patch.managerEmail)
		forceSend = append(forceSend, "Relations")
	}

	// Only treat custom schemas as a real update when non-empty. An empty object
	// ("{}") unmarshals to a non-nil empty map; assigning it would pass the
	// "at least one updatable field" guard and issue a no-op patch that falsely
	// reports CustomSchemas as changed.
	if len(patch.customSchemas) > 0 {
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

// buildUpdatedOrganizations merges the requested department/job title/cost
// center/employee type changes into the current primary organization,
// preserving secondary organizations and any sibling fields on the primary
// entry the caller did not set. Google does not guarantee a Primary flag is
// set (e.g. accounts provisioned via GCDS or third-party sync); this mirrors
// the read path's extractPrimaryOrganizations fallback of orgs[0] so an
// existing organization is updated in place instead of appending a second one
// that silently orphans its sibling fields.
func buildUpdatedOrganizations(orgs []*admin.UserOrganization, patch userProfilePatch) []admin.UserOrganization {
	primaryIdx := -1
	for i, org := range orgs {
		if org.Primary {
			primaryIdx = i
			break
		}
	}
	if primaryIdx < 0 && len(orgs) > 0 {
		primaryIdx = 0
	}
	primary := &admin.UserOrganization{}
	if primaryIdx >= 0 {
		*primary = *orgs[primaryIdx]
	}
	primary.Primary = true
	if patch.department != nil {
		primary.Department = *patch.department
		primary.ForceSendFields = append(primary.ForceSendFields, "Department")
	}
	if patch.jobTitle != nil {
		primary.Title = *patch.jobTitle
		primary.ForceSendFields = append(primary.ForceSendFields, "Title")
	}
	if patch.costCenter != nil {
		primary.CostCenter = *patch.costCenter
		primary.ForceSendFields = append(primary.ForceSendFields, "CostCenter")
	}
	if patch.employeeType != nil {
		primary.Description = *patch.employeeType
		primary.ForceSendFields = append(primary.ForceSendFields, "Description")
	}
	updatedOrgs := make([]admin.UserOrganization, 0, len(orgs)+1)
	for i, org := range orgs {
		if i == primaryIdx {
			updatedOrgs = append(updatedOrgs, *primary)
		} else {
			updatedOrgs = append(updatedOrgs, *org)
		}
	}
	if primaryIdx < 0 {
		updatedOrgs = append(updatedOrgs, *primary)
	}
	return updatedOrgs
}

// buildUpdatedExternalIDs sets the ExternalIds entry with Type "organization"
// (the Admin console's "Employee ID") to employeeID, removing it entirely when
// employeeID is empty. Other ExternalIds entries (account/login_id/network,
// etc.) are preserved.
func buildUpdatedExternalIDs(ids []*admin.UserExternalId, employeeID string) []admin.UserExternalId {
	updated := make([]admin.UserExternalId, 0, len(ids)+1)
	for _, id := range ids {
		if id.Type != externalIDTypeOrganization {
			updated = append(updated, *id)
		}
	}
	if employeeID != "" {
		updated = append(updated, admin.UserExternalId{
			Type:  externalIDTypeOrganization,
			Value: employeeID,
		})
	}
	return updated
}

// buildManagerRelations sets the "manager" Relations entry to managerEmail,
// preserving any other relation types. Shared by the standalone
// update_user_manager action and the bulk profile-patch path so the two stay
// behaviorally identical.
func buildManagerRelations(relations []*admin.UserRelation, managerEmail string) []admin.UserRelation {
	updated := make([]admin.UserRelation, 0, len(relations)+1)
	for _, rel := range relations {
		if rel.Type != relTypeManager {
			updated = append(updated, *rel)
		}
	}
	updated = append(updated, admin.UserRelation{
		Type:  relTypeManager,
		Value: managerEmail,
	})
	return updated
}

const (
	actionUpdateUser = "update_user"
	argUserProfile   = "user_profile"
	argUserID        = "user_id"

	argGivenName     = "given_name"
	argFamilyName    = "family_name"
	argRecoveryEmail = "recovery_email"
	argRecoveryPhone = "recovery_phone"
	argCustomSchemas = "custom_schemas"
	argDepartment    = "department"
	argJobTitle      = "job_title"
	argCostCenter    = "cost_center"
	argEmployeeType  = "employee_type"
	argEmployeeID    = "employee_id"
	argManagerEmail  = "manager_email"
	displayUser      = "User"

	// externalIDTypeOrganization is the admin.UserExternalId.Type value the
	// Admin console's "Employee ID" field is stored under.
	externalIDTypeOrganization = "organization"
	// externalIDTypeLoginID is one of the ExternalId types read as an
	// additional login during sync (see userResource in user.go).
	externalIDTypeLoginID = "login_id"
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
		"Supported keys: given_name, family_name, recovery_email, recovery_phone, department, job_title, " +
		"cost_center, employee_type, employee_id, manager_email, custom_schemas.",
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
			Description: "A JSON object with any of: given_name, family_name, recovery_email, recovery_phone, " +
				"department, job_title, cost_center, employee_type, employee_id, manager_email, custom_schemas.",
			IsRequired: true,
			Field:      &config.Field_StringField{},
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

// updateUserActionHandler backs the global update_user action. It lives here,
// next to the resource-scoped update_user_profile handler, rather than in
// actions.go: both share the unexported applyUserProfilePatch/profileFromJSON
// helpers, and colocating them avoids exporting those helpers just for placement.
func (c *GoogleWorkspace) updateUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	userRef, ok := actions.GetResourceIDArg(args, argUserID)
	if !ok || userRef.GetResource() == "" {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: update_user: user_id is required")
	}
	userId := userRef.GetResource()

	profileJSON, err := actions.RequireStringArg(args, argUserProfile)
	if err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: update_user: user_profile is required", err)
	}

	var profile map[string]any
	if err := json.Unmarshal([]byte(profileJSON), &profile); err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: update_user: invalid user_profile JSON", err)
	}

	patch, err := profileFromJSON(profile)
	if err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: update_user: invalid user_profile", err)
	}

	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	if client.UserProvisioningService == nil {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition,
			fmt.Sprintf("google-workspace: update_user: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope))
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
	if v, ok := stringFromJSON(profile, argRecoveryPhone, "recoveryPhone"); ok {
		patch.recoveryPhone = &v
	}
	if v, ok := stringFromJSON(profile, argDepartment, "department"); ok {
		patch.department = &v
	}
	if v, ok := stringFromJSON(profile, argJobTitle, "jobTitle"); ok {
		patch.jobTitle = &v
	}
	if v, ok := stringFromJSON(profile, argCostCenter, "costCenter"); ok {
		patch.costCenter = &v
	}
	if v, ok := stringFromJSON(profile, argEmployeeType, "employeeType"); ok {
		patch.employeeType = &v
	}
	if v, ok := stringFromJSON(profile, argEmployeeID, "employeeId"); ok {
		patch.employeeID = &v
	}
	if v, ok := stringFromJSON(profile, argManagerEmail, "managerEmail"); ok {
		patch.managerEmail = &v
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
