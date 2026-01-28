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
)

// ResourceActions implements the ResourceActionProvider interface for user resource actions.
func (o *userResourceType) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := o.registerChangeUserOrgUnitAction(ctx, registry); err != nil {
		return err
	}
	return nil
}

func (o *userResourceType) registerChangeUserOrgUnitAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, changeUserOrgUnitActionSchema, o.changeUserOrgUnitActionHandler)
}

func (o *userResourceType) changeUserOrgUnitActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.userProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	// Extract user_id argument
	userIdValue, ok := args.Fields["user_id"]
	if !ok || userIdValue == nil {
		l.Debug("google-workspace: user action handler: missing user_id argument", zap.Any("args", args))
		return nil, nil, fmt.Errorf("missing user_id argument")
	}
	userIdField, ok := userIdValue.GetKind().(*structpb.Value_StringValue)
	if !ok || userIdField.StringValue == "" {
		return nil, nil, fmt.Errorf("invalid user_id argument")
	}
	userId := userIdField.StringValue

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
