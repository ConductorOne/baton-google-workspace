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
	createGroupActionSchema = &v2.BatonActionSchema{
		Name:        "create_group",
		DisplayName: "Create Group",
		Description: "Creates a new Google Workspace group. The group email must be unique within the domain.",
		Arguments: []*config.Field{
			{
				Name:        "email",
				DisplayName: "Group Email",
				Description: "The group's email address. Must be unique within the domain. Group email addresses are subject to the same character usage rules as usernames.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "name",
				DisplayName: "Group Name",
				Description: "The group's display name.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "description",
				DisplayName: "Description",
				Description: "An extended description to help users determine the purpose of a group. Maximum length is 4,096 characters.",
				Field:       &config.Field_StringField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the group was created successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "resource",
				DisplayName: "Created Group",
				Description: "The created group resource.",
				Field:       &config.Field_ResourceField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_RESOURCE_CREATE},
	}
)

// ResourceActions implements the ResourceActionProvider interface for group resource actions.
func (o *groupResourceType) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := o.registerCreateGroupAction(ctx, registry); err != nil {
		return err
	}
	return nil
}

func (o *groupResourceType) registerCreateGroupAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, createGroupActionSchema, o.createGroupActionHandler)
}

func (o *groupResourceType) createGroupActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	if o.groupProvisioningService == nil {
		return nil, nil, fmt.Errorf("google-workspace: group provisioning service not available - requires %s scope", admin.AdminDirectoryGroupScope)
	}

	// Extract email argument
	emailValue, ok := args.Fields["email"]
	if !ok || emailValue == nil {
		l.Debug("google-workspace: group action handler: missing email argument", zap.Any("args", args))
		return nil, nil, fmt.Errorf("missing email argument")
	}
	emailField, ok := emailValue.GetKind().(*structpb.Value_StringValue)
	if !ok || emailField.StringValue == "" {
		return nil, nil, fmt.Errorf("invalid email argument")
	}
	email := emailField.StringValue

	// Extract name argument
	nameValue, ok := args.Fields["name"]
	if !ok || nameValue == nil {
		l.Debug("google-workspace: group action handler: missing name argument", zap.Any("args", args))
		return nil, nil, fmt.Errorf("missing name argument")
	}
	nameField, ok := nameValue.GetKind().(*structpb.Value_StringValue)
	if !ok || nameField.StringValue == "" {
		return nil, nil, fmt.Errorf("invalid name argument")
	}
	name := nameField.StringValue

	// Extract description argument (optional)
	var description string
	if descValue, ok := args.Fields["description"]; ok && descValue != nil {
		if descField, ok := descValue.GetKind().(*structpb.Value_StringValue); ok {
			description = descField.StringValue
		}
	}

	// Create the group
	group := &admin.Group{
		Email:       email,
		Name:        name,
		Description: description,
	}
	l.Debug("google-workspace: group action handler: create group input", zap.Any("group", group))
	createdGroup, err := o.groupProvisioningService.Groups.Insert(group).Context(ctx).Do()
	if err != nil {
		// Check if it's a 403 Forbidden error and enhance the message
		gerr := &googleapi.Error{}
		if errors.As(err, &gerr) && gerr.Code == http.StatusForbidden {
			l.Debug("google-workspace: group action handler: 403 Forbidden error", zap.Any("error", err))
			return nil, nil, fmt.Errorf(
				"google-workspace: failed to create group (403 Forbidden). "+
					"This may be due to: 1) missing OAuth scope %s, "+
					"2) insufficient admin permissions, or "+
					"3) invalid email domain '%s' (domain must be verified in Google Workspace): %w",
				admin.AdminDirectoryGroupScope, email, err)
		}
		return nil, nil, fmt.Errorf("google-workspace: failed to create group: %w", err)
	}
	l.Debug("google-workspace: group action handler: created group", zap.Any("createdGroup", createdGroup))
	// Create the group resource
	resource, err := groupToResource(ctx, createdGroup)
	if err != nil {
		l := ctxzap.Extract(ctx)
		l.Error("failed to create group resource", zap.Error(err))
		return nil, nil, fmt.Errorf("google-workspace: failed to create group resource: %w", err)
	}

	resourceRv, err := actions.NewResourceReturnField("resource", resource)
	if err != nil {
		l := ctxzap.Extract(ctx)
		l.Error("failed to build resource return field", zap.Error(err))
		return nil, nil, err
	}

	return actions.NewReturnValues(true, resourceRv), nil, nil
}
