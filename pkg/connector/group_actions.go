package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	groupssettings "google.golang.org/api/groupssettings/v1"
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

var (
	modifyGroupSettingsActionSchema = &v2.BatonActionSchema{
		Name:        "modify_group_settings",
		DisplayName: "Modify Group Settings",
		Description: "Update settings for an existing Google Group.",
		Arguments: []*config.Field{
			{
				Name:        "group_key",
				DisplayName: "Group Key",
				Description: "Email address or unique ID of the group to modify.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "allow_external_members",
				DisplayName: "Allow External Members",
				Description: "If true, allows external members to join the group. Defaults to false.",
				Field:       &config.Field_BoolField{},
				IsRequired:  false,
			},
			{
				Name:        "allow_web_posting",
				DisplayName: "Allow Web Posting",
				Description: "If true, allows posting via email from external (non-member) addresses. Defaults to false.",
				Field:       &config.Field_BoolField{},
				IsRequired:  false,
			},
			{
				Name:        "who_can_post_message",
				DisplayName: "Who Can Post Messages",
				Description: "Control who can post messages.",
				Field: &config.Field_StringField{
					StringField: &config.StringField{
						Rules: &config.StringRules{
							In: []string{
								"ANYONE_CAN_POST",
								"ALL_MEMBERS_CAN_POST",
								"ALL_MANAGERS_CAN_POST",
								"ALL_OWNERS_CAN_POST",
								"NONE_CAN_POST",
								"ALL_IN_DOMAIN_CAN_POST",
							},
						},
					},
				},
				IsRequired: false,
			},
			{
				Name:        "message_moderation_level",
				DisplayName: "Message Moderation Level",
				Description: "Control moderation.",
				Field: &config.Field_StringField{
					StringField: &config.StringField{
						Rules: &config.StringRules{
							In: []string{
								"MODERATE_NONE",
								"MODERATE_NON_MEMBERS",
								"MODERATE_ALL_MESSAGES",
								"MODERATE_NEW_MEMBERS",
							},
						},
					},
				},
				IsRequired: false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        "success",
				DisplayName: "Success",
				Description: "Whether the settings were updated successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "group_email",
				DisplayName: "Group Email",
				Description: "Email address of the group.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "settings_updated",
				DisplayName: "Settings Updated",
				Description: "Whether any settings were changed.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "previous_allow_external_members",
				DisplayName: "Previous Allow External Members",
				Description: "Previous value of allow_external_members setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "new_allow_external_members",
				DisplayName: "New Allow External Members",
				Description: "New value of allow_external_members setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "previous_allow_web_posting",
				DisplayName: "Previous Allow Web Posting",
				Description: "Previous value of allow_web_posting setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "new_allow_web_posting",
				DisplayName: "New Allow Web Posting",
				Description: "New value of allow_web_posting setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "previous_who_can_post_message",
				DisplayName: "Previous Who Can Post Messages",
				Description: "Previous value of who_can_post_message setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "new_who_can_post_message",
				DisplayName: "New Who Can Post Messages",
				Description: "New value of who_can_post_message setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "previous_message_moderation_level",
				DisplayName: "Previous Message Moderation Level",
				Description: "Previous value of message_moderation_level setting.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "new_message_moderation_level",
				DisplayName: "New Message Moderation Level",
				Description: "New value of message_moderation_level setting.",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_UNSPECIFIED},
	}
)

// ResourceActions implements the ResourceActionProvider interface for group resource actions.
func (o *groupResourceType) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := o.registerCreateGroupAction(ctx, registry); err != nil {
		return err
	}
	if err := o.registerModifyGroupSettingsAction(ctx, registry); err != nil {
		return err
	}
	return nil
}

func (o *groupResourceType) registerCreateGroupAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, createGroupActionSchema, o.createGroupActionHandler)
}

func (o *groupResourceType) registerModifyGroupSettingsAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, modifyGroupSettingsActionSchema, o.modifyGroupSettingsActionHandler)
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

// applyGroupSettingsWithTracking applies group settings and returns what changed.
// Returns (settingsUpdated bool, previousSettings map, newSettings map, error).
func (o *groupResourceType) applyGroupSettingsWithTracking(
	ctx context.Context,
	groupEmail string,
	allowExternalMembers bool,
	allowWebPosting bool,
	whoCanPostMessage string,
	messageModerationLevel string,
	hasAllowExternal bool,
	hasAllowWebPosting bool,
) (bool, map[string]string, map[string]string, error) {
	if o.groupsSettingsService == nil {
		return false, nil, nil, fmt.Errorf("group settings service not available")
	}
	previousSettings := make(map[string]string)
	newSettings := make(map[string]string)

	// Fetch current settings for idempotency check
	currentSettings, err := o.groupsSettingsService.Groups.Get(groupEmail).Context(ctx).Do()
	if err != nil {
		return false, nil, nil, err
	}

	// Check if any updates are needed
	needsUpdate := false
	updatedSettings := &groupssettings.Groups{}

	// Apply boolean settings
	if hasAllowExternal {
		result := applyBooleanGroupSetting(
			currentSettings.AllowExternalMembers,
			allowExternalMembers,
			"AllowExternalMembers",
		)
		previousSettings["allow_external_members"] = result.PreviousValue
		newSettings["allow_external_members"] = result.NewValue
		if result.NeedsUpdate {
			needsUpdate = true
			updatedSettings.AllowExternalMembers = result.NewValue
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, result.ForceSendField)
		}
	}

	if hasAllowWebPosting {
		result := applyBooleanGroupSetting(
			currentSettings.AllowWebPosting,
			allowWebPosting,
			"AllowWebPosting",
		)
		previousSettings["allow_web_posting"] = result.PreviousValue
		newSettings["allow_web_posting"] = result.NewValue
		if result.NeedsUpdate {
			needsUpdate = true
			updatedSettings.AllowWebPosting = result.NewValue
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, result.ForceSendField)
		}
	}

	// Apply string settings
	if whoCanPostMessage != "" {
		result := applyStringGroupSetting(
			currentSettings.WhoCanPostMessage,
			whoCanPostMessage,
			"WhoCanPostMessage",
		)
		previousSettings["who_can_post_message"] = result.PreviousValue
		newSettings["who_can_post_message"] = result.NewValue
		if result.NeedsUpdate {
			needsUpdate = true
			updatedSettings.WhoCanPostMessage = result.NewValue
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, result.ForceSendField)
		}
	}

	if messageModerationLevel != "" {
		result := applyStringGroupSetting(
			currentSettings.MessageModerationLevel,
			messageModerationLevel,
			"MessageModerationLevel",
		)
		previousSettings["message_moderation_level"] = result.PreviousValue
		newSettings["message_moderation_level"] = result.NewValue
		if result.NeedsUpdate {
			needsUpdate = true
			updatedSettings.MessageModerationLevel = result.NewValue
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, result.ForceSendField)
		}
	}

	// If no updates needed, return success (idempotent)
	if !needsUpdate {
		return false, previousSettings, newSettings, nil
	}

	// Update settings
	_, err = o.groupsSettingsService.Groups.Patch(groupEmail, updatedSettings).Context(ctx).Do()
	if err != nil {
		return false, previousSettings, newSettings, err
	}

	return true, previousSettings, newSettings, nil
}

// modifyGroupSettingsActionHandler updates settings for an existing Google Group (idempotent: checks current settings before updating).
func (o *groupResourceType) modifyGroupSettingsActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	// Extract and validate group_key parameter
	groupKeyField, ok := args.Fields["group_key"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing group_key")
	}

	groupKey := strings.TrimSpace(groupKeyField.StringValue)
	if groupKey == "" {
		return nil, nil, fmt.Errorf("group_key must be non-empty")
	}

	// Extract optional settings parameters
	allowExternalMembers, hasAllowExternal := getBoolField(args, "allow_external_members")
	allowWebPosting, hasAllowWebPosting := getBoolField(args, "allow_web_posting")
	whoCanPostMessage := getStringField(args, "who_can_post_message")
	messageModerationLevel := getStringField(args, "message_moderation_level")

	// Check if at least one setting parameter is provided
	if !hasAllowExternal && !hasAllowWebPosting && whoCanPostMessage == "" && messageModerationLevel == "" {
		return nil, nil, fmt.Errorf("at least one settings parameter must be provided")
	}

	// Validate settings values if provided
	if whoCanPostMessage != "" {
		validWhoCanPost := map[string]bool{
			"ANYONE_CAN_POST":        true,
			"ALL_MEMBERS_CAN_POST":   true,
			"ALL_MANAGERS_CAN_POST":  true,
			"ALL_OWNERS_CAN_POST":    true,
			"NONE_CAN_POST":          true,
			"ALL_IN_DOMAIN_CAN_POST": true,
		}
		if !validWhoCanPost[whoCanPostMessage] {
			return nil, nil, fmt.Errorf("invalid who_can_post_message value '%s': must be one of ANYONE_CAN_POST, ALL_MEMBERS_CAN_POST, ALL_MANAGERS_CAN_POST, ALL_OWNERS_CAN_POST, NONE_CAN_POST, ALL_IN_DOMAIN_CAN_POST", whoCanPostMessage)
		}
	}

	if messageModerationLevel != "" {
		validModeration := map[string]bool{
			"MODERATE_NONE":         true,
			"MODERATE_NON_MEMBERS":  true,
			"MODERATE_ALL_MESSAGES": true,
			"MODERATE_NEW_MEMBERS":  true,
		}
		if !validModeration[messageModerationLevel] {
			return nil, nil, fmt.Errorf("invalid message_moderation_level value '%s': must be one of MODERATE_NONE, MODERATE_NON_MEMBERS, MODERATE_ALL_MESSAGES, MODERATE_NEW_MEMBERS", messageModerationLevel)
		}
	}

	// Verify group exists and get its email
	group, err := o.groupService.Groups.Get(groupKey).Context(ctx).Do()
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				return nil, nil, fmt.Errorf("group not found: %s", groupKey)
			}
		}
		return nil, nil, err
	}

	// Apply settings with tracking
	settingsUpdated, previousSettings, newSettings, err := o.applyGroupSettingsWithTracking(
		ctx,
		group.Email,
		allowExternalMembers,
		allowWebPosting,
		whoCanPostMessage,
		messageModerationLevel,
		hasAllowExternal,
		hasAllowWebPosting,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update group settings: %w", err)
	}

	// Build response with previous and new values
	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success":          {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"group_email":      {Kind: &structpb.Value_StringValue{StringValue: group.Email}},
		"settings_updated": {Kind: &structpb.Value_BoolValue{BoolValue: settingsUpdated}},
	}}

	// Add previous and new values for settings that were provided
	settingNames := []string{
		"allow_external_members",
		"allow_web_posting",
		"who_can_post_message",
		"message_moderation_level",
	}

	for _, settingName := range settingNames {
		if prevVal, ok := previousSettings[settingName]; ok {
			response.Fields["previous_"+settingName] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: prevVal}}
		}
		if newVal, ok := newSettings[settingName]; ok {
			response.Fields["new_"+settingName] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: newVal}}
		}
	}

	return &response, nil, nil
}
