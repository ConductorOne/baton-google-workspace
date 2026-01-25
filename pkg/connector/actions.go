package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// https://developers.google.com/workspace/admin/data-transfer/v1/parameters
const (
	appIdGoogleDocsAndGoogleDrive = int64(55656082996)
	appIdGoogleCalendar           = int64(435070579839)
)

func (c *GoogleWorkspace) updateUserStatus(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields["resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing resource ID")
	}

	isSuspendedField, ok := args.Fields["is_suspended"].GetKind().(*structpb.Value_BoolValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing is_suspended")
	}

	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		return nil, nil, err
	}

	isSuspended := isSuspendedField.BoolValue

	userId := guidField.StringValue

	// update user.isSuspended state
	_, err = userService.Users.Update(userId, &directoryAdmin.User{
		Suspended:       isSuspended,
		ForceSendFields: []string{"Suspended"},
	}).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": {
				Kind: &structpb.Value_BoolValue{BoolValue: true},
			},
		},
	}

	return &response, nil, nil
}

// disableUserActionHandler suspends a user (idempotent: if already suspended, returns success).
func (c *GoogleWorkspace) disableUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields["user_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing user ID")
	}

	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		return nil, nil, err
	}

	userId := guidField.StringValue

	// fetch current to ensure idempotency
	u, err := userService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}
	if u.Suspended { // already suspended
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	_, err = userService.Users.Update(
		userId,
		&directoryAdmin.User{
			Suspended:       true,
			ForceSendFields: []string{"Suspended"},
		},
	).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	return &response, nil, nil
}

// enableUserActionHandler unsuspends a user (idempotent: if already active, returns success).
func (c *GoogleWorkspace) enableUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields["user_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing user ID")
	}

	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		return nil, nil, err
	}

	userId := guidField.StringValue

	// fetch current to ensure idempotency
	u, err := userService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}
	if !u.Suspended { // already active
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	_, err = userService.Users.Update(
		userId,
		&directoryAdmin.User{
			Suspended:       false,
			ForceSendFields: []string{"Suspended"}, // This is needed becasuse the SDK would omit any field that has the field type default value (false).
		},
	).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	return &response, nil, nil
}

// changeUserPrimaryEmail updates a user's primary email.
func (c *GoogleWorkspace) changeUserPrimaryEmail(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields["resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing resource ID")
	}
	newEmailField, ok := args.Fields["new_primary_email"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing new_primary_email")
	}

	userId := guidField.StringValue
	newPrimary := newEmailField.StringValue

	// Validate that newPrimary is a valid email address
	if _, err := mail.ParseAddress(newPrimary); err != nil {
		return nil, nil, fmt.Errorf("invalid email address '%s': %w", newPrimary, err)
	}

	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		return nil, nil, err
	}

	// fetch current for return payload
	u, err := userService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}
	prev := u.PrimaryEmail
	if emailsEqual(prev, newPrimary) { // Already primary email
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success":                {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			"previous_primary_email": {Kind: &structpb.Value_StringValue{StringValue: prev}},
			"new_primary_email":      {Kind: &structpb.Value_StringValue{StringValue: newPrimary}},
		}}
		return &response, nil, nil
	}

	_, err = userService.Users.Update(
		userId,
		&directoryAdmin.User{
			PrimaryEmail:    newPrimary,
			ForceSendFields: []string{"PrimaryEmail"},
		},
	).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success":                {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"previous_primary_email": {Kind: &structpb.Value_StringValue{StringValue: prev}},
		"new_primary_email":      {Kind: &structpb.Value_StringValue{StringValue: newPrimary}},
	}}
	return &response, nil, nil
}

// transferUserDriveFiles initiates a Drive ownership transfer using Data Transfer API.
func (c *GoogleWorkspace) transferUserDriveFiles(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	sourceField, ok := args.Fields["resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing resource_id")
	}
	targetField, ok := args.Fields["target_resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing target_resource_id")
	}

	// Validate non-empty and different user keys
	src := strings.TrimSpace(sourceField.StringValue)
	dst := strings.TrimSpace(targetField.StringValue)
	if src == "" || dst == "" {
		return nil, nil, fmt.Errorf("resource_id and target_resource_id must be non-empty")
	}
	if strings.EqualFold(src, dst) {
		return nil, nil, fmt.Errorf("resource_id and target_resource_id must be different")
	}

	// Build Drive params from privacy_levels
	params := []*datatransferAdmin.ApplicationTransferParam{}
	levels, err := parseDrivePrivacyLevels(args)
	if err != nil {
		return nil, nil, err
	}
	params = append(params, &datatransferAdmin.ApplicationTransferParam{Key: "PRIVACY_LEVEL", Value: levels})

	return c.dataTransferInsert(ctx, appIdGoogleDocsAndGoogleDrive, sourceField.StringValue, targetField.StringValue, params)
}

// transferUserCalendar initiates a Calendar transfer using Data Transfer API.
func (c *GoogleWorkspace) transferUserCalendar(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	sourceField, ok := args.Fields["resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing resource_id")
	}
	targetField, ok := args.Fields["target_resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing target_resource_id")
	}

	// Validate non-empty and different user keys
	src := strings.TrimSpace(sourceField.StringValue)
	dst := strings.TrimSpace(targetField.StringValue)
	if src == "" || dst == "" {
		return nil, nil, fmt.Errorf("resource_id and target_resource_id must be non-empty")
	}
	if strings.EqualFold(src, dst) {
		return nil, nil, fmt.Errorf("resource_id and target_resource_id must be different")
	}

	params := []*datatransferAdmin.ApplicationTransferParam{}
	if p, err := buildReleaseResourcesParam(args); err != nil {
		return nil, nil, err
	} else if p != nil {
		params = append(params, p)
	}

	return c.dataTransferInsert(ctx, appIdGoogleCalendar, sourceField.StringValue, targetField.StringValue, params)
}

// dataTransferInsert encapsulates idempotency and insert logic for Data Transfer API.
func (c *GoogleWorkspace) dataTransferInsert(ctx context.Context, appID int64, oldOwnerUserId, newOwnerUserId string, params []*datatransferAdmin.ApplicationTransferParam) (*structpb.Struct, annotations.Annotations, error) {
	dtService, err := c.getDataTransferService(ctx, datatransferAdmin.AdminDatatransferScope)
	if err != nil {
		return nil, nil, err
	}

	pageToken := ""
	for {
		// Go through the transfers list and check if there is a transfer in progress for the given appID, source and target users.
		// If there is, return the transfer ID and status.
		listCall := dtService.Transfers.List().OldOwnerUserId(oldOwnerUserId).NewOwnerUserId(newOwnerUserId)
		if pageToken != "" {
			listCall = listCall.PageToken(pageToken)
		}
		transfers, err := listCall.Context(ctx).Do()
		if err != nil {
			return nil, nil, err
		}
		if transfers != nil {
			for _, t := range transfers.DataTransfers {
				if strings.EqualFold(t.OverallTransferStatusCode, "new") || strings.EqualFold(t.OverallTransferStatusCode, "inProgress") {
					for _, adt := range t.ApplicationDataTransfers {
						if adt.ApplicationId == appID {
							resp := &structpb.Struct{Fields: map[string]*structpb.Value{
								"success":     {Kind: &structpb.Value_BoolValue{BoolValue: true}},
								"transfer_id": {Kind: &structpb.Value_StringValue{StringValue: t.Id}},
								"status":      {Kind: &structpb.Value_StringValue{StringValue: t.OverallTransferStatusCode}},
							}}
							return resp, nil, nil
						}
					}
				}
			}
		}
		if transfers.NextPageToken == "" {
			break
		}
		pageToken = transfers.NextPageToken
	}

	// If no transfer is in progress, create a new transfer.
	transfer := &datatransferAdmin.DataTransfer{
		OldOwnerUserId: oldOwnerUserId,
		NewOwnerUserId: newOwnerUserId,
		ApplicationDataTransfers: []*datatransferAdmin.ApplicationDataTransfer{
			{
				ApplicationId:             appID,
				ApplicationTransferParams: params,
			},
		},
	}

	created, err := dtService.Transfers.Insert(transfer).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	resp := &structpb.Struct{Fields: map[string]*structpb.Value{
		"success":     {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"transfer_id": {Kind: &structpb.Value_StringValue{StringValue: created.Id}},
		"status":      {Kind: &structpb.Value_StringValue{StringValue: created.OverallTransferStatusCode}},
	}}
	return resp, nil, nil
}

// buildReleaseResourcesParam returns the RELEASE_RESOURCES param if the optional
// release_resources boolean is present and true. It validates type strictly.
func buildReleaseResourcesParam(args *structpb.Struct) (*datatransferAdmin.ApplicationTransferParam, error) {
	v, present := args.Fields["release_resources"]
	if !present {
		return nil, nil
	}
	b, ok := v.GetKind().(*structpb.Value_BoolValue)
	if !ok {
		return nil, fmt.Errorf("release_resources must be a boolean")
	}
	if !b.BoolValue {
		return nil, nil
	}
	return &datatransferAdmin.ApplicationTransferParam{Key: "RELEASE_RESOURCES", Value: []string{"TRUE"}}, nil
}

// parseDrivePrivacyLevels parses the optional privacy_levels argument, validating values and type.
// Returns default ["private","shared"] if argument is absent.
func parseDrivePrivacyLevels(args *structpb.Struct) ([]string, error) {
	// Defaults
	allowed := map[string]bool{"private": true, "shared": true}
	defaults := []string{"private", "shared"}

	v, present := args.Fields["privacy_levels"]
	if !present {
		return defaults, nil
	}
	ss, ok := v.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return nil, fmt.Errorf("privacy_levels must be a list of strings: allowed values are private, shared")
	}
	normalized := make([]string, 0, len(ss.ListValue.Values))
	seen := map[string]bool{}
	for _, lv := range ss.ListValue.Values {
		sv, ok := lv.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("privacy_levels must be a list of strings: allowed values are private, shared")
		}
		s := strings.TrimSpace(strings.ToLower(sv.StringValue))
		if s == "" {
			continue
		}
		if !allowed[s] {
			return nil, fmt.Errorf("invalid privacy_levels value '%s': allowed values are private, shared", sv.StringValue)
		}
		if !seen[s] {
			normalized = append(normalized, s)
			seen[s] = true
		}
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("privacy_levels list must include at least one value: private or shared")
	}
	return normalized, nil
}

// Helper to get optional string field from args.
func getStringField(args *structpb.Struct, fieldName string) string {
	if field, ok := args.Fields[fieldName]; ok {
		if strVal, ok := field.GetKind().(*structpb.Value_StringValue); ok {
			return strings.TrimSpace(strVal.StringValue)
		}
	}
	return ""
}

// Helper to get optional boolean field from args.
func getBoolField(args *structpb.Struct, fieldName string) (bool, bool) {
	if field, ok := args.Fields[fieldName]; ok {
		if boolVal, ok := field.GetKind().(*structpb.Value_BoolValue); ok {
			return boolVal.BoolValue, true
		}
	}
	return false, false
}

// createGroupActionHandler creates a new Google Group (idempotent: returns existing if already exists).
func (c *GoogleWorkspace) createGroupActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	// Extract and validate required parameters
	groupEmailField, ok := args.Fields["group_email"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing group_email")
	}

	groupNameField, ok := args.Fields["group_name"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing group_name")
	}

	groupEmail := strings.TrimSpace(groupEmailField.StringValue)
	if groupEmail == "" {
		return nil, nil, fmt.Errorf("group_email must be non-empty")
	}

	groupName := strings.TrimSpace(groupNameField.StringValue)
	if groupName == "" {
		return nil, nil, fmt.Errorf("group_name must be non-empty")
	}

	// Validate email format
	if _, err := mail.ParseAddress(groupEmail); err != nil {
		return nil, nil, fmt.Errorf("invalid email address '%s': %w", groupEmail, err)
	}

	// Extract optional description
	description := getStringField(args, "description")

	// Get Directory service
	groupService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupScope)
	if err != nil {
		return nil, nil, err
	}

	// Check if group already exists (idempotency)
	existingGroup, err := groupService.Groups.Get(groupEmail).Context(ctx).Do()
	if err == nil {
		// Group exists - return existing group details
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success":     {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			"group_id":    {Kind: &structpb.Value_StringValue{StringValue: existingGroup.Id}},
			"group_email": {Kind: &structpb.Value_StringValue{StringValue: existingGroup.Email}},
			"group_name":  {Kind: &structpb.Value_StringValue{StringValue: existingGroup.Name}},
		}}
		return &response, nil, nil
	}

	// Check if error is something other than "not found"
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		if gerr.Code != http.StatusNotFound {
			return nil, nil, err
		}
	} else {
		return nil, nil, err
	}

	// Group doesn't exist - create it
	newGroup := &directoryAdmin.Group{
		Email:       groupEmail,
		Name:        groupName,
		Description: description,
	}

	createdGroup, err := groupService.Groups.Insert(newGroup).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success":     {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"group_id":    {Kind: &structpb.Value_StringValue{StringValue: createdGroup.Id}},
		"group_email": {Kind: &structpb.Value_StringValue{StringValue: createdGroup.Email}},
		"group_name":  {Kind: &structpb.Value_StringValue{StringValue: createdGroup.Name}},
	}}
	return &response, nil, nil
}

// applyGroupSettings applies group settings using the Groups Settings API (idempotent: checks current values before updating).
func (c *GoogleWorkspace) applyGroupSettings(ctx context.Context, groupEmail string, allowExternalMembers bool, allowWebPosting bool, whoCanPostMessage string, messageModerationLevel string, hasAllowExternal bool, hasAllowWebPosting bool) (bool, error) {
	// Get Groups Settings service
	settingsService, err := c.getGroupsSettingsService(ctx)
	if err != nil {
		return false, err
	}

	// Fetch current settings for idempotency check
	currentSettings, err := settingsService.Groups.Get(groupEmail).Context(ctx).Do()
	if err != nil {
		return false, err
	}

	// Check if any updates are needed
	needsUpdate := false
	updatedSettings := &groupssettings.Groups{}

	// Check allow_external_members
	if hasAllowExternal {
		currentAllowExternal := strings.EqualFold(currentSettings.AllowExternalMembers, "true")
		if currentAllowExternal != allowExternalMembers {
			needsUpdate = true
			if allowExternalMembers {
				updatedSettings.AllowExternalMembers = "true"
			} else {
				updatedSettings.AllowExternalMembers = "false"
			}
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "AllowExternalMembers")
		}
	}

	// Check allow_web_posting
	if hasAllowWebPosting {
		currentAllowWebPosting := strings.EqualFold(currentSettings.AllowWebPosting, "true")
		if currentAllowWebPosting != allowWebPosting {
			needsUpdate = true
			if allowWebPosting {
				updatedSettings.AllowWebPosting = "true"
			} else {
				updatedSettings.AllowWebPosting = "false"
			}
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "AllowWebPosting")
		}
	}

	// Check who_can_post_message
	if whoCanPostMessage != "" && currentSettings.WhoCanPostMessage != whoCanPostMessage {
		needsUpdate = true
		updatedSettings.WhoCanPostMessage = whoCanPostMessage
		updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "WhoCanPostMessage")
	}

	// Check message_moderation_level
	if messageModerationLevel != "" && currentSettings.MessageModerationLevel != messageModerationLevel {
		needsUpdate = true
		updatedSettings.MessageModerationLevel = messageModerationLevel
		updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "MessageModerationLevel")
	}

	// If no updates needed, return success (idempotent)
	if !needsUpdate {
		return true, nil
	}

	// Update settings
	_, err = settingsService.Groups.Patch(groupEmail, updatedSettings).Context(ctx).Do()
	if err != nil {
		return false, err
	}

	return true, nil
}

// applyGroupSettingsWithTracking applies group settings and returns what changed.
// Returns (settingsUpdated bool, previousSettings map, newSettings map, error).
func (c *GoogleWorkspace) applyGroupSettingsWithTracking(ctx context.Context, groupEmail string, allowExternalMembers bool, allowWebPosting bool, whoCanPostMessage string, messageModerationLevel string, hasAllowExternal bool, hasAllowWebPosting bool) (bool, map[string]string, map[string]string, error) {
	previousSettings := make(map[string]string)
	newSettings := make(map[string]string)

	// Get Groups Settings service
	settingsService, err := c.getGroupsSettingsService(ctx)
	if err != nil {
		return false, nil, nil, err
	}

	// Fetch current settings for idempotency check
	currentSettings, err := settingsService.Groups.Get(groupEmail).Context(ctx).Do()
	if err != nil {
		return false, nil, nil, err
	}

	// Check if any updates are needed
	needsUpdate := false
	updatedSettings := &groupssettings.Groups{}

	// Check allow_external_members
	if hasAllowExternal {
		previousSettings["allow_external_members"] = currentSettings.AllowExternalMembers
		currentAllowExternal := strings.EqualFold(currentSettings.AllowExternalMembers, "true")
		if currentAllowExternal != allowExternalMembers {
			needsUpdate = true
			if allowExternalMembers {
				updatedSettings.AllowExternalMembers = "true"
				newSettings["allow_external_members"] = "true"
			} else {
				updatedSettings.AllowExternalMembers = "false"
				newSettings["allow_external_members"] = "false"
			}
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "AllowExternalMembers")
		} else {
			newSettings["allow_external_members"] = currentSettings.AllowExternalMembers
		}
	}

	// Check allow_web_posting
	if hasAllowWebPosting {
		previousSettings["allow_web_posting"] = currentSettings.AllowWebPosting
		currentAllowWebPosting := strings.EqualFold(currentSettings.AllowWebPosting, "true")
		if currentAllowWebPosting != allowWebPosting {
			needsUpdate = true
			if allowWebPosting {
				updatedSettings.AllowWebPosting = "true"
				newSettings["allow_web_posting"] = "true"
			} else {
				updatedSettings.AllowWebPosting = "false"
				newSettings["allow_web_posting"] = "false"
			}
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "AllowWebPosting")
		} else {
			newSettings["allow_web_posting"] = currentSettings.AllowWebPosting
		}
	}

	// Check who_can_post_message
	if whoCanPostMessage != "" {
		previousSettings["who_can_post_message"] = currentSettings.WhoCanPostMessage
		if currentSettings.WhoCanPostMessage != whoCanPostMessage {
			needsUpdate = true
			updatedSettings.WhoCanPostMessage = whoCanPostMessage
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "WhoCanPostMessage")
			newSettings["who_can_post_message"] = whoCanPostMessage
		} else {
			newSettings["who_can_post_message"] = currentSettings.WhoCanPostMessage
		}
	}

	// Check message_moderation_level
	if messageModerationLevel != "" {
		previousSettings["message_moderation_level"] = currentSettings.MessageModerationLevel
		if currentSettings.MessageModerationLevel != messageModerationLevel {
			needsUpdate = true
			updatedSettings.MessageModerationLevel = messageModerationLevel
			updatedSettings.ForceSendFields = append(updatedSettings.ForceSendFields, "MessageModerationLevel")
			newSettings["message_moderation_level"] = messageModerationLevel
		} else {
			newSettings["message_moderation_level"] = currentSettings.MessageModerationLevel
		}
	}

	// If no updates needed, return success (idempotent)
	if !needsUpdate {
		return false, previousSettings, newSettings, nil
	}

	// Update settings
	_, err = settingsService.Groups.Patch(groupEmail, updatedSettings).Context(ctx).Do()
	if err != nil {
		return false, previousSettings, newSettings, err
	}

	return true, previousSettings, newSettings, nil
}

// modifyGroupSettingsActionHandler updates settings for an existing Google Group (idempotent: checks current settings before updating).
func (c *GoogleWorkspace) modifyGroupSettingsActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
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
			"ANYONE_CAN_POST":       true,
			"ALL_MEMBERS_CAN_POST":  true,
			"ALL_MANAGERS_CAN_POST": true,
			"ALL_OWNERS_CAN_POST":   true,
		}
		if !validWhoCanPost[whoCanPostMessage] {
			return nil, nil, fmt.Errorf("invalid who_can_post_message value '%s': must be one of ANYONE_CAN_POST, ALL_MEMBERS_CAN_POST, ALL_MANAGERS_CAN_POST, ALL_OWNERS_CAN_POST", whoCanPostMessage)
		}
	}

	if messageModerationLevel != "" {
		validModeration := map[string]bool{
			"MODERATE_NONE":         true,
			"MODERATE_NON_MEMBERS":  true,
			"MODERATE_ALL_MESSAGES": true,
		}
		if !validModeration[messageModerationLevel] {
			return nil, nil, fmt.Errorf("invalid message_moderation_level value '%s': must be one of MODERATE_NONE, MODERATE_NON_MEMBERS, MODERATE_ALL_MESSAGES", messageModerationLevel)
		}
	}

	// Get Directory service to verify group exists
	groupService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupReadonlyScope)
	if err != nil {
		return nil, nil, err
	}

	// Verify group exists and get its email
	group, err := groupService.Groups.Get(groupKey).Context(ctx).Do()
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
	settingsUpdated, previousSettings, newSettings, err := c.applyGroupSettingsWithTracking(ctx, group.Email, allowExternalMembers, allowWebPosting, whoCanPostMessage, messageModerationLevel, hasAllowExternal, hasAllowWebPosting)
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
	if prevVal, ok := previousSettings["allow_external_members"]; ok {
		response.Fields["previous_allow_external_members"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: prevVal}}
	}
	if newVal, ok := newSettings["allow_external_members"]; ok {
		response.Fields["new_allow_external_members"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: newVal}}
	}

	if prevVal, ok := previousSettings["allow_web_posting"]; ok {
		response.Fields["previous_allow_web_posting"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: prevVal}}
	}
	if newVal, ok := newSettings["allow_web_posting"]; ok {
		response.Fields["new_allow_web_posting"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: newVal}}
	}

	if prevVal, ok := previousSettings["who_can_post_message"]; ok {
		response.Fields["previous_who_can_post_message"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: prevVal}}
	}
	if newVal, ok := newSettings["who_can_post_message"]; ok {
		response.Fields["new_who_can_post_message"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: newVal}}
	}

	if prevVal, ok := previousSettings["message_moderation_level"]; ok {
		response.Fields["previous_message_moderation_level"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: prevVal}}
	}
	if newVal, ok := newSettings["message_moderation_level"]; ok {
		response.Fields["new_message_moderation_level"] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: newVal}}
	}

	return &response, nil, nil
}

// addUserToGroupActionHandler adds a user to a Google Group with a specified role (idempotent: checks if already a member).
func (c *GoogleWorkspace) addUserToGroupActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	// Extract and validate group_key parameter
	groupKeyField, ok := args.Fields["group_key"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing group_key")
	}

	groupKey := strings.TrimSpace(groupKeyField.StringValue)
	if groupKey == "" {
		return nil, nil, fmt.Errorf("group_key must be non-empty")
	}

	// Extract and validate user_email parameter
	userEmailField, ok := args.Fields["user_email"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing user_email")
	}

	userEmail := strings.TrimSpace(userEmailField.StringValue)
	if userEmail == "" {
		return nil, nil, fmt.Errorf("user_email must be non-empty")
	}

	// Validate email format
	if _, err := mail.ParseAddress(userEmail); err != nil {
		return nil, nil, fmt.Errorf("invalid email address '%s': %w", userEmail, err)
	}

	// Extract optional role parameter (default to MEMBER)
	role := strings.ToUpper(strings.TrimSpace(getStringField(args, "role")))
	if role == "" {
		role = "MEMBER"
	}

	// Validate role
	validRoles := map[string]bool{
		"MEMBER":  true,
		"MANAGER": true,
		"OWNER":   true,
	}
	if !validRoles[role] {
		return nil, nil, fmt.Errorf("invalid role '%s': must be one of MEMBER, MANAGER, OWNER", role)
	}

	// Get Directory service for group member operations
	memberService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupMemberScope)
	if err != nil {
		return nil, nil, err
	}

	// Get Directory service to verify group exists
	groupService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryGroupReadonlyScope)
	if err != nil {
		return nil, nil, err
	}

	// Verify group exists and get its email
	group, err := groupService.Groups.Get(groupKey).Context(ctx).Do()
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				return nil, nil, fmt.Errorf("group not found: %s", groupKey)
			}
		}
		return nil, nil, err
	}

	// Check if user is already a member (idempotency check)
	existingMember, err := memberService.Members.Get(group.Email, userEmail).Context(ctx).Do()
	if err == nil {
		// User is already a member
		alreadyMember := true
		currentRole := strings.ToUpper(existingMember.Role)

		// Check if role needs to be updated
		if currentRole != role {
			// Update the role
			memberUpdate := &directoryAdmin.Member{
				Role: role,
			}
			_, err = memberService.Members.Patch(group.Email, userEmail, memberUpdate).Context(ctx).Do()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to update member role: %w", err)
			}

			response := structpb.Struct{Fields: map[string]*structpb.Value{
				"success":        {Kind: &structpb.Value_BoolValue{BoolValue: true}},
				"group_email":    {Kind: &structpb.Value_StringValue{StringValue: group.Email}},
				"user_email":     {Kind: &structpb.Value_StringValue{StringValue: userEmail}},
				"role":           {Kind: &structpb.Value_StringValue{StringValue: role}},
				"already_member": {Kind: &structpb.Value_BoolValue{BoolValue: alreadyMember}},
			}}
			return &response, nil, nil
		}

		// Already a member with the correct role (idempotent - no API call)
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success":        {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			"group_email":    {Kind: &structpb.Value_StringValue{StringValue: group.Email}},
			"user_email":     {Kind: &structpb.Value_StringValue{StringValue: userEmail}},
			"role":           {Kind: &structpb.Value_StringValue{StringValue: role}},
			"already_member": {Kind: &structpb.Value_BoolValue{BoolValue: alreadyMember}},
		}}
		return &response, nil, nil
	}

	// Check if error is something other than "not found"
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		if gerr.Code != http.StatusNotFound {
			return nil, nil, err
		}
	} else {
		return nil, nil, err
	}

	// User is not a member - add them
	newMember := &directoryAdmin.Member{
		Email: userEmail,
		Role:  role,
	}

	_, err = memberService.Members.Insert(group.Email, newMember).Context(ctx).Do()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to add user to group: %w", err)
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success":        {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"group_email":    {Kind: &structpb.Value_StringValue{StringValue: group.Email}},
		"user_email":     {Kind: &structpb.Value_StringValue{StringValue: userEmail}},
		"role":           {Kind: &structpb.Value_StringValue{StringValue: role}},
		"already_member": {Kind: &structpb.Value_BoolValue{BoolValue: false}},
	}}
	return &response, nil, nil
}
