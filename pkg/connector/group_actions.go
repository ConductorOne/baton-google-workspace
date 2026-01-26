package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

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
