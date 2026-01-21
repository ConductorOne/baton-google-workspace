package connector

import (
	"context"
	"fmt"
	"net/mail"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
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

// archiveUser archives a user account using the simple archived=true method (idempotent).
func (c *GoogleWorkspace) archiveUser(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	// Extract user_id parameter
	userIdField, ok := args.Fields["user_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing user_id")
	}

	userId := strings.TrimSpace(userIdField.StringValue)

	// Validate non-empty
	if userId == "" {
		return nil, nil, fmt.Errorf("user_id must be non-empty")
	}

	// Get Directory service with write scope
	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		return nil, nil, err
	}

	// Fetch current user state for idempotency check
	u, err := userService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	previousArchivedStatus := u.Archived

	// Check if already archived (idempotency)
	if previousArchivedStatus {
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success":                  {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			"previous_archived_status": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			"new_archived_status":      {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	// Archive the user
	_, err = userService.Users.Update(
		userId,
		&directoryAdmin.User{
			Archived:        true,
			ForceSendFields: []string{"Archived"},
		},
	).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success":                  {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		"previous_archived_status": {Kind: &structpb.Value_BoolValue{BoolValue: false}},
		"new_archived_status":      {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	return &response, nil, nil
}
