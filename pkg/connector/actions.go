package connector

import (
	"context"
	"fmt"
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
		Suspended: isSuspended,
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

// lockUser suspends a user (idempotent: if already suspended, returns success).
func (c *GoogleWorkspace) lockUser(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields["resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing resource ID")
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
	if u.Suspended { // already locked
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	_, err = userService.Users.Update(userId, &directoryAdmin.User{Suspended: true}).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	return &response, nil, nil
}

// unlockUser unsuspends a user (idempotent: if already active, returns success).
func (c *GoogleWorkspace) unlockUser(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields["resource_id"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing resource ID")
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
	if !u.Suspended { // already unlocked
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	_, err = userService.Users.Update(userId, &directoryAdmin.User{Suspended: false}).Context(ctx).Do()
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

	userService, err := c.getDirectoryService(ctx, directoryAdmin.AdminDirectoryUserScope)
	if err != nil {
		return nil, nil, err
	}

	userId := guidField.StringValue
	newPrimary := newEmailField.StringValue

	// fetch current for return payload
	u, err := userService.Users.Get(userId).Context(ctx).Do()
	if err != nil {
		return nil, nil, err
	}
	prev := u.PrimaryEmail

	_, err = userService.Users.Update(userId, &directoryAdmin.User{PrimaryEmail: newPrimary}).Context(ctx).Do()
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

	// Build Drive params from privacy_levels
	params := []*datatransferAdmin.ApplicationTransferParam{}
	allowed := map[string]bool{"PRIVATE": true, "SHARED": true}
	levels := []string{"PRIVATE", "SHARED"}
	if v, present := args.Fields["privacy_levels"]; present {
		if ss, ok := v.GetKind().(*structpb.Value_ListValue); ok {
			normalized := make([]string, 0, len(ss.ListValue.Values))
			seen := map[string]bool{}
			for _, lv := range ss.ListValue.Values {
				if sv, ok := lv.GetKind().(*structpb.Value_StringValue); ok {
					s := strings.TrimSpace(strings.ToUpper(sv.StringValue))
					if s == "" {
						continue
					}
					if !allowed[s] {
						return nil, nil, fmt.Errorf("invalid privacy_levels value '%s': allowed values are PRIVATE, SHARED", sv.StringValue)
					}
					if !seen[s] {
						normalized = append(normalized, s)
						seen[s] = true
					}
				}
			}
			if len(normalized) > 0 {
				levels = normalized
			}
		}
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

	params := []*datatransferAdmin.ApplicationTransferParam{}
	if v, present := args.Fields["release_resources"]; present {
		if b, ok := v.GetKind().(*structpb.Value_BoolValue); ok && b.BoolValue {
			params = append(params, &datatransferAdmin.ApplicationTransferParam{Key: "RELEASE_RESOURCES", Value: []string{"TRUE"}})
		}
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
		listCall := dtService.Transfers.List().OldOwnerUserId(oldOwnerUserId).NewOwnerUserId(newOwnerUserId)
		if pageToken != "" {
			listCall = listCall.PageToken(pageToken)
		}
		transfers, err := listCall.Context(ctx).Do()
		if err != nil {
			break
		}
		if transfers != nil {
			for _, t := range transfers.DataTransfers {
				if t.OverallTransferStatusCode == "NEW" || t.OverallTransferStatusCode == "IN_PROGRESS" {
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
