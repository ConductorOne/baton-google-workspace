package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	datatransferAdmin "google.golang.org/api/admin/datatransfer/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

// https://developers.google.com/workspace/admin/data-transfer/v1/parameters
const (
	appIdGoogleDocsAndGoogleDrive = int64(55656082996)
	appIdGoogleCalendar           = int64(435070579839)
)

// Argument, return-field, and display-name literals shared across the action
// schemas and handlers (extracted to satisfy goconst).
const (
	fieldSuccess          = "success"
	displaySuccess        = "Success"
	fieldStatus           = "status"
	argResourceID         = "resource_id"
	fieldTransferID       = "transfer_id"
	argTargetResourceID   = "target_resource_id"
	argNewPrimaryEmail    = "new_primary_email"
	fieldPreviousEmail    = "previous_primary_email"
	displayUserResourceID = "User Resource ID"
	// fieldSuspended is the directoryAdmin.User Go struct field name, used only
	// in ForceSendFields entries (must match the field name exactly). Kept
	// separate from any user-facing display text even where the literal
	// happens to match, since the two can diverge independently - a struct
	// field rename would need to update this constant, but should never
	// accidentally affect display text (or vice versa).
	fieldSuspended     = "Suspended"
	fieldResource      = "resource"
	fieldSkippedFields = "skipped_fields"
	// descriptionSkippedFields is shared by update_user_profile and
	// update_user's skipped_fields return field, both backed by the same
	// applyUserProfilePatch.
	descriptionSkippedFields = "Comma-separated list of provided fields that were not applied, with the reason why (e.g. an invalid manager_email)."
)

// Global (account-level) connector action schemas. Their handlers live in this
// same file; they are registered in GlobalActions (connector.go).
var (
	updateUserStatusActionSchema = &v2.BatonActionSchema{
		Name: "update_user_status",
		Arguments: []*config.Field{
			{
				Name:        argResourceID,
				DisplayName: displayUserResourceID,
				Description: "ID of the user resource to update the status of",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "is_suspended",
				DisplayName: "Is Suspended",
				Description: "Update the user status to suspended or active",
				Field:       &config.Field_BoolField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the user resource status was updated successfully",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	transferUserDriveFilesActionSchema = &v2.BatonActionSchema{
		Name:        "transfer_user_drive_files",
		DisplayName: "Transfer User Drive Files",
		Description: "Initiate a Google Drive ownership transfer from one user to another.",
		Arguments: []*config.Field{
			{
				Name:        argResourceID,
				DisplayName: "Source User Resource ID",
				Description: "ID of the user resource to transfer Drive ownership from.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        argTargetResourceID,
				DisplayName: "Target User Resource ID",
				Description: "ID of the user resource to receive Drive ownership.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "privacy_levels",
				DisplayName: "Drive Privacy Levels",
				Description: "One or more of private, shared. Defaults to both.",
				Field:       &config.Field_StringSliceField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the transfer request was created successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        fieldTransferID,
				DisplayName: "Transfer ID",
				Description: "ID of the Data Transfer request.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        fieldStatus,
				DisplayName: "Transfer Status",
				Description: "Initial status returned by the Data Transfer API (e.g., IN_PROGRESS).",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	transferUserCalendarActionSchema = &v2.BatonActionSchema{
		Name:        "transfer_user_calendar",
		DisplayName: "Transfer User Calendar",
		Description: "Initiate a Google Calendar transfer from one user to another.",
		Arguments: []*config.Field{
			{
				Name:        argResourceID,
				DisplayName: "Source User Resource ID",
				Description: "ID of the user resource to transfer calendar data from.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        argTargetResourceID,
				DisplayName: "Target User Resource ID",
				Description: "ID of the user resource to receive calendar data.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "release_resources",
				DisplayName: "Release Resources",
				Description: "If true, sets RELEASE_RESOURCES=TRUE (release resources for future events).",
				Field:       &config.Field_BoolField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the transfer request was created successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        fieldTransferID,
				DisplayName: "Transfer ID",
				Description: "ID of the Data Transfer request.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        fieldStatus,
				DisplayName: "Transfer Status",
				Description: "Initial status returned by the Data Transfer API (e.g., IN_PROGRESS).",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	getUserDataTransferActionSchema = &v2.BatonActionSchema{
		Name:        "get_user_data_transfer",
		DisplayName: "Get User Data Transfer",
		Description: "Read a saved data-transfer operation by its provider transfer ID and verify it against the " +
			"expected source, recipient, application and parameters. Returns the provider's overall and per-application " +
			"status. Read-only: never creates, restarts or mutates a transfer.",
		Arguments: []*config.Field{
			{
				Name:        fieldTransferID,
				DisplayName: "Transfer ID",
				Description: "The provider-assigned data transfer ID returned when the transfer was requested.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        argResourceID,
				DisplayName: "Expected Source User Resource ID",
				Description: "The stable user ID the transfer is expected to move data from.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        argTargetResourceID,
				DisplayName: "Expected Target User Resource ID",
				Description: "The stable user ID the transfer is expected to move data to.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "application_id",
				DisplayName: "Application ID",
				Description: "The expected Google application ID as a numeric string (Drive: 55656082996, Calendar: 435070579839).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        "privacy_levels",
				DisplayName: "Drive Privacy Levels",
				Description: "Required for Drive transfers: one or more of private, shared that the transfer must cover.",
				Field:       &config.Field_StringSliceField{},
				IsRequired:  false,
			},
			{
				Name:        "release_resources",
				DisplayName: "Release Resources",
				Description: "Required for Calendar transfers: the release_resources setting the transfer must carry.",
				Field:       &config.Field_BoolField{},
				IsRequired:  false,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the transfer record was read and matched the expected identity and parameters.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        fieldTransferID,
				DisplayName: "Transfer ID",
				Description: "The provider transfer ID that was read.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        fieldStatus,
				DisplayName: "Transfer Status",
				Description: "The overall status code reported by the provider (e.g., completed, inProgress).",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        "completed",
				DisplayName: "Completed",
				Description: "Whether the verified provider record reports completion overall and for every application. An error or missing status is never completed.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        "per_application_status",
				DisplayName: "Per-Application Status",
				Description: "Map of application ID to its transfer status code as reported by the provider.",
				Field:       &config.Field_StringMapField{},
			},
			{
				Name:        "parameters",
				DisplayName: "Parameters",
				Description: "Map of application ID to the JSON-encoded application transfer parameters recorded by the provider.",
				Field:       &config.Field_StringMapField{},
			},
			{
				Name:        "observed_at",
				DisplayName: "Observed At",
				Description: "RFC 3339 timestamp of when the provider record was read.",
				Field:       &config.Field_StringField{},
			},
			{Name: argResourceID, DisplayName: "Observed Source User ID", Field: &config.Field_StringField{}},
			{Name: argTargetResourceID, DisplayName: "Observed Target User ID", Field: &config.Field_StringField{}},
			{Name: "request_time", DisplayName: "Provider Request Time", Field: &config.Field_StringField{}},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	changeUserPrimaryEmailActionSchema = &v2.BatonActionSchema{
		Name:        "change_user_primary_email",
		DisplayName: "Change User Primary Email",
		Description: "Update a user's primary email address.",
		Arguments: []*config.Field{
			{
				Name:        argResourceID,
				DisplayName: displayUserResourceID,
				Description: "ID of the user resource to update.",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
			{
				Name:        argNewPrimaryEmail,
				DisplayName: "New Primary Email",
				Description: "New primary email address (must be within a verified domain).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the primary email was updated successfully.",
				Field:       &config.Field_BoolField{},
			},
			{
				Name:        fieldPreviousEmail,
				DisplayName: "Previous Primary Email",
				Description: "User's previous primary email address.",
				Field:       &config.Field_StringField{},
			},
			{
				Name:        argNewPrimaryEmail,
				DisplayName: "New Primary Email",
				Description: "User's updated primary email address.",
				Field:       &config.Field_StringField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT},
	}
	disableUserActionSchema = &v2.BatonActionSchema{
		Name:        "disable_user",
		DisplayName: "Disable User",
		Description: "Suspend a user account.",
		Arguments: []*config.Field{
			{
				Name:        argUserID,
				DisplayName: displayUserResourceID,
				Description: "ID of the user resource to disable (suspend).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the user was disabled (suspended) successfully.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT_DISABLE},
	}
	enableUserActionSchema = &v2.BatonActionSchema{
		Name:        "enable_user",
		DisplayName: "Enable User",
		Description: "Unsuspend a user account.",
		Arguments: []*config.Field{
			{
				Name:        argUserID,
				DisplayName: displayUserResourceID,
				Description: "ID of the user resource to enable (unsuspend).",
				Field:       &config.Field_StringField{},
				IsRequired:  true,
			},
		},
		ReturnTypes: []*config.Field{
			{
				Name:        fieldSuccess,
				DisplayName: displaySuccess,
				Description: "Whether the user was enabled (unsuspended) successfully.",
				Field:       &config.Field_BoolField{},
			},
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT_ENABLE},
	}
)

func (c *GoogleWorkspace) updateUserStatus(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields[argResourceID].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing resource ID")
	}

	isSuspendedField, ok := args.Fields["is_suspended"].GetKind().(*structpb.Value_BoolValue)
	if !ok {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing is_suspended")
	}

	isSuspended := isSuspendedField.BoolValue
	userId := guidField.StringValue

	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// update user.isSuspended state
	err = withRateLimitWait(ctx, func() error {
		_, err := client.UpdateUser(ctx, userId, &directoryAdmin.User{
			Suspended:       isSuspended,
			ForceSendFields: []string{fieldSuspended},
		})
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to update user status: %w", err)
	}

	response := structpb.Struct{
		Fields: map[string]*structpb.Value{
			fieldSuccess: {
				Kind: &structpb.Value_BoolValue{BoolValue: true},
			},
		},
	}

	return &response, nil, nil
}

// disableUserActionHandler suspends a user (idempotent: if already suspended, returns success).
func (c *GoogleWorkspace) disableUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields[argUserID].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing user ID")
	}

	userId := guidField.StringValue
	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// fetch current to ensure idempotency
	u, err := withRateLimitWaitValue(ctx, func() (*directoryAdmin.User, error) {
		return client.GetUserForProvisioning(ctx, userId)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to get user %s for disableUser: %w", userId, err)
	}
	if u.Suspended { // already suspended
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			fieldSuccess: {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	err = withRateLimitWait(ctx, func() error {
		_, err := client.UpdateUser(ctx, userId, &directoryAdmin.User{
			Suspended:       true,
			ForceSendFields: []string{fieldSuspended},
		})
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to suspend user %s: %w", userId, err)
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		fieldSuccess: {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	return &response, nil, nil
}

// enableUserActionHandler unsuspends a user (idempotent: if already active, returns success).
func (c *GoogleWorkspace) enableUserActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields[argUserID].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing user ID")
	}

	userId := guidField.StringValue
	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// fetch current to ensure idempotency
	u, err := withRateLimitWaitValue(ctx, func() (*directoryAdmin.User, error) {
		return client.GetUserForProvisioning(ctx, userId)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to get user %s for enableUser: %w", userId, err)
	}
	if !u.Suspended { // already active
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			fieldSuccess: {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		}}
		return &response, nil, nil
	}

	err = withRateLimitWait(ctx, func() error {
		_, err := client.UpdateUser(ctx, userId, &directoryAdmin.User{
			Suspended:       false,
			ForceSendFields: []string{fieldSuspended}, // This is needed because the SDK would omit any field that has the field type default value (false).
		})
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to unsuspend user %s: %w", userId, err)
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		fieldSuccess: {Kind: &structpb.Value_BoolValue{BoolValue: true}},
	}}
	return &response, nil, nil
}

// changeUserPrimaryEmail updates a user's primary email.
func (c *GoogleWorkspace) changeUserPrimaryEmail(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	guidField, ok := args.Fields[argResourceID].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing resource ID")
	}
	newEmailField, ok := args.Fields[argNewPrimaryEmail].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "missing new_primary_email")
	}

	userId := guidField.StringValue
	newPrimary := newEmailField.StringValue

	// Validate that newPrimary is a valid email address
	if _, err := mail.ParseAddress(newPrimary); err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("invalid email address: %s", newPrimary), err)
	}

	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// fetch current for return payload
	u, err := withRateLimitWaitValue(ctx, func() (*directoryAdmin.User, error) {
		return client.GetUserForProvisioning(ctx, userId)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to get user %s for changeUserPrimaryEmail: %w", userId, err)
	}
	prev := u.PrimaryEmail
	if emailsEqual(prev, newPrimary) { // Already primary email
		response := structpb.Struct{Fields: map[string]*structpb.Value{
			fieldSuccess:       {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			fieldPreviousEmail: {Kind: &structpb.Value_StringValue{StringValue: prev}},
			argNewPrimaryEmail: {Kind: &structpb.Value_StringValue{StringValue: newPrimary}},
		}}
		return &response, nil, nil
	}

	err = withRateLimitWait(ctx, func() error {
		_, err := client.UpdateUser(ctx, userId, &directoryAdmin.User{
			PrimaryEmail:    newPrimary,
			ForceSendFields: []string{"PrimaryEmail"},
		})
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to update primary email for user %s: %w", userId, err)
	}

	response := structpb.Struct{Fields: map[string]*structpb.Value{
		fieldSuccess:       {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		fieldPreviousEmail: {Kind: &structpb.Value_StringValue{StringValue: prev}},
		argNewPrimaryEmail: {Kind: &structpb.Value_StringValue{StringValue: newPrimary}},
	}}
	return &response, nil, nil
}

// transferUserDriveFiles initiates a Drive ownership transfer using Data Transfer API.
func (c *GoogleWorkspace) transferUserDriveFiles(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	source, err := requiredStringArg(args, argResourceID)
	if err != nil {
		return nil, nil, err
	}
	target, err := requiredStringArg(args, argTargetResourceID)
	if err != nil {
		return nil, nil, err
	}
	if strings.EqualFold(source, target) {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "resource_id and target_resource_id must be different")
	}
	levels, err := parseDrivePrivacyLevels(args)
	if err != nil {
		return nil, nil, err
	}
	params := []*datatransferAdmin.ApplicationTransferParam{{Key: "PRIVACY_LEVEL", Value: levels}}
	return c.dataTransferInsert(ctx, appIdGoogleDocsAndGoogleDrive, source, target, params)
}

// transferUserCalendar initiates a Calendar transfer using Data Transfer API.
func (c *GoogleWorkspace) transferUserCalendar(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	source, err := requiredStringArg(args, argResourceID)
	if err != nil {
		return nil, nil, err
	}
	target, err := requiredStringArg(args, argTargetResourceID)
	if err != nil {
		return nil, nil, err
	}
	if strings.EqualFold(source, target) {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "resource_id and target_resource_id must be different")
	}
	var params []*datatransferAdmin.ApplicationTransferParam
	if parameter, err := buildReleaseResourcesParam(args); err != nil {
		return nil, nil, err
	} else if parameter != nil {
		params = append(params, parameter)
	}
	return c.dataTransferInsert(ctx, appIdGoogleCalendar, source, target, params)
}

// dataTransferInsert encapsulates idempotency and insert logic for Data Transfer API.
//
// Idempotency contract: an existing ONGOING transfer may only be adopted when
// it matches the requested source, recipient, application AND the complete
// parameter set (every key and value, no unknown or duplicate parameters).
// A conflicting, unknown-parameter, or incomplete ongoing transfer — or more
// than one match — returns FailedPrecondition listing the available transfer
// IDs instead of starting a second concurrent transfer. Historical completed
// records are never adopted as this operation's result; the durable caller
// reconciles those via the saved transfer ID and get_user_data_transfer.
func (c *GoogleWorkspace) dataTransferInsert(
	ctx context.Context,
	appID int64,
	oldOwnerUserId, newOwnerUserId string,
	params []*datatransferAdmin.ApplicationTransferParam,
) (*structpb.Struct, annotations.Annotations, error) {
	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	const maxDiscoveryPages = 10
	pageToken := ""
	var matched *datatransferAdmin.DataTransfer
	waitLoop := newRateLimitWaitLoopValue[*datatransferAdmin.DataTransfersListResponse](ctx)
	for page := range maxDiscoveryPages {
		transfers, err := waitLoop(func() (*datatransferAdmin.DataTransfersListResponse, error) {
			return client.ListDataTransfers(ctx, oldOwnerUserId, newOwnerUserId, pageToken)
		})
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to discover transfers: %w", err)
		}
		if transfers == nil {
			return nil, nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: transfer discovery returned no response; refusing to insert")
		}
		for _, candidate := range transfers.DataTransfers {
			if candidate == nil || candidate.Id == "" || candidate.OldOwnerUserId != oldOwnerUserId || candidate.NewOwnerUserId != newOwnerUserId {
				return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: transfer discovery returned incomplete or mismatched identity; refusing to insert")
			}
			switch strings.ToLower(candidate.OverallTransferStatusCode) {
			case transferStatusComplete, "failed":
				continue
			case "new", "inprogress":
			default:
				return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("google-workspace: transfer %s has unknown status; refusing to insert", candidate.Id))
			}
			if len(candidate.ApplicationDataTransfers) == 0 {
				return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("google-workspace: transfer %s has no application evidence", candidate.Id))
			}
			var application *datatransferAdmin.ApplicationDataTransfer
			for _, item := range candidate.ApplicationDataTransfers {
				if item == nil || item.ApplicationId == 0 {
					return nil, nil, uhttp.WrapErrors(codes.DataLoss, fmt.Sprintf("google-workspace: transfer %s has an invalid application entry", candidate.Id))
				}
				if item.ApplicationId == appID {
					if application != nil {
						return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("google-workspace: transfer %s has ambiguous application entries", candidate.Id))
					}
					application = item
				}
			}
			if application == nil {
				continue
			}
			if len(candidate.ApplicationDataTransfers) != 1 || !transferParamsMatch(application.ApplicationTransferParams, params) {
				return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("google-workspace: ongoing transfer %s has different or incomplete parameters", candidate.Id))
			}
			if matched != nil {
				if matched.Id == candidate.Id {
					continue
				}
				return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, fmt.Sprintf("google-workspace: transfers %s and %s both match; correlation is ambiguous", matched.Id, candidate.Id))
			}
			matched = candidate
		}
		if transfers.NextPageToken == "" {
			break
		}
		if page == maxDiscoveryPages-1 {
			return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: transfer discovery is incomplete; refusing to insert")
		}
		pageToken = transfers.NextPageToken
	}
	if matched == nil {
		request := &datatransferAdmin.DataTransfer{
			OldOwnerUserId: oldOwnerUserId,
			NewOwnerUserId: newOwnerUserId,
			ApplicationDataTransfers: []*datatransferAdmin.ApplicationDataTransfer{{
				ApplicationId: appID, ApplicationTransferParams: params,
			}},
		}
		matched, err = withRateLimitWaitValue(ctx, func() (*datatransferAdmin.DataTransfer, error) {
			return client.InsertDataTransfer(ctx, request)
		})
		if err != nil {
			return nil, nil, fmt.Errorf("google-workspace: failed to create data transfer: %w", err)
		}
		if matched == nil || matched.Id == "" {
			return nil, nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: data transfer insert returned no ID; outcome unknown")
		}
	}
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		fieldSuccess:    structpb.NewBoolValue(true),
		fieldTransferID: structpb.NewStringValue(matched.Id),
		fieldStatus:     structpb.NewStringValue(matched.OverallTransferStatusCode),
	}}, nil, nil
}

// isOngoingTransferStatus reports whether the provider status string means the
// transfer is still running (adoptable) rather than terminal or unknown.
func isOngoingTransferStatus(status string) bool {
	return strings.EqualFold(status, "new") || strings.EqualFold(status, "inProgress")
}

// transferParamsMatch compares complete parameter sets, including the documented
// empty set for Calendar's retain-resources default. Privacy value order is irrelevant.
func transferParamsMatch(got, want []*datatransferAdmin.ApplicationTransferParam) bool {
	if len(got) != len(want) {
		return false
	}
	for i, expected := range want {
		if expected == nil || expected.Key == "" || len(expected.Value) == 0 {
			return false
		}
		for _, previous := range want[:i] {
			if previous.Key == expected.Key {
				return false
			}
		}
		var actual *datatransferAdmin.ApplicationTransferParam
		for _, candidate := range got {
			if candidate == nil || candidate.Key == "" {
				return false
			}
			if candidate.Key == expected.Key {
				if actual != nil {
					return false
				}
				actual = candidate
			}
		}
		if actual == nil || len(actual.Value) != len(expected.Value) {
			return false
		}
		for valueIndex, value := range expected.Value {
			if value == "" {
				return false
			}
			for _, previous := range expected.Value[:valueIndex] {
				if strings.EqualFold(previous, value) {
					return false
				}
			}
			count := 0
			for _, candidate := range actual.Value {
				if strings.EqualFold(candidate, value) {
					count++
				}
			}
			if count != 1 {
				return false
			}
		}
	}
	return true
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
		return nil, uhttp.WrapErrors(codes.InvalidArgument, "release_resources must be a boolean")
	}
	if !b.BoolValue {
		return nil, nil
	}
	return &datatransferAdmin.ApplicationTransferParam{Key: "RELEASE_RESOURCES", Value: []string{"TRUE"}}, nil
}

// parseDrivePrivacyLevels parses the optional privacy_levels argument, validating values and type.
// Returns the documented uppercase wire values, defaulting to both privacy levels.
func parseDrivePrivacyLevels(args *structpb.Struct) ([]string, error) {
	value, present := args.GetFields()["privacy_levels"]
	if !present {
		return []string{"PRIVATE", "SHARED"}, nil
	}
	list, ok := value.GetKind().(*structpb.Value_ListValue)
	if !ok || list.ListValue == nil {
		return nil, uhttp.WrapErrors(codes.InvalidArgument, "privacy_levels must be a list of private/shared strings")
	}
	levels := make([]string, 0, 2)
	private, shared := false, false
	for _, item := range list.ListValue.Values {
		text, ok := item.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, uhttp.WrapErrors(codes.InvalidArgument, "privacy_levels must contain strings")
		}
		switch strings.ToLower(strings.TrimSpace(text.StringValue)) {
		case "":
		case "private":
			if !private {
				levels = append(levels, "PRIVATE")
				private = true
			}
		case "shared":
			if !shared {
				levels = append(levels, "SHARED")
				shared = true
			}
		default:
			return nil, uhttp.WrapErrors(codes.InvalidArgument, "privacy_levels accepts only private and shared")
		}
	}
	if len(levels) == 0 {
		return nil, uhttp.WrapErrors(codes.InvalidArgument, "privacy_levels must include at least one privacy level")
	}
	return levels, nil
}

// argApplicationID / return-field literals for get_user_data_transfer.
const (
	argApplicationID       = "application_id"
	fieldCompleted         = "completed"
	fieldPerAppStatus      = "per_application_status"
	fieldParameters        = "parameters"
	fieldObservedAt        = "observed_at"
	transferStatusComplete = "completed"
)

// getUserDataTransfer reads one saved data-transfer record by its provider ID
// and verifies it against the caller's expected identity and parameters. It
// performs exactly one provider read (Transfers.Get; a throttled read may
// wait via withRateLimitWaitValue but is never retried in a loop here) and
// never mutates or restarts anything.
//
// Mismatch and unreadable state are explicit: a record whose ID, source,
// recipient, application or complete parameter set does not match the request
// returns FailedPrecondition WITH the observed record fields where available,
// so a caller can reconcile from the returned data instead of re-submitting.
// Unknown/missing statuses or parameters are never reported as completed.
func (c *GoogleWorkspace) getUserDataTransfer(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	// Required scalar arguments.
	transferID, err := requiredStringArg(args, fieldTransferID)
	if err != nil {
		return nil, nil, err
	}
	expectedSource, err := requiredStringArg(args, argResourceID)
	if err != nil {
		return nil, nil, err
	}
	expectedTarget, err := requiredStringArg(args, argTargetResourceID)
	if err != nil {
		return nil, nil, err
	}
	appIDStr, err := requiredStringArg(args, argApplicationID)
	if err != nil {
		return nil, nil, err
	}
	appID, err := strconv.ParseInt(strings.TrimSpace(appIDStr), 10, 64)
	if err != nil {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument,
			fmt.Sprintf("application_id must be a numeric Google application ID string (Drive: %d, Calendar: %d)", appIdGoogleDocsAndGoogleDrive, appIdGoogleCalendar))
	}

	// Application-specific expected parameters. privacy_levels is required
	// for Drive; release_resources is required for Calendar; the wrong
	// application's parameter is rejected outright.
	var expectedParams []*datatransferAdmin.ApplicationTransferParam
	switch appID {
	case appIdGoogleDocsAndGoogleDrive:
		if _, present := args.Fields["release_resources"]; present {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument,
				fmt.Sprintf("release_resources applies to Calendar transfers (application %d), not Drive (application %d)", appIdGoogleCalendar, appIdGoogleDocsAndGoogleDrive))
		}
		if _, present := args.GetFields()["privacy_levels"]; !present {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "privacy_levels is required for Drive transfer verification")
		}
		levels, err := parseDrivePrivacyLevels(args)
		if err != nil {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "privacy_levels is required for Drive transfers: ", err)
		}
		expectedParams = append(expectedParams, &datatransferAdmin.ApplicationTransferParam{Key: "PRIVACY_LEVEL", Value: levels})
	case appIdGoogleCalendar:
		v, present := args.Fields["release_resources"]
		if !present {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "release_resources is required for Calendar transfers")
		}
		b, ok := v.GetKind().(*structpb.Value_BoolValue)
		if !ok {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "release_resources must be a boolean")
		}
		if _, present := args.Fields["privacy_levels"]; present {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument,
				fmt.Sprintf("privacy_levels applies to Drive transfers (application %d), not Calendar (application %d)", appIdGoogleDocsAndGoogleDrive, appIdGoogleCalendar))
		}
		if b.BoolValue {
			expectedParams = append(expectedParams, &datatransferAdmin.ApplicationTransferParam{Key: "RELEASE_RESOURCES", Value: []string{"TRUE"}})
		} else {
			// The absence of RELEASE_RESOURCES=TRUE is the wire expression of
			// release_resources=false. An expected-false verification can only
			// assert the parameter is absent from the provider record.
			expectedParams = nil
		}
	default:
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument,
			fmt.Sprintf("unsupported application_id %s: supported values are Drive (%d) and Calendar (%d)", strings.TrimSpace(appIDStr), appIdGoogleDocsAndGoogleDrive, appIdGoogleCalendar))
	}

	client, err := c.getClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Exactly one bounded provider read. 403/404 preserve their codes via
	// wrapGoogleApiErrorWithContext inside the client helper.
	transfer, err := withRateLimitWaitValue(ctx, func() (*datatransferAdmin.DataTransfer, error) {
		return client.GetDataTransfer(ctx, transferID)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to read data transfer: %w", err)
	}

	observed, err := observedTransferReturnFields(transfer, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return observed, nil, err
	}
	if transfer == nil || transfer.Id != transferID ||
		transfer.OldOwnerUserId != expectedSource || transfer.NewOwnerUserId != expectedTarget {
		return observed, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: transfer record does not match the saved ID and expected owners")
	}
	var application *datatransferAdmin.ApplicationDataTransfer
	completed := strings.EqualFold(transfer.OverallTransferStatusCode, transferStatusComplete)
	if !completed && !isOngoingTransferStatus(transfer.OverallTransferStatusCode) {
		return observed, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: transfer status is failed, missing, or unknown")
	}
	for _, item := range transfer.ApplicationDataTransfers {
		if item.ApplicationId == appID {
			application = item
		}
		if strings.EqualFold(item.ApplicationTransferStatus, transferStatusComplete) {
			continue
		}
		completed = false
		if !isOngoingTransferStatus(item.ApplicationTransferStatus) {
			return observed, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: application transfer status is failed, missing, or unknown")
		}
	}
	if application == nil || !transferParamsMatch(application.ApplicationTransferParams, expectedParams) {
		return observed, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: transfer application or parameters do not match the captured request")
	}
	observed.Fields[fieldSuccess] = structpb.NewBoolValue(true)
	observed.Fields[fieldCompleted] = structpb.NewBoolValue(completed)
	return observed, nil, nil
}

// requiredStringArg extracts a required non-empty string argument. Split from
// the per-action inline patterns so the transfer-read handler cannot
// accidentally treat a missing field as an empty match.
func requiredStringArg(args *structpb.Struct, name string) (string, error) {
	value, err := actions.RequireStringArg(args, name)
	if err != nil {
		return "", uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: invalid required argument", err)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", uhttp.WrapErrors(codes.InvalidArgument, fmt.Sprintf("%s must be non-empty", name))
	}
	return value, nil
}

// observedTransferReturnFields maps a provider DataTransfer record onto the
// action's return fields without asserting anything about completion.
func observedTransferReturnFields(transfer *datatransferAdmin.DataTransfer, observedAt string) (*structpb.Struct, error) {
	perAppStatus := map[string]*structpb.Value{}
	parameters := map[string]*structpb.Value{}
	result := &structpb.Struct{Fields: map[string]*structpb.Value{
		fieldSuccess:      structpb.NewBoolValue(false),
		fieldCompleted:    structpb.NewBoolValue(false),
		fieldPerAppStatus: structpb.NewStructValue(&structpb.Struct{Fields: perAppStatus}),
		fieldParameters:   structpb.NewStructValue(&structpb.Struct{Fields: parameters}),
		fieldObservedAt:   structpb.NewStringValue(observedAt),
	}}
	if transfer == nil {
		return result, uhttp.WrapErrors(codes.DataLoss, "google-workspace: transfer response is missing")
	}
	result.Fields[fieldTransferID] = structpb.NewStringValue(transfer.Id)
	result.Fields[fieldStatus] = structpb.NewStringValue(transfer.OverallTransferStatusCode)
	result.Fields[argResourceID] = structpb.NewStringValue(transfer.OldOwnerUserId)
	result.Fields[argTargetResourceID] = structpb.NewStringValue(transfer.NewOwnerUserId)
	result.Fields["request_time"] = structpb.NewStringValue(transfer.RequestTime)
	for _, item := range transfer.ApplicationDataTransfers {
		if item == nil || item.ApplicationId == 0 {
			return result, uhttp.WrapErrors(codes.DataLoss, "google-workspace: transfer contains invalid application data")
		}
		key := strconv.FormatInt(item.ApplicationId, 10)
		if _, duplicate := perAppStatus[key]; duplicate {
			return result, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: transfer contains duplicate application identities")
		}
		perAppStatus[key] = structpb.NewStringValue(item.ApplicationTransferStatus)
		encoded, err := json.Marshal(item.ApplicationTransferParams)
		if err != nil {
			return result, uhttp.WrapErrors(codes.DataLoss, "google-workspace: invalid transfer parameters", err)
		}
		parameters[key] = structpb.NewStringValue(string(encoded))
	}
	return result, nil
}
