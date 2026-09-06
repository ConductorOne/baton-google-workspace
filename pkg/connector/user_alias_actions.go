package connector

import (
	"context"
	"net/mail"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	aliasOutcomeDeleted            = "deleted"
	aliasOutcomeAlreadyAbsent      = "already_absent"
	aliasOutcomePrimaryRejected    = "primary_address_rejected"
	aliasOutcomePreconditionFailed = "precondition_failed"
	aliasOutcomeOwnerMismatch      = "owner_mismatch"
	aliasOutcomeUserNotFound       = "user_not_found"
	aliasOutcomeReadbackUnknown    = "readback_unknown"
	aliasArg                       = "alias"
	aliasExpectedPrimary           = "expected_primary_email"
	aliasExpectedCustomer          = "expected_customer_id"
)

var removeUserAliasActionSchema = &v2.BatonActionSchema{
	Name:        "remove_user_alias",
	DisplayName: "Remove User Alias",
	Description: "Verify account preconditions, remove an editable alias, and confirm the same account's alias collection. Does not prove global address availability.",
	Arguments: []*config.Field{
		{Name: argUserID, DisplayName: displayUserID, Description: "Stable Google user ID, not an email address.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasArg, DisplayName: "Alias Email", Description: "Exact alias address to remove.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasExpectedPrimary, DisplayName: "Expected Primary Email", Description: "Required current primary-address precondition.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasExpectedCustomer, DisplayName: "Expected Customer ID", Description: "Required customer-ID precondition.", Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: []*config.Field{
		{Name: fieldSuccess, DisplayName: displaySuccess, Field: &config.Field_BoolField{}},
		{Name: "outcome", DisplayName: "Outcome", Description: "Qualified deletion, absence, ownership, precondition or readback outcome.", Field: &config.Field_StringField{}},
		{Name: "alias_present_before", DisplayName: "Alias Present Before", Description: "Omitted when the initial alias collection could not be read.", Field: &config.Field_BoolField{}},
		{Name: "alias_present_after", DisplayName: "Alias Present After", Description: "Omitted when post-state is unknown, never defaulted to false.", Field: &config.Field_BoolField{}},
		{Name: "observation_complete", DisplayName: "Observation Complete", Field: &config.Field_BoolField{}},
		{Name: argUserID, DisplayName: displayUserID, Description: "Requested stable target user ID.", Field: &config.Field_StringField{}},
		{Name: "primary_email", DisplayName: "Observed Primary Email", Field: &config.Field_StringField{}},
		{Name: "customer_id", DisplayName: "Observed Customer ID", Field: &config.Field_StringField{}},
	},
	ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
}

func (o *userResourceType) registerRemoveUserAliasAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, removeUserAliasActionSchema, o.removeUserAliasActionHandler)
}

func aliasListHas(aliases []string, alias string) bool {
	for _, candidate := range aliases {
		if strings.EqualFold(candidate, alias) {
			return true
		}
	}
	return false
}

func (o *userResourceType) removeUserAliasActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	userID, err := requiredStringArg(args, argUserID)
	if err != nil {
		return nil, nil, err
	}
	alias, err := requiredStringArg(args, aliasArg)
	if err != nil {
		return nil, nil, err
	}
	primary, err := requiredStringArg(args, aliasExpectedPrimary)
	if err != nil {
		return nil, nil, err
	}
	customer, err := requiredStringArg(args, aliasExpectedCustomer)
	if err != nil {
		return nil, nil, err
	}
	if strings.Contains(userID, "@") {
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: alias removal requires a stable user ID")
	}
	for _, value := range []string{alias, primary} {
		address, err := mail.ParseAddress(value)
		if err != nil || address.Name != "" || address.Address != value || !strings.Contains(value, "@") {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: alias and primary email must be plain email addresses")
		}
	}
	var user *admin.User
	var before, after *bool
	result := func(outcome string) *structpb.Struct {
		fields := map[string]*structpb.Value{
			fieldSuccess:           structpb.NewBoolValue(after != nil && !*after && (outcome == aliasOutcomeDeleted || outcome == aliasOutcomeAlreadyAbsent)),
			"outcome":              structpb.NewStringValue(outcome),
			"observation_complete": structpb.NewBoolValue(after != nil),
			argUserID:              structpb.NewStringValue(userID),
		}
		if user != nil {
			fields["primary_email"] = structpb.NewStringValue(user.PrimaryEmail)
			fields["customer_id"] = structpb.NewStringValue(user.CustomerId)
		}
		if before != nil {
			fields["alias_present_before"] = structpb.NewBoolValue(*before)
		}
		if after != nil {
			fields["alias_present_after"] = structpb.NewBoolValue(*after)
		}
		return &structpb.Struct{Fields: fields}
	}
	if strings.EqualFold(alias, primary) {
		return result(aliasOutcomePrimaryRejected), nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: cannot remove a primary address as an alias")
	}
	if o.client == nil || o.client.UserProvisioningService == nil {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: user provisioning service is required for alias removal")
	}
	readUser := newRateLimitWaitLoopValue[*admin.User](ctx)
	user, err = readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, userID) })
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return result(aliasOutcomeUserNotFound), nil, err
		}
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	matchesAccount := func(candidate *admin.User) bool {
		return candidate != nil && candidate.Id == userID && candidate.CustomerId == customer && strings.EqualFold(candidate.PrimaryEmail, primary)
	}
	if !matchesAccount(user) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias target does not match the pinned account")
	}
	if aliasListHas(user.NonEditableAliases, alias) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: non-editable aliases cannot be removed")
	}
	// A dedicated alias collection read is authoritative; omission of the optional
	// User.Aliases profile field is not evidence of absence.
	readAliases := newRateLimitWaitLoopValue[[]string](ctx)
	aliases, err := readAliases(func() ([]string, error) { return o.client.ListUserAliases(ctx, userID) })
	if err != nil {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	present := aliasListHas(aliases, alias)
	before = &present
	if !present {
		owner, ownerErr := readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, alias) })
		if ownerErr == nil {
			if !matchesAccount(owner) {
				return result(aliasOutcomeOwnerMismatch), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias resolves to a different account")
			}
			return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.Aborted, "google-workspace: alias ownership changed during verification")
		}
		if status.Code(ownerErr) != codes.NotFound {
			return result(aliasOutcomeReadbackUnknown), nil, ownerErr
		}
		absent := false
		after = &absent
		// A group or another namespace can still own the address.
		return result(aliasOutcomeAlreadyAbsent), nil, nil
	}
	if err := o.client.DeleteUserAlias(ctx, userID, alias); err != nil && status.Code(err) != codes.NotFound {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	observed, err := readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, userID) })
	if err != nil {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	if !matchesAccount(observed) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.Aborted, "google-workspace: account identity or preconditions changed during alias removal")
	}
	aliases, err = readAliases(func() ([]string, error) { return o.client.ListUserAliases(ctx, userID) })
	if err != nil {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	remaining := aliasListHas(aliases, alias)
	after = &remaining
	if remaining {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias remains after deletion")
	}
	return result(aliasOutcomeDeleted), nil, nil
}
