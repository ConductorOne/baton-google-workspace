package connector

import (
	"context"
	"fmt"
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
	aliasOutcomeAdded              = "added"
	aliasOutcomeAlreadyPresent     = "already_present"
	aliasOutcomeConflict           = "alias_conflict"
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

var addUserAliasActionSchema = &v2.BatonActionSchema{
	Name:        "add_user_alias",
	DisplayName: "Add User Alias",
	Description: "Verify account preconditions, add an email alias to a user, and confirm the same account's alias collection. " +
		"Alias-domain policy and caller authorization are host-side bindings; the connector only verifies the pinned target account. " +
		"Does not prove or reserve global address availability.",
	Arguments: []*config.Field{
		{Name: argUserID, DisplayName: displayUserID, Description: "Stable Google user ID, not an email address.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasArg, DisplayName: "Alias Email", Description: "Exact alias address to add.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasExpectedPrimary, DisplayName: "Expected Primary Email", Description: "Required current primary-address precondition.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasExpectedCustomer, DisplayName: "Expected Customer ID", Description: "Required customer-ID precondition.", Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: []*config.Field{
		{Name: fieldSuccess, DisplayName: displaySuccess, Field: &config.Field_BoolField{}},
		{Name: "outcome", DisplayName: "Outcome", Description: "Qualified addition, presence, ownership, conflict, precondition or readback outcome.", Field: &config.Field_StringField{}},
		{Name: "alias_present_before", DisplayName: "Alias Present Before", Description: "Omitted when the initial alias collection could not be read.", Field: &config.Field_BoolField{}},
		{Name: "alias_present_after", DisplayName: "Alias Present After", Description: "Omitted when post-state is unknown, never defaulted to false.", Field: &config.Field_BoolField{}},
		{
			Name:        "observation_complete",
			DisplayName: "Observation Complete",
			Description: "Whether the target's alias post-state was observed; inspect the outcome and error separately.",
			Field:       &config.Field_BoolField{},
		},
		{Name: "insert_attempted", DisplayName: "Insert Attempted", Description: "True only once an insert was actually attempted against the provider.", Field: &config.Field_BoolField{}},
		{
			Name:        "insert_acknowledged",
			DisplayName: "Insert Acknowledged",
			Description: "True only when the provider insert returned success. False means no observed acknowledgement, not proof that no mutation occurred.",
			Field:       &config.Field_BoolField{},
		},
		{Name: argUserID, DisplayName: displayUserID, Description: "Requested stable target user ID.", Field: &config.Field_StringField{}},
		{Name: "primary_email", DisplayName: "Observed Primary Email", Field: &config.Field_StringField{}},
		{Name: "customer_id", DisplayName: "Observed Customer ID", Field: &config.Field_StringField{}},
	},
	ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
}

func (o *userResourceType) registerAddUserAliasAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, addUserAliasActionSchema, o.addUserAliasActionHandler)
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
	var trustedAccount bool
	result := func(outcome string) *structpb.Struct {
		fields := map[string]*structpb.Value{
			fieldSuccess:           structpb.NewBoolValue(after != nil && !*after && (outcome == aliasOutcomeDeleted || outcome == aliasOutcomeAlreadyAbsent)),
			"outcome":              structpb.NewStringValue(outcome),
			"observation_complete": structpb.NewBoolValue(after != nil),
			argUserID:              structpb.NewStringValue(userID),
		}
		if trustedAccount {
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
	if o.customerId == "" || strings.EqualFold(o.customerId, "my_customer") {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias actions require a concrete configured customer ID")
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
	if user == nil || user.Id == "" || user.PrimaryEmail == "" || user.CustomerId == "" {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias target response is incomplete")
	}
	if user.CustomerId != o.customerId {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.PermissionDenied, "google-workspace: alias target is outside the configured customer")
	}
	matchesAccount := func(candidate *admin.User) bool {
		return candidate != nil && candidate.Id == userID && candidate.CustomerId == customer && strings.EqualFold(candidate.PrimaryEmail, primary)
	}
	if !matchesAccount(user) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias target does not match the pinned account")
	}
	trustedAccount = true
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
	if err := withRateLimitWait(ctx, func() error {
		return o.client.DeleteUserAlias(ctx, userID, alias)
	}); err != nil && status.Code(err) != codes.NotFound {
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

// addUserAliasActionHandler implements the add_user_alias resource action:
// read the pinned target, verify identity/customer/primary preconditions, read
// the dedicated alias collection, insert once, and confirm by reading the
// same pinned target's alias collection again. Only observed post-state
// produces success; every uncertain outcome is qualified, and no unknown
// insert is ever retried.
func (o *userResourceType) addUserAliasActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
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
		return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: alias addition requires a stable user ID")
	}
	for _, value := range []string{alias, primary} {
		address, err := mail.ParseAddress(value)
		if err != nil || address.Name != "" || address.Address != value || !strings.Contains(value, "@") {
			return nil, nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: alias and primary email must be plain email addresses")
		}
	}

	var user *admin.User
	var before, after *bool
	var insertAttempted, insertAcknowledged bool
	var trustedAccount bool
	result := func(outcome string) *structpb.Struct {
		fields := map[string]*structpb.Value{
			fieldSuccess:           structpb.NewBoolValue(after != nil && *after && (outcome == aliasOutcomeAdded || outcome == aliasOutcomeAlreadyPresent)),
			"outcome":              structpb.NewStringValue(outcome),
			"observation_complete": structpb.NewBoolValue(after != nil),
			"insert_attempted":     structpb.NewBoolValue(insertAttempted),
			"insert_acknowledged":  structpb.NewBoolValue(insertAcknowledged),
			argUserID:              structpb.NewStringValue(userID),
		}
		if trustedAccount {
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
		return result(aliasOutcomePrimaryRejected), nil, uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: cannot add the primary address as an alias")
	}
	if o.customerId == "" || strings.EqualFold(o.customerId, "my_customer") {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias actions require a concrete configured customer ID")
	}
	completeAccount := func(candidate *admin.User) bool {
		return candidate != nil && candidate.Id != "" && candidate.PrimaryEmail != "" && candidate.CustomerId != ""
	}

	if o.client == nil || o.client.UserProvisioningService == nil {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: user provisioning service is required for alias addition")
	}

	readUser := newRateLimitWaitLoopValue[*admin.User](ctx)
	user, err = readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, userID) })
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return result(aliasOutcomeUserNotFound), nil, err
		}
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	if !completeAccount(user) {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias target response is incomplete")
	}
	// The authoritative configured-customer boundary: the provider record's
	// customer must match the connector's configured customer independent of
	// the caller-submitted expected_customer_id (never trusted alone). The
	// submitted pin is additionally checked by matchesAccount. The alias's
	// domain is deliberately NOT checked against the configured sync domain:
	// provider-verified secondary domains in the same customer are allowed.
	if user.CustomerId != o.customerId {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.PermissionDenied, "google-workspace: alias target is outside the configured customer")
	}
	matchesAccount := func(candidate *admin.User) bool {
		return candidate != nil && candidate.Id == userID && candidate.CustomerId == customer && strings.EqualFold(candidate.PrimaryEmail, primary)
	}
	if !matchesAccount(user) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias target does not match the pinned account")
	}
	trustedAccount = true

	// A dedicated alias collection read is authoritative; omission of the
	// optional User.Aliases profile field is not evidence of absence.
	readAliases := newRateLimitWaitLoopValue[[]string](ctx)
	aliases, err := readAliases(func() ([]string, error) { return o.client.ListUserAliases(ctx, userID) })
	if err != nil {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	present := aliasListHas(aliases, alias)
	before = &present
	if present {
		// Already attached to the same pinned target: an idempotent
		// already_present result with verified state. No insert was attempted
		// or acknowledged.
		after = &present
		return result(aliasOutcomeAlreadyPresent), nil, nil
	}
	// Same-account non-editable aliases cannot be added as editable aliases;
	// checking first avoids classifying them as foreign/group conflicts after
	// a doomed insert (same semantics as remove_user_alias's pre-check).
	if aliasListHas(user.NonEditableAliases, alias) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: non-editable alias cannot be added or replaced")
	}
	// The alias is not on the pinned target. Foreign user ownership is
	// distinguishable via a read-by-alias: if another user owns the address,
	// fail now instead of forcing a provider conflict.
	owner, ownerErr := readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, alias) })
	if ownerErr == nil {
		if !completeAccount(owner) {
			return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias owner response is incomplete")
		}
		if !matchesAccount(owner) {
			return result(aliasOutcomeOwnerMismatch), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias resolves to a different account")
		}
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.Aborted, "google-workspace: alias ownership changed during verification")
	}
	if status.Code(ownerErr) != codes.NotFound {
		return result(aliasOutcomeReadbackUnknown), nil, ownerErr
	}
	// A user-lookup 404 is not global availability: a group or another
	// namespace may still own the address. The insert's provider conflict
	// is the authoritative signal for those.

	insertAttempted = true
	// Wait once on a retryable error, then return it without replaying the write.
	err = withRateLimitWait(ctx, func() error { return o.client.InsertUserAlias(ctx, userID, alias) })
	if err != nil {
		if status.Code(err) == codes.Aborted { // provider 409: address owned elsewhere
			// Bounded qualified readback ONLY to establish same-account replay.
			// Re-read and revalidate the pinned target identity first: a replay
			// acceptance requires the same account to still be the target.
			replayUser, replayUserErr := readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, userID) })
			if replayUserErr != nil {
				// Identity could not be revalidated: the conflict result stays
				// UNKNOWN, not alias_conflict and not success.
				return result(aliasOutcomeReadbackUnknown), nil, replayUserErr
			}
			if !completeAccount(replayUser) {
				return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias target readback is incomplete")
			}
			if !matchesAccount(replayUser) {
				return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.Aborted, "google-workspace: account identity or preconditions changed during alias conflict resolution")
			}
			replayAliases, replayErr := readAliases(func() ([]string, error) { return o.client.ListUserAliases(ctx, userID) })
			if replayErr != nil {
				// A failed replay readback preserves the readback error and
				// uncertainty (insert_attempted=true, insert_acknowledged=false,
				// alias_present_after omitted) — never alias_conflict.
				return result(aliasOutcomeReadbackUnknown), nil, replayErr
			}
			presentAfter := aliasListHas(replayAliases, alias)
			after = &presentAfter
			if presentAfter {
				// Same-account replay: the concurrent duplicate landed on this
				// verified target. This insert was not acknowledged.
				return result(aliasOutcomeAlreadyPresent), nil, nil
			}
			// The target's collection does not carry the alias. A bounded
			// by-alias lookup distinguishes foreign user ownership from
			// group/namespace conflict; 404 only qualifies the latter and is
			// never availability.
			replayOwner, replayOwnerErr := readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, alias) })
			if replayOwnerErr == nil {
				if !completeAccount(replayOwner) {
					return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias owner readback is incomplete")
				}
				if replayOwner.Id == userID {
					return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.Aborted, "google-workspace: alias owner and target collection disagree after insert conflict")
				}
				return result(aliasOutcomeOwnerMismatch), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias resolves to a different account after insert conflict")
			}
			if status.Code(replayOwnerErr) != codes.NotFound {
				return result(aliasOutcomeReadbackUnknown), nil, replayOwnerErr
			}
			return result(aliasOutcomeConflict), nil, uhttp.WrapErrors(codes.FailedPrecondition,
				fmt.Sprintf("google-workspace: alias %s conflicts with another user, group, or namespace; provider insert conflict stands", alias))
		}
		// Permission, timeout, or any other provider error: preserve the
		// error and uncertainty; never silently claim success, never retry.
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	insertAcknowledged = true

	// Acknowledged insert: read back the exact pinned target and its
	// dedicated alias collection. Only observed presence produces success.
	observed, err := readUser(func() (*admin.User, error) { return o.client.GetUserFullForProvisioning(ctx, userID) })
	if err != nil {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	if !completeAccount(observed) {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias target readback is incomplete")
	}
	if !matchesAccount(observed) {
		return result(aliasOutcomePreconditionFailed), nil, uhttp.WrapErrors(codes.Aborted, "google-workspace: account identity or preconditions changed during alias addition")
	}
	aliases, err = readAliases(func() ([]string, error) { return o.client.ListUserAliases(ctx, userID) })
	if err != nil {
		return result(aliasOutcomeReadbackUnknown), nil, err
	}
	attached := aliasListHas(aliases, alias)
	after = &attached
	if !attached {
		return result(aliasOutcomeReadbackUnknown), nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias absent after acknowledged insert")
	}
	return result(aliasOutcomeAdded), nil, nil
}
