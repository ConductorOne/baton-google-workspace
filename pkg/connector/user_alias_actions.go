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
	aliasOutcomeDeleted        = "deleted"
	aliasOutcomeAlreadyAbsent  = "already_absent"
	aliasOutcomeAdded          = "added"
	aliasOutcomeAlreadyPresent = "already_present"
	aliasArg                   = "alias"
)

var removeUserAliasActionSchema = &v2.BatonActionSchema{
	Name:        "remove_user_alias",
	DisplayName: "Remove User Alias",
	Description: "Remove an alias from the selected user.",
	Arguments: []*config.Field{
		{Name: argUserID, DisplayName: displayUserID, Description: "Stable Google user ID.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasArg, DisplayName: "Alias Email", Description: "Exact alias address to remove.", Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: []*config.Field{
		{Name: fieldSuccess, DisplayName: displaySuccess, Field: &config.Field_BoolField{}},
		{Name: "outcome", DisplayName: "Outcome", Field: &config.Field_StringField{}},
	},
	ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
}

func (o *userResourceType) registerRemoveUserAliasAction(ctx context.Context, registry actions.ActionRegistry) error {
	return registry.Register(ctx, removeUserAliasActionSchema, o.removeUserAliasActionHandler)
}

var addUserAliasActionSchema = &v2.BatonActionSchema{
	Name:        "add_user_alias",
	DisplayName: "Add User Alias",
	Description: "Add an alias to the selected user.",
	Arguments: []*config.Field{
		{Name: argUserID, DisplayName: displayUserID, Description: "Stable Google user ID.", Field: &config.Field_StringField{}, IsRequired: true},
		{Name: aliasArg, DisplayName: "Alias Email", Description: "Exact alias address to add.", Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: []*config.Field{
		{Name: fieldSuccess, DisplayName: displaySuccess, Field: &config.Field_BoolField{}},
		{Name: "outcome", DisplayName: "Outcome", Field: &config.Field_StringField{}},
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

func aliasResult(outcome string) *structpb.Struct {
	return actions.NewReturnValues(true, actions.NewStringReturnField("outcome", outcome))
}

func validateAliasAction(o *userResourceType, args *structpb.Struct) (string, string, error) {
	userID, err := requiredStringArg(args, argUserID)
	if err != nil {
		return "", "", err
	}
	alias, err := requiredStringArg(args, aliasArg)
	if err != nil {
		return "", "", err
	}
	if strings.Contains(userID, "@") {
		return "", "", uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: alias actions require a stable user ID")
	}
	address, err := mail.ParseAddress(alias)
	if err != nil || address.Name != "" || address.Address != alias || !strings.Contains(alias, "@") {
		return "", "", uhttp.WrapErrors(codes.InvalidArgument, "google-workspace: alias must be a plain email address")
	}
	if o.customerId == "" || strings.EqualFold(o.customerId, "my_customer") {
		return "", "", uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias actions require a concrete configured customer ID")
	}
	if o.client == nil || o.client.UserProvisioningService == nil {
		return "", "", uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: user provisioning service is required for alias actions")
	}
	return userID, alias, nil
}

func (o *userResourceType) aliasTarget(ctx context.Context, userID, alias string) (*admin.User, error) {
	user, err := o.client.GetUserFullForProvisioning(ctx, userID)
	if err != nil {
		return nil, err
	}
	if user == nil || user.Id != userID || user.PrimaryEmail == "" || user.CustomerId == "" {
		return nil, uhttp.WrapErrors(codes.DataLoss, "google-workspace: alias target response is incomplete or mismatched")
	}
	if user.CustomerId != o.customerId {
		return nil, uhttp.WrapErrors(codes.PermissionDenied, "google-workspace: alias target is outside the configured customer")
	}
	if strings.EqualFold(user.PrimaryEmail, alias) {
		return nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: primary email cannot be changed as an alias")
	}
	return user, nil
}

func (o *userResourceType) removeUserAliasActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	userID, alias, err := validateAliasAction(o, args)
	if err != nil {
		return nil, nil, err
	}
	user, err := o.aliasTarget(ctx, userID, alias)
	if err != nil {
		return nil, nil, err
	}
	if aliasListHas(user.NonEditableAliases, alias) {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: non-editable alias cannot be removed")
	}
	aliases, err := o.client.ListUserAliases(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if !aliasListHas(aliases, alias) {
		return aliasResult(aliasOutcomeAlreadyAbsent), nil, nil
	}
	if err := o.client.DeleteUserAlias(ctx, userID, alias); err != nil && status.Code(err) != codes.NotFound {
		return nil, nil, err
	}
	aliases, err = o.client.ListUserAliases(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if aliasListHas(aliases, alias) {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias remains after deletion")
	}
	return aliasResult(aliasOutcomeDeleted), nil, nil
}

func (o *userResourceType) addUserAliasActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	userID, alias, err := validateAliasAction(o, args)
	if err != nil {
		return nil, nil, err
	}
	user, err := o.aliasTarget(ctx, userID, alias)
	if err != nil {
		return nil, nil, err
	}
	aliases, err := o.client.ListUserAliases(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if aliasListHas(aliases, alias) {
		return aliasResult(aliasOutcomeAlreadyPresent), nil, nil
	}
	if aliasListHas(user.NonEditableAliases, alias) {
		return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: non-editable alias cannot be added")
	}
	err = o.client.InsertUserAlias(ctx, userID, alias)
	if err != nil && status.Code(err) != codes.Aborted {
		return nil, nil, err
	}
	aliases, readErr := o.client.ListUserAliases(ctx, userID)
	if readErr != nil {
		return nil, nil, readErr
	}
	if aliasListHas(aliases, alias) {
		if status.Code(err) == codes.Aborted {
			return aliasResult(aliasOutcomeAlreadyPresent), nil, nil
		}
		return aliasResult(aliasOutcomeAdded), nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return nil, nil, uhttp.WrapErrors(codes.FailedPrecondition, "google-workspace: alias absent after insertion")
}
