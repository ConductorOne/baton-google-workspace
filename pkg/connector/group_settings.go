package connector

import (
	"fmt"
	"slices"
	"strconv"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var groupPrivacyFields = []*config.Field{
	{
		Name:        "who_can_view_group",
		DisplayName: "Who Can View Conversations",
		Field: &config.Field_StringField{
			StringField: &config.StringField{Rules: &config.StringRules{In: []string{"ANYONE_CAN_VIEW", "ALL_IN_DOMAIN_CAN_VIEW", "ALL_MEMBERS_CAN_VIEW", "ALL_MANAGERS_CAN_VIEW"}}},
		},
	},
	{
		Name:        "who_can_view_membership",
		DisplayName: "Who Can View Membership",
		Field:       &config.Field_StringField{StringField: &config.StringField{Rules: &config.StringRules{In: []string{"ALL_IN_DOMAIN_CAN_VIEW", "ALL_MEMBERS_CAN_VIEW", "ALL_MANAGERS_CAN_VIEW"}}}},
	},
	{
		Name:        "who_can_discover_group",
		DisplayName: "Who Can Discover Group",
		Field: &config.Field_StringField{
			StringField: &config.StringField{Rules: &config.StringRules{In: []string{"ANYONE_CAN_DISCOVER", "ALL_IN_DOMAIN_CAN_DISCOVER", "ALL_MEMBERS_CAN_DISCOVER"}}},
		},
	},
	{Name: "include_in_global_address_list", DisplayName: "Include in Global Address List", Field: &config.Field_BoolField{}},
	{
		Name:        "who_can_join",
		DisplayName: "Who Can Join",
		Field: &config.Field_StringField{
			StringField: &config.StringField{Rules: &config.StringRules{In: []string{"ANYONE_CAN_JOIN", "ALL_IN_DOMAIN_CAN_JOIN", "INVITED_CAN_JOIN", "CAN_REQUEST_TO_JOIN"}}},
		},
	},
}

func groupPrivacyReturnFields() []*config.Field {
	fields := []*config.Field{
		{
			Name:        fieldResource,
			DisplayName: "Observed Group",
			Description: "Group resource with settings observed after the update, not an echo of requested values.",
			Field:       &config.Field_ResourceField{},
		},
	}
	for _, field := range groupPrivacyFields {
		for _, prefix := range []string{"previous_", "new_"} {
			fields = append(
				fields,
				&config.Field{
					Name:        prefix + field.Name,
					DisplayName: prefix + field.DisplayName,
					Description: "Previous or requested value. See the returned resource for observed settings.",
					Field:       &config.Field_StringField{},
				},
			)
		}
	}
	return fields
}

func parseGroupPrivacy(args *structpb.Struct) (map[string]string, error) {
	values := make(map[string]string)
	for _, field := range groupPrivacyFields {
		value, present := args.GetFields()[field.Name]
		if !present || value == nil {
			continue
		}
		if _, isNull := value.GetKind().(*structpb.Value_NullValue); isNull {
			continue
		}
		if field.Name == "include_in_global_address_list" {
			boolean, ok := value.GetKind().(*structpb.Value_BoolValue)
			if !ok {
				return nil, fmt.Errorf("google-workspace: %s must be a boolean", field.Name)
			}
			values[field.Name] = strconv.FormatBool(boolean.BoolValue)
			continue
		}
		str, ok := value.GetKind().(*structpb.Value_StringValue)
		if !ok || !slices.Contains(field.GetStringField().GetRules().GetIn(), str.StringValue) {
			return nil, fmt.Errorf("google-workspace: invalid %s", field.Name)
		}
		values[field.Name] = str.StringValue
	}
	return values, nil
}

func groupSettingsProfile(settings *groupssettings.Groups) map[string]any {
	profile := make(map[string]any)
	if settings == nil {
		return profile
	}
	for key, value := range map[string]string{
		"allow_external_members":         settings.AllowExternalMembers,
		"allow_web_posting":              settings.AllowWebPosting,
		"who_can_post_message":           settings.WhoCanPostMessage,
		"message_moderation_level":       settings.MessageModerationLevel,
		"who_can_view_group":             settings.WhoCanViewGroup,
		"who_can_view_membership":        settings.WhoCanViewMembership,
		"who_can_discover_group":         settings.WhoCanDiscoverGroup,
		"include_in_global_address_list": settings.IncludeInGlobalAddressList,
		"who_can_join":                   settings.WhoCanJoin,
	} {
		if value != "" {
			profile[key] = value
		}
	}
	return profile
}

func addGroupSettings(resource *v2.Resource, settings *groupssettings.Groups) error {
	settingsProfile := groupSettingsProfile(settings)
	if len(settingsProfile) == 0 {
		return status.Error(codes.DataLoss, "google-workspace: group settings response contains no supported settings")
	}
	profile := resource.GetProfile().AsMap()
	profile["group_settings"] = settingsProfile
	updated, err := structpb.NewStruct(profile)
	if err != nil {
		return err
	}
	resource.Profile = updated
	return nil
}
