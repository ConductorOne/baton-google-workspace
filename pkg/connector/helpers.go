package connector

import (
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	MembershipEntitlementIDTemplate = "membership:%s"
	GrantIDTemplate                 = "grant:%s:%s"
	groupSettingTrue                = "true"
	groupSettingFalse               = "false"
)

// GroupSettingUpdateResult holds the result of applying a group setting update.
type GroupSettingUpdateResult struct {
	NeedsUpdate    bool
	PreviousValue  string
	NewValue       string
	ForceSendField string
}

func v1AnnotationsForResourceType(resourceTypeID string) annotations.Annotations {
	annos := annotations.New(
		&v2.V1Identifier{
			Id: resourceTypeID,
		},
	)
	if resourceTypeID == "user" {
		annos.Update(&v2.SkipEntitlementsAndGrants{})
	}
	return annos
}

// Convert accepts a list of T and returns a list of R based on the input func.
func Convert[T any, R any](slice []T, f func(in T) R) []R {
	ret := make([]R, 0, len(slice))
	for _, t := range slice {
		ret = append(ret, f(t))
	}
	return ret
}

type GoogleWorkspaceOAuthUnauthorizedError struct {
	o *oauth2.RetrieveError
}

func (g *GoogleWorkspaceOAuthUnauthorizedError) Error() string {
	return g.o.Error()
}

func V1GrantID(entitlementID string, userID string) string {
	return fmt.Sprintf(GrantIDTemplate, entitlementID, userID)
}

func V1MembershipEntitlementID(resourceID string) string {
	return fmt.Sprintf(MembershipEntitlementIDTemplate, resourceID)
}

// emailsEqual compares two email addresses after trimming whitespace and case-insensitive comparison.
func emailsEqual(email1 string, email2 string) bool {
	// Trim whitespace and use EqualFold for efficient case-insensitive comparison
	return strings.EqualFold(strings.TrimSpace(email1), strings.TrimSpace(email2))
}

// extractUserId extracts and validates the user_id argument from action args.
func extractUserId(args *structpb.Struct, l *zap.Logger, actionName string) (string, error) {
	userIdValue, ok := args.Fields["user_id"]
	if !ok || userIdValue == nil {
		l.Debug("google-workspace: user action handler: missing user_id argument", zap.String("action", actionName), zap.Any("args", args))
		return "", fmt.Errorf("missing user_id argument")
	}
	userIdField, ok := userIdValue.GetKind().(*structpb.Value_StringValue)
	if !ok || userIdField.StringValue == "" {
		return "", fmt.Errorf("invalid user_id argument")
	}
	return userIdField.StringValue, nil
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

// applyBooleanGroupSetting applies a boolean group setting and returns the update result.
func applyBooleanGroupSetting(
	currentValue string,
	desiredValue bool,
	forceSendField string,
) GroupSettingUpdateResult {
	result := GroupSettingUpdateResult{
		PreviousValue:  currentValue,
		ForceSendField: forceSendField,
	}
	currentBool := strings.EqualFold(currentValue, groupSettingTrue)
	if currentBool != desiredValue {
		result.NeedsUpdate = true
		if desiredValue {
			result.NewValue = groupSettingTrue
		} else {
			result.NewValue = groupSettingFalse
		}
	} else {
		result.NewValue = currentValue
	}
	return result
}

// applyStringGroupSetting applies a string group setting and returns the update result.
func applyStringGroupSetting(
	currentValue string,
	desiredValue string,
	forceSendField string,
) GroupSettingUpdateResult {
	result := GroupSettingUpdateResult{
		PreviousValue:  currentValue,
		ForceSendField: forceSendField,
	}
	if currentValue != desiredValue {
		result.NeedsUpdate = true
		result.NewValue = desiredValue
	} else {
		result.NewValue = currentValue
	}
	return result
}
