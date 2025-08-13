package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	admin "google.golang.org/api/admin/directory/v1"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/protobuf/types/known/structpb"
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
	userService.Users.Update(userId, &admin.User{
		Suspended: isSuspended,
	}).Context(ctx).Do()

	response := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": {
				Kind: &structpb.Value_BoolValue{BoolValue: true},
			},
		},
	}

	return &response, nil, nil
}
