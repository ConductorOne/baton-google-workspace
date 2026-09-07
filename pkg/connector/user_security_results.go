package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// securityResultFields extends the existing mutations, not a separate inventory action.
func securityResultFields() []*config.Field {
	return []*config.Field{
		{Name: "deleted_ids", DisplayName: "Deleted IDs", Field: &config.Field_StringSliceField{}},
		{Name: "already_absent_ids", DisplayName: "Already Absent IDs", Field: &config.Field_StringSliceField{}},
		{Name: "failed_ids", DisplayName: "Failed IDs", Field: &config.Field_StringSliceField{}},
		{Name: "skipped_ids", DisplayName: "Skipped IDs", Field: &config.Field_StringSliceField{}},
		{Name: "remaining_ids", DisplayName: "Remaining IDs", Field: &config.Field_StringSliceField{}},
		{
			Name:        "inventory_complete",
			DisplayName: "Inventory Complete",
			Description: "Whether the final provider enumeration completed. False is not evidence of absence.",
			Field:       &config.Field_BoolField{},
		},
		{Name: "observed_at", DisplayName: "Observed At", Field: &config.Field_StringField{}},
	}
}

// revokeUserCredentials retains both failed per-item results and final enumeration.
// The SDK publishes return values on FAILED actions, so partial results survive without
// downgrading a failed revocation to a successful handler completion.
func revokeUserCredentials(ctx context.Context, countField string, list func() ([]string, error), revoke func(string) error) (*structpb.Struct, error) {
	var deleted, absent, failed, skipped, remaining []string
	var failures []error
	complete := false
	err := ctx.Err()
	var ids []string
	if err == nil {
		ids, err = list()
	}
	if err != nil {
		failures = append(failures, err)
	} else {
		waitLoop := newRateLimitWaitLoop(ctx)
		for _, id := range ids {
			if ctx.Err() != nil {
				break
			}
			if id == "" {
				skipped = append(skipped, id)
				continue
			}
			err := waitLoop(func() error { return revoke(id) })
			switch {
			case err == nil:
				deleted = append(deleted, id)
			case status.Code(err) == codes.NotFound:
				absent = append(absent, id)
			default:
				failed = append(failed, id)
				failures = append(failures, err)
			}
		}
		if len(skipped) != 0 {
			failures = append(failures, fmt.Errorf("google-workspace: skipped %d credential entries with missing identity", len(skipped)))
		}
		if err := ctx.Err(); err != nil {
			failures = append(failures, fmt.Errorf("google-workspace: credential revocation interrupted: %w", err))
		} else if len(ids) == 0 {
			complete = true
		} else {
			remaining, err = list()
			if err != nil {
				failures = append(failures, fmt.Errorf("google-workspace: final credential enumeration failed: %w", err))
			} else {
				complete = true
				for _, id := range remaining {
					if id == "" {
						complete = false
					}
				}
				if len(remaining) != 0 {
					failures = append(failures, fmt.Errorf("google-workspace: %d credential entries remain after revocation", len(remaining)))
				}
			}
		}
	}
	err = errors.Join(failures...)
	return actions.NewReturnValues(err == nil && complete,
		actions.NewNumberReturnField(countField, float64(len(deleted)+len(absent))),
		actions.NewStringListReturnField("deleted_ids", deleted),
		actions.NewStringListReturnField("already_absent_ids", absent),
		actions.NewStringListReturnField("failed_ids", failed),
		actions.NewStringListReturnField("skipped_ids", skipped),
		actions.NewStringListReturnField("remaining_ids", remaining),
		actions.NewBoolReturnField("inventory_complete", complete),
		actions.NewStringReturnField("observed_at", time.Now().UTC().Format(time.RFC3339Nano))), err
}
