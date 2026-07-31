package connector

import (
	"context"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
)

// TestUserResource_ResourceLevelAttributes locks in the migration away from
// the legacy UserTrait-level options (WithStatus/WithDetailedStatus/
// WithUserIcon/WithCreatedAt/WithUserProfile) toward their Resource-level
// replacements (WithResourceStatus/WithResourceIcon/WithResourceCreatedAt/
// WithResourceProfile) in userResource(): status, icon, created_at, and
// profile must land on the *v2.Resource* itself, not only on the trait.
func TestUserResource_ResourceLevelAttributes(t *testing.T) {
	o := &userResourceType{resourceType: resourceTypeUser}

	t.Run("enabled user: status, icon, created_at, profile all set on resource", func(t *testing.T) {
		user := &directoryAdmin.User{
			Id:                "user123",
			PrimaryEmail:      "test@example.com",
			Name:              &directoryAdmin.UserName{GivenName: "Test", FamilyName: "User", FullName: "Test User"},
			ThumbnailPhotoUrl: "https://example.com/photo.jpg",
			CreationTime:      "2020-01-02T03:04:05Z",
		}

		res, err := o.userResource(context.Background(), user)
		require.NoError(t, err)

		require.NotNil(t, res.GetStatus())
		require.Equal(t, v2.Status_RESOURCE_STATUS_ENABLED, res.GetStatus().GetStatus())

		require.NotNil(t, res.GetIcon())
		require.Equal(t, "https://example.com/photo.jpg", res.GetIcon().GetId())

		require.NotNil(t, res.GetCreatedAt())
		require.Equal(t, int64(1577934245), res.GetCreatedAt().AsTime().Unix())

		require.NotNil(t, res.GetProfile())
		require.Equal(t, "user123", res.GetProfile().GetFields()[argUserID].GetStringValue())

		// Regression guard: WithAnnotation appends rather than dedupes, so a
		// duplicated resourceOpts append (e.g. from a bad merge/rebase) would
		// silently give every synced user resource two identical V1Identifier
		// annotations instead of failing loudly.
		v1IdentifierCount := 0
		for _, a := range res.GetAnnotations() {
			if strings.Contains(a.GetTypeUrl(), "V1Identifier") {
				v1IdentifierCount++
			}
		}
		require.Equal(t, 1, v1IdentifierCount, "expected exactly one V1Identifier annotation, got a duplicate")
	})

	t.Run("suspended user: DISABLED status wins over the initial detailed status", func(t *testing.T) {
		user := &directoryAdmin.User{
			Id:           "user456",
			PrimaryEmail: "suspended@example.com",
			Name:         &directoryAdmin.UserName{GivenName: "Sus", FamilyName: "Pended", FullName: "Sus Pended"},
			Suspended:    true,
		}

		res, err := o.userResource(context.Background(), user)
		require.NoError(t, err)

		require.NotNil(t, res.GetStatus())
		require.Equal(t, v2.Status_RESOURCE_STATUS_DISABLED, res.GetStatus().GetStatus())
	})

	t.Run("deleted user: DELETED status wins as the last status set", func(t *testing.T) {
		user := &directoryAdmin.User{
			Id:           "user789",
			PrimaryEmail: "deleted@example.com",
			Name:         &directoryAdmin.UserName{GivenName: "Del", FamilyName: "Eted", FullName: "Del Eted"},
			DeletionTime: "2020-01-01T00:00:00Z",
		}

		res, err := o.userResource(context.Background(), user)
		require.NoError(t, err)

		require.NotNil(t, res.GetStatus())
		require.Equal(t, v2.Status_RESOURCE_STATUS_DELETED, res.GetStatus().GetStatus())
	})

	// Regression guard: update_user_profile/update_user write job_title,
	// employee_type, and employee_id, so a push rule must be able to read
	// those same keys back from the synced profile - otherwise it can never
	// observe the value it wrote and either no-ops forever or re-pushes every
	// sync. These must stay present alongside the pre-existing "title"/
	// "description" keys (never removed/renamed - see CLAUDE.md on profile
	// stability), just additively aliased under the write-side names.
	t.Run("job_title, employee_type, and employee_id round-trip under the same keys the write path uses", func(t *testing.T) {
		user := &directoryAdmin.User{
			Id:           "user999",
			PrimaryEmail: "roundtrip@example.com",
			Name:         &directoryAdmin.UserName{GivenName: "Round", FamilyName: "Trip", FullName: "Round Trip"},
			Organizations: []directoryAdmin.UserOrganization{
				{Primary: true, Title: "Staff Engineer", Description: "Full-time"},
			},
			ExternalIds: []directoryAdmin.UserExternalId{
				{Type: "organization", Value: "E12345"},
			},
		}

		res, err := o.userResource(context.Background(), user)
		require.NoError(t, err)

		profile := res.GetProfile().GetFields()
		require.Equal(t, "Staff Engineer", profile["title"].GetStringValue(), "pre-existing title key must be unchanged")
		require.Equal(t, "Staff Engineer", profile[argJobTitle].GetStringValue())
		require.Equal(t, "Full-time", profile["description"].GetStringValue(), "pre-existing description key must be unchanged")
		require.Equal(t, "Full-time", profile[argEmployeeType].GetStringValue())
		require.Equal(t, "E12345", profile[argEmployeeID].GetStringValue())
	})

	// Regression guard: employeeIDs is a mapset.Set, so ToSlice() returns
	// elements in nondeterministic order. Joining unsorted would make the
	// profile's employee_id value churn between syncs (and re-trigger push
	// rules) purely from iteration-order jitter, even when the underlying set
	// of external IDs hasn't changed. Run repeatedly since a single run can
	// pass by chance regardless of sorting.
	t.Run("multiple organization-type external IDs join deterministically regardless of set iteration order", func(t *testing.T) {
		user := &directoryAdmin.User{
			Id:           "user111",
			PrimaryEmail: "multi@example.com",
			Name:         &directoryAdmin.UserName{GivenName: "Multi", FamilyName: "ID", FullName: "Multi ID"},
			ExternalIds: []directoryAdmin.UserExternalId{
				{Type: "organization", Value: "E-ZEBRA"},
				{Type: "organization", Value: "E-APPLE"},
				{Type: "organization", Value: "E-MANGO"},
			},
		}

		for i := 0; i < 20; i++ {
			res, err := o.userResource(context.Background(), user)
			require.NoError(t, err)
			require.Equal(t, "E-APPLE,E-MANGO,E-ZEBRA", res.GetProfile().GetFields()[argEmployeeID].GetStringValue())
		}
	})
}
