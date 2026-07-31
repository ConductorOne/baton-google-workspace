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
}
