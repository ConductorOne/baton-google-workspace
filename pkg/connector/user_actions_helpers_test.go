package connector

import (
	"testing"

	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
)

func strPtr(s string) *string { return &s }

func TestBuildUpdatedOrganizations(t *testing.T) {
	t.Run("updates primary in place, preserves siblings and secondary orgs", func(t *testing.T) {
		orgs := []*directoryAdmin.UserOrganization{
			{Primary: true, Department: "Old Dept", Title: "Old Title", CostCenter: "CC1"},
			{Primary: false, Department: "Secondary Dept"},
		}
		patch := userProfilePatch{department: strPtr("New Dept")}

		updated := buildUpdatedOrganizations(orgs, patch)

		require.Len(t, updated, 2)
		require.True(t, updated[0].Primary)
		require.Equal(t, "New Dept", updated[0].Department)
		require.Equal(t, "Old Title", updated[0].Title)
		require.Equal(t, "CC1", updated[0].CostCenter)
		require.Equal(t, "Secondary Dept", updated[1].Department)
	})

	t.Run("falls back to orgs[0] when none flagged primary", func(t *testing.T) {
		orgs := []*directoryAdmin.UserOrganization{
			{Department: "Sales", Title: "Rep", CostCenter: "CC9"},
		}
		patch := userProfilePatch{department: strPtr("Engineering")}

		updated := buildUpdatedOrganizations(orgs, patch)

		require.Len(t, updated, 1)
		require.True(t, updated[0].Primary, "existing org must be promoted to primary")
		require.Equal(t, "Engineering", updated[0].Department)
		require.Equal(t, "Rep", updated[0].Title)
		require.Equal(t, "CC9", updated[0].CostCenter)
	})

	t.Run("creates a new primary org when none exist", func(t *testing.T) {
		updated := buildUpdatedOrganizations(nil, userProfilePatch{employeeType: strPtr("Contractor")})

		require.Len(t, updated, 1)
		require.True(t, updated[0].Primary)
		require.Equal(t, "Contractor", updated[0].Description)
	})

	t.Run("employee type maps to Description, force-sent even when empty", func(t *testing.T) {
		orgs := []*directoryAdmin.UserOrganization{{Primary: true, Description: "Full-time"}}
		updated := buildUpdatedOrganizations(orgs, userProfilePatch{employeeType: strPtr("")})

		require.Len(t, updated, 1)
		require.Equal(t, "", updated[0].Description)
		require.Contains(t, updated[0].ForceSendFields, "Description")
	})

	t.Run("no organizations and only empty-string clears: does not create a phantom org", func(t *testing.T) {
		updated := buildUpdatedOrganizations(nil, userProfilePatch{
			department:   strPtr(""),
			employeeType: strPtr(""),
		})

		require.Len(t, updated, 0, "must not fabricate an empty primary organization when there is nothing to persist")
	})

	t.Run("no organizations, one empty clear and one real value: still creates the org", func(t *testing.T) {
		updated := buildUpdatedOrganizations(nil, userProfilePatch{
			department: strPtr(""),
			jobTitle:   strPtr("Engineer"),
		})

		require.Len(t, updated, 1)
		require.True(t, updated[0].Primary)
		require.Equal(t, "Engineer", updated[0].Title)
	})
}

func TestBuildUpdatedExternalIDs(t *testing.T) {
	t.Run("replaces organization entry, preserves other types", func(t *testing.T) {
		ids := []*directoryAdmin.UserExternalId{
			{Type: "organization", Value: "E-OLD"},
			{Type: "login_id", Value: "alogin"},
		}
		updated := buildUpdatedExternalIDs(ids, "E-NEW")

		require.Len(t, updated, 2)
		byType := map[string]string{}
		for _, id := range updated {
			byType[id.Type] = id.Value
		}
		require.Equal(t, "E-NEW", byType["organization"])
		require.Equal(t, "alogin", byType["login_id"])
	})

	t.Run("empty employeeID removes the organization entry entirely", func(t *testing.T) {
		ids := []*directoryAdmin.UserExternalId{{Type: "organization", Value: "E-OLD"}}
		updated := buildUpdatedExternalIDs(ids, "")

		require.NotNil(t, updated, "must be non-nil so the clear is actually sent on the wire")
		require.Len(t, updated, 0)
	})
}

func TestBuildManagerRelations(t *testing.T) {
	t.Run("replaces manager relation, preserves other relation types", func(t *testing.T) {
		relations := []*directoryAdmin.UserRelation{
			{Type: "manager", Value: "old-manager@example.com"},
			{Type: "assistant", Value: "assistant@example.com"},
		}
		updated := buildManagerRelations(relations, "new-manager@example.com")

		require.Len(t, updated, 2)
		byType := map[string]string{}
		for _, r := range updated {
			byType[r.Type] = r.Value
		}
		require.Equal(t, "new-manager@example.com", byType["manager"])
		require.Equal(t, "assistant@example.com", byType["assistant"])
	})

	t.Run("adds manager relation when none exists", func(t *testing.T) {
		updated := buildManagerRelations(nil, "manager@example.com")

		require.Len(t, updated, 1)
		require.Equal(t, "manager", updated[0].Type)
		require.Equal(t, "manager@example.com", updated[0].Value)
	})
}
