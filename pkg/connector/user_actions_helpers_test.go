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

		updated, changed := buildUpdatedOrganizations(orgs, patch)

		require.True(t, changed)
		require.Len(t, updated, 2)
		require.True(t, updated[0].Primary)
		require.Equal(t, "New Dept", updated[0].Department)
		require.Equal(t, "Old Title", updated[0].Title)
		require.Equal(t, "CC1", updated[0].CostCenter)
		require.Equal(t, "Secondary Dept", updated[1].Department)
	})

	t.Run("falls back to orgs[0] when none flagged primary, without persisting a Primary flag Google never set", func(t *testing.T) {
		orgs := []*directoryAdmin.UserOrganization{
			{Department: "Sales", Title: "Rep", CostCenter: "CC9"},
		}
		patch := userProfilePatch{department: strPtr("Engineering")}

		updated, changed := buildUpdatedOrganizations(orgs, patch)

		require.True(t, changed)
		require.Len(t, updated, 1)
		require.False(t, updated[0].Primary, "editing the fallback-selected org must not silently promote it to primary")
		require.Equal(t, "Engineering", updated[0].Department)
		require.Equal(t, "Rep", updated[0].Title)
		require.Equal(t, "CC9", updated[0].CostCenter)
	})

	t.Run("multiple orgs, none flagged primary: edits orgs[0] and leaves the rest untouched", func(t *testing.T) {
		orgs := []*directoryAdmin.UserOrganization{
			{Department: "Sales", Title: "Rep", CostCenter: "CC9"},
			{Department: "Marketing", Title: "Analyst", CostCenter: "CC7"},
		}
		patch := userProfilePatch{department: strPtr("Engineering")}

		updated, changed := buildUpdatedOrganizations(orgs, patch)

		require.True(t, changed)
		require.Len(t, updated, 2, "the fallback must never drop a sibling organization entry")
		require.False(t, updated[0].Primary, "editing the fallback-selected org must not silently promote it to primary")
		require.Equal(t, "Engineering", updated[0].Department, "orgs[0] is the entry the current fallback picks")
		require.Equal(t, "Rep", updated[0].Title)
		require.Equal(t, "CC9", updated[0].CostCenter)
		require.Equal(t, "Marketing", updated[1].Department, "the second entry must survive verbatim, not be silently mutated instead")
		require.Equal(t, "Analyst", updated[1].Title)
		require.Equal(t, "CC7", updated[1].CostCenter)
	})

	t.Run("creates a new primary org when none exist", func(t *testing.T) {
		updated, changed := buildUpdatedOrganizations(nil, userProfilePatch{employeeType: strPtr("Contractor")})

		require.True(t, changed)
		require.Len(t, updated, 1)
		require.True(t, updated[0].Primary, "a brand-new organization entry is created as primary")
		require.Equal(t, "Contractor", updated[0].Description)
	})

	t.Run("employee type maps to Description, force-sent even when empty", func(t *testing.T) {
		orgs := []*directoryAdmin.UserOrganization{{Primary: true, Description: "Full-time"}}
		updated, changed := buildUpdatedOrganizations(orgs, userProfilePatch{employeeType: strPtr("")})

		require.True(t, changed)
		require.Len(t, updated, 1)
		require.Equal(t, "", updated[0].Description)
		require.Contains(t, updated[0].ForceSendFields, "Description")
	})

	t.Run("no organizations and only empty-string clears: does not create a phantom org", func(t *testing.T) {
		updated, changed := buildUpdatedOrganizations(nil, userProfilePatch{
			department:   strPtr(""),
			employeeType: strPtr(""),
		})

		require.False(t, changed, "a no-op clear-only patch must not be reported as a change")
		require.Len(t, updated, 0, "must not fabricate an empty primary organization when there is nothing to persist")
	})

	t.Run("no organizations, one empty clear and one real value: still creates the org", func(t *testing.T) {
		updated, changed := buildUpdatedOrganizations(nil, userProfilePatch{
			department: strPtr(""),
			jobTitle:   strPtr("Engineer"),
		})

		require.True(t, changed)
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
