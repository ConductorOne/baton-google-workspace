package connector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

func capabilityPermissions(perms ...string) *v2.CapabilityPermissions {
	cp := &v2.CapabilityPermissions{}
	for _, p := range perms {
		cp.Permissions = append(cp.Permissions, &v2.CapabilityPermission{Permission: p})
	}
	return cp
}

func v1AnnotationsWithPermissions(resourceTypeID string, perms *v2.CapabilityPermissions) annotations.Annotations {
	annos := v1AnnotationsForResourceType(resourceTypeID)
	annos.Update(perms)
	return annos
}

var (
	resourceTypeRole = &v2.ResourceType{
		Id:          "role",
		DisplayName: "role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
		Annotations: v1AnnotationsWithPermissions("role", capabilityPermissions(
			"admin.directory.rolemanagement",
			"admin.directory.domain.readonly",
		)),
	}
	resourceTypeGroup = &v2.ResourceType{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: v1AnnotationsWithPermissions("group", capabilityPermissions(
			"admin.directory.group",
			"admin.directory.group.member",
			"admin.directory.domain.readonly",
			// Requested at runtime by getGroupsSettingsService (connector.go) for
			// the modify_group_settings action, which reads then patches
			// settings (client.go GroupsSettingsService.Groups.Get/Patch) -
			// previously requested but never declared here, so a tenant
			// granted exactly the declared set had the action fail silently
			// against a nil service. create_group does not use this service
			// (it only calls InsertGroup) and needs no additional scope here.
			"apps.groups.settings",
		)),
	}
	resourceTypeUser = &v2.ResourceType{
		Id:          "user",
		DisplayName: displayUser,
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_USER,
		},
		Annotations: v1AnnotationsWithPermissions("user", capabilityPermissions(
			// Write scope: the user resource supports provisioning (create/delete)
			// and write actions (update_user_profile, update_user, make_admin), so
			// the declared capability must request admin.directory.user, not the
			// read-only variant. The write scope subsumes read.
			//
			// admin.directory.user.alias.readonly was removed from this list:
			// no code path in this connector reads user Aliases (confirmed via
			// grep for .Aliases across pkg/), so it was declared but never
			// actually used - dropped per least-privilege.
			"admin.directory.user",
			"admin.directory.domain.readonly",
			// Requested at runtime by getDataTransferService (connector.go) for
			// transfer_user_drive_files/transfer_user_calendar (client.go
			// DataTransferService.Transfers.List/Insert) - previously requested
			// but never declared here, so a tenant granted exactly the declared
			// set had both actions fail silently against a nil service.
			"admin.datatransfer",
		)),
	}
	resourceTypeEnterpriseApplication = &v2.ResourceType{
		Id:          "enterprise_application",
		DisplayName: "Enterprise Application",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
		Annotations: annotations.New(
			capabilityPermissions(
				"admin.directory.user.readonly",
				"admin.directory.user.security",
				"admin.reports.audit.readonly",
				"cloud-identity.inboundsso.readonly",
			),
		),
	}
)
