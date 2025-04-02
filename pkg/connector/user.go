package connector

import (
	"context"
	"encoding/json"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"

	mapset "github.com/deckarep/golang-set/v2"
)

type userResourceType struct {
	resourceType *v2.ResourceType
	userService  *admin.Service
	customerId   string
	domain       string
}

func (o *userResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return o.resourceType
}

func (o *userResourceType) userStatus(ctx context.Context, user *admin.User) (v2.UserTrait_Status_Status, string) {
	if user.DeletionTime != "" {
		return v2.UserTrait_Status_STATUS_DELETED, ""
	}

	if user.Suspended {
		reason := "Suspended"
		if user.SuspensionReason != "" {
			reason += ": " + user.SuspensionReason
		}
		return v2.UserTrait_Status_STATUS_DISABLED, reason
	}

	if user.Archived {
		return v2.UserTrait_Status_STATUS_DISABLED, "Archived"
	}

	return v2.UserTrait_Status_STATUS_ENABLED, ""
}

func (o *userResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)
	bag := &pagination.Bag{}
	err := bag.Unmarshal(pt.Token)
	if err != nil {
		return nil, "", nil, err
	}

	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: resourceTypeUser.Id,
		})
	}

	r := o.userService.Users.List().OrderBy("email").Projection("full")

	if o.domain != "" {
		r = r.Domain(o.domain)
	} else {
		r = r.Customer(o.customerId)
	}

	// https://developers.google.com/admin-sdk/directory/v1/limits
	// Users – A default of 100 entries and a maximum of 500 entries per page.
	r = r.MaxResults(500)

	if bag.PageToken() != "" {
		r = r.PageToken(bag.PageToken())
	}

	users, err := r.Context(ctx).Do()
	if err != nil {
		return nil, "", nil, err
	}

	rv := make([]*v2.Resource, 0, len(users.Users))
	for _, user := range users.Users {
		if user.Id == "" {
			l.Error("google-workspace: user had no id", zap.String("email", user.PrimaryEmail))
			continue
		}
		annos := &v2.V1Identifier{
			Id: user.Id,
		}
		profile := userProfile(ctx, user)
		additionalLogins := mapset.NewSet[string]()
		employeeIDs := mapset.NewSet[string]()
		traitOpts := []sdkResource.UserTraitOption{
			sdkResource.WithEmail(user.PrimaryEmail, true),
			sdkResource.WithDetailedStatus(o.userStatus(ctx, user)),
		}

		if user.ThumbnailPhotoUrl != "" {
			traitOpts = append(traitOpts, sdkResource.WithUserIcon(&v2.AssetRef{
				Id: user.ThumbnailPhotoUrl,
			}))
		}
		if user.Archived || user.Suspended {
			traitOpts = append(traitOpts, sdkResource.WithStatus(v2.UserTrait_Status_STATUS_DISABLED))
		}
		if user.IsEnrolledIn2Sv {
			traitOpts = append(traitOpts, sdkResource.WithMFAStatus(
				&v2.UserTrait_MFAStatus{MfaEnabled: true},
			))
		}

		if len(user.CustomSchemas) > 0 {
			customSchemas := flattenCustomSchemas(ctx, user.CustomSchemas)
			for k, v := range customSchemas {
				profile[k] = v
			}
		}

		if user.PosixAccounts != nil {
			posixAccounts, err := extractFromInterface[*admin.UserPosixAccount](user.PosixAccounts)
			if err != nil {
				return nil, "", nil, err
			}
			for _, posixAccount := range posixAccounts {
				if posixAccount.Username != "" {
					additionalLogins.Add(posixAccount.Username)
				}
			}
		}

		if user.ExternalIds != nil {
			externalIDs, err := extractFromInterface[*admin.UserExternalId](user.ExternalIds)
			if err != nil {
				return nil, "", nil, err
			}
			/*
				Acceptable values: account, custom, customer, login_id, network, organization.
			*/
			for _, id := range externalIDs {
				switch id.Type {
				case "organization":
					// oddly named, this is the employee ID in the google console.
					if id.Value != "" {
						employeeIDs.Add(id.Value)
					}
				case "account":
					if id.Value != "" {
						additionalLogins.Add(id.Value)
					}
				case "login_id":
					if id.Value != "" {
						additionalLogins.Add(id.Value)
					}
				case "network":
					if id.Value != "" {
						additionalLogins.Add(id.Value)
					}
				}
			}
		}
		if user.DeletionTime != "" {
			traitOpts = append(traitOpts, sdkResource.WithStatus(v2.UserTrait_Status_STATUS_DELETED))
		}
		if user.CreationTime != "" {
			if t, err := time.Parse("2006-01-02T15:04:05-0700", user.CreationTime); err == nil {
				traitOpts = append(traitOpts, sdkResource.WithCreatedAt(t))
			}
		}
		if user.LastLoginTime != "" {
			if t, err := time.Parse("2006-01-02T15:04:05-0700", user.LastLoginTime); err == nil {
				traitOpts = append(traitOpts, sdkResource.WithLastLogin(t))
			}
		}

		if employeeIDs.Cardinality() > 0 {
			traitOpts = append(traitOpts,
				sdkResource.WithEmployeeID(employeeIDs.ToSlice()...),
			)
		}

		traitOpts = append(traitOpts,
			sdkResource.WithUserProfile(profile),
			sdkResource.WithUserLogin(user.PrimaryEmail, additionalLogins.ToSlice()...),
		)

		userResource, err := sdkResource.NewUserResource(
			user.Name.FullName,
			resourceTypeUser,
			user.Id,
			traitOpts,
			sdkResource.WithAnnotation(annos),
		)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, userResource)
	}

	nextPage, err := bag.NextToken(users.NextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func (o *userResourceType) Entitlements(_ context.Context, _ *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (o *userResourceType) Grants(_ context.Context, _ *v2.Resource, _ *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func userBuilder(userService *admin.Service, customerId string, domain string) *userResourceType {
	return &userResourceType{
		resourceType: resourceTypeUser,
		userService:  userService,
		customerId:   customerId,
		domain:       domain,
	}
}

func userProfile(ctx context.Context, user *admin.User) map[string]interface{} {
	profile := make(map[string]interface{})
	if user.Name != nil {
		profile["given_name"] = user.Name.GivenName
		profile["family_name"] = user.Name.FamilyName
		profile["full_name"] = user.Name.FullName
		profile["icon"] = user.ThumbnailPhotoUrl
		profile["manager_email"] = extractManagerEmail(user)
	}

	primaryOrg := extractPrimaryOrganizations(user)
	if primaryOrg != nil {
		// add all org[0] fields to the profile
		profile["organization"] = primaryOrg.Name
		profile["department"] = primaryOrg.Department
		profile["title"] = primaryOrg.Title
		profile["location"] = primaryOrg.Location
		profile["cost_center"] = primaryOrg.CostCenter
	}

	return profile
}

func extractManagerEmail(u *admin.User) string {
	for _, rel := range extractRelations(u) {
		if rel.Type == "manager" {
			return rel.Value
		}
	}
	return ""
}

func extractRelations(u *admin.User) []*admin.UserRelation {
	if u.Relations == nil {
		return nil
	}

	data, err := json.Marshal(u.Relations)
	if err != nil {
		return nil
	}
	rv := make([]*admin.UserRelation, 0)
	err = json.Unmarshal(data, &rv)
	if err != nil {
		return nil
	}
	return rv
}

func extractOrganizations(u *admin.User) []*admin.UserOrganization {
	if u.Organizations == nil {
		return nil
	}

	data, err := json.Marshal(u.Organizations)
	if err != nil {
		return nil
	}
	rv := make([]*admin.UserOrganization, 0)
	err = json.Unmarshal(data, &rv)
	if err != nil {
		return nil
	}
	return rv
}

func extractPrimaryOrganizations(u *admin.User) *admin.UserOrganization {
	orgs := extractOrganizations(u)
	if len(orgs) == 0 {
		return nil
	}
	for _, org := range orgs {
		if org.Primary {
			return org
		}
	}
	return orgs[0]
}

// extractFromInterface extracts a typed slice from an interface{} value using JSON marshal/unmarshal.
func extractFromInterface[T any](data interface{}) ([]T, error) {
	if data == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var result []T
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
