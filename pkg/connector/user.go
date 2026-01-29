package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	admin "google.golang.org/api/admin/directory/v1"

	mapset "github.com/deckarep/golang-set/v2"
)

type userResourceType struct {
	resourceType            *v2.ResourceType
	userService             *admin.Service
	userProvisioningService *admin.Service
	userSecurityService     *admin.Service
	customerId              string
	domain                  string
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

func (o *userResourceType) List(ctx context.Context, _ *v2.ResourceId, attrs rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)
	bag := &pagination.Bag{}
	err := bag.Unmarshal(attrs.PageToken.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal pagination token in user List: %w", err)
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
		return nil, nil, wrapGoogleApiErrorWithContext(err, "failed to list users")
	}

	rv := make([]*v2.Resource, 0, len(users.Users))
	for _, user := range users.Users {
		if user.Id == "" {
			l.Error("user had no id", zap.String("email", user.PrimaryEmail))
			continue
		}

		userResource, err := o.userResource(ctx, user)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build user resource in List: %w", err)
		}
		rv = append(rv, userResource)
	}

	nextPage, err := bag.NextToken(users.NextPageToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate next page token in user List: %w", err)
	}

	return rv, &rs.SyncOpResults{NextPageToken: nextPage}, nil
}

func (o *userResourceType) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (o *userResourceType) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func userBuilder(userService *admin.Service, customerId string, domain string, userProvisioningService *admin.Service, userSecurityService *admin.Service) *userResourceType {
	return &userResourceType{
		resourceType:            resourceTypeUser,
		userService:             userService,
		userProvisioningService: userProvisioningService,
		userSecurityService:     userSecurityService,
		customerId:              customerId,
		domain:                  domain,
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

	profile["org_unit_path"] = user.OrgUnitPath

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

func (o *userResourceType) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	r := o.userService.Users.Get(resourceId.Resource).Projection("full")

	user, err := r.Context(ctx).Do()
	if err != nil {
		return nil, nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to retrieve user: %s", resourceId.Resource))
	}

	if o.domain != "" {
		orgs, err := extractFromInterface[*admin.UserOrganization](user.Organizations)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to extract user organizations: %w", err)
		}

		found := false
		for _, org := range orgs {
			if org.Domain == o.domain {
				found = true
				break
			}
		}
		if !found {
			l.Info("user not in domain", zap.String("email", user.PrimaryEmail), zap.String("domain", o.domain))
			return nil, nil, nil
		}
	} else if o.customerId != "" {
		if user.CustomerId != o.customerId {
			l.Info("user not in customer account", zap.String("email", user.PrimaryEmail), zap.String("customer_id", user.CustomerId))
			return nil, nil, nil
		}
	}

	if user.Id == "" {
		l.Error("user had no id", zap.String("email", user.PrimaryEmail))
		return nil, nil, nil
	}

	userResource, err := o.userResource(ctx, user)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build user resource in Get: %w", err)
	}

	return userResource, nil, nil
}

func (o *userResourceType) userResource(ctx context.Context, user *admin.User) (*v2.Resource, error) {
	profile := userProfile(ctx, user)
	additionalLogins := mapset.NewSet[string]()
	employeeIDs := mapset.NewSet[string]()
	traitOpts := []rs.UserTraitOption{
		rs.WithEmail(user.PrimaryEmail, true),
		rs.WithDetailedStatus(o.userStatus(ctx, user)),
	}

	if user.ThumbnailPhotoUrl != "" {
		traitOpts = append(traitOpts, rs.WithUserIcon(&v2.AssetRef{
			Id: user.ThumbnailPhotoUrl,
		}))
	}
	if user.Archived || user.Suspended {
		traitOpts = append(traitOpts, rs.WithStatus(v2.UserTrait_Status_STATUS_DISABLED))
	}
	if user.IsEnrolledIn2Sv {
		traitOpts = append(traitOpts, rs.WithMFAStatus(
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
			return nil, fmt.Errorf("failed to extract posix accounts: %w", err)
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
			return nil, fmt.Errorf("failed to extract external IDs: %w", err)
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
		traitOpts = append(traitOpts, rs.WithStatus(v2.UserTrait_Status_STATUS_DELETED))
	}
	if user.CreationTime != "" {
		if t, err := time.Parse(time.RFC3339, user.CreationTime); err == nil {
			traitOpts = append(traitOpts, rs.WithCreatedAt(t))
		}
	}
	if user.LastLoginTime != "" {
		if t, err := time.Parse(time.RFC3339, user.LastLoginTime); err == nil {
			traitOpts = append(traitOpts, rs.WithLastLogin(t))
		}
	}

	if employeeIDs.Cardinality() > 0 {
		traitOpts = append(traitOpts,
			rs.WithEmployeeID(employeeIDs.ToSlice()...),
		)
	}

	traitOpts = append(traitOpts,
		rs.WithUserProfile(profile),
		rs.WithUserLogin(user.PrimaryEmail, additionalLogins.ToSlice()...),
	)

	userResource, err := rs.NewUserResource(
		user.Name.FullName,
		resourceTypeUser,
		user.Id,
		traitOpts,
		rs.WithAnnotation(
			&v2.V1Identifier{
				Id: user.Id,
			},
		),
	)
	return userResource, err
}

func (o *userResourceType) CreateAccountCapabilityDetails(
	_ context.Context,
) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
	}, nil, nil
}

// https://developers.google.com/workspace/admin/directory/reference/rest/v1/users .
func (o *userResourceType) CreateAccount(ctx context.Context, accountInfo *v2.AccountInfo, credentialOptions *v2.LocalCredentialOptions) (
	connectorbuilder.CreateAccountResponse,
	[]*v2.PlaintextData,
	annotations.Annotations,
	error,
) {
	pMap := accountInfo.Profile.AsMap()
	email, ok := pMap["email"].(string)
	if !ok || email == "" {
		return nil, nil, nil, fmt.Errorf("email not found in profile")
	}

	givenName, ok := pMap["given_name"].(string)
	if !ok || givenName == "" {
		return nil, nil, nil, fmt.Errorf("given_name not found in profile")
	}

	familyName, ok := pMap["family_name"].(string)
	if !ok || familyName == "" {
		return nil, nil, nil, fmt.Errorf("family_name not found in profile")
	}

	changePasswordAtNextLogin, ok := pMap["changePasswordAtNextLogin"].(bool)
	if !ok {
		changePasswordAtNextLogin = false
	}

	user := &admin.User{
		PrimaryEmail: email,
		Name: &admin.UserName{
			GivenName:  givenName,
			FamilyName: familyName,
		},
		ChangePasswordAtNextLogin: changePasswordAtNextLogin,
	}

	if credentialOptions == nil {
		return nil, nil, nil, fmt.Errorf("credentialOptions cannot be nil")
	}

	if o.userProvisioningService == nil {
		return nil, nil, nil, fmt.Errorf("user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	var password string
	var plaintextData []*v2.PlaintextData
	var err error

	if credentialOptions.GetRandomPassword() != nil || credentialOptions.GetPlaintextPassword() != nil {
		password, err = crypto.GeneratePassword(ctx, credentialOptions)
	} else {
		password, err = crypto.GenerateRandomPassword(&v2.LocalCredentialOptions_RandomPassword{
			Length: 16,
		})
	}

	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate password: %w", err)
	}

	plaintextData = append(plaintextData, &v2.PlaintextData{
		Name:        "password",
		Description: "Generated password for the new account",
		Bytes:       []byte(password),
	})

	user.Password = password

	user, err = o.userProvisioningService.Users.Insert(user).Context(ctx).Do()
	if err != nil {
		return nil, nil, nil, wrapGoogleApiErrorWithContext(err, "failed to create account")
	}

	userResource, err := o.userResource(ctx, user)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create user resource: %w", err)
	}

	return &v2.CreateAccountResponse_SuccessResult{
		Resource:              userResource,
		IsCreateAccountResult: true,
	}, plaintextData, nil, nil
}

func (o *userResourceType) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	if o.userProvisioningService == nil {
		return nil, fmt.Errorf("user provisioning service not available - requires %s scope", admin.AdminDirectoryUserScope)
	}

	err := o.userProvisioningService.Users.Delete(resourceId.Resource).Context(ctx).Do()
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, fmt.Sprintf("failed to delete user: %s", resourceId.Resource))
	}

	return nil, nil
}
