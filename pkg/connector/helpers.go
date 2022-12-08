package connector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"golang.org/x/oauth2"
)

const MembershipEntitlementIDTemplate = "%s:%s:member"

// The format of grant IDs follows: 'grant:principal-type:principal-id:entitlement'.
const GrantIDTemplate = "grant:%s:%s:%s"

func v1AnnotationsForResourceType(resourceTypeID string) annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.V1Identifier{
		Id: resourceTypeID,
	})
	return annos
}

func MembershipEntitlementID(resource *v2.ResourceId) string {
	return fmt.Sprintf(MembershipEntitlementIDTemplate, resource.ResourceType, resource.Resource)
}

func GrantID(entitlement *v2.Entitlement, principalId *v2.ResourceId) string {
	return fmt.Sprintf(GrantIDTemplate, principalId.ResourceType, principalId.Resource, entitlement.Id)
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
