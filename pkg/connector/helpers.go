package connector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"golang.org/x/oauth2"
)

const (
	MembershipEntitlementIDTemplate = "membership:%s"
	GrantIDTemplate                 = "grant:%s:%s"
)

func v1AnnotationsForResourceType(resourceTypeID string) annotations.Annotations {
	annos := annotations.New(
		&v2.V1Identifier{
			Id: resourceTypeID,
		},
	)
	if resourceTypeID == "user" {
		annos.Update(&v2.SkipEntitlementsAndGrants{})
	}
	return annos
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

func V1GrantID(entitlementID string, userID string) string {
	return fmt.Sprintf(GrantIDTemplate, entitlementID, userID)
}

func V1MembershipEntitlementID(resourceID string) string {
	return fmt.Sprintf(MembershipEntitlementIDTemplate, resourceID)
}
