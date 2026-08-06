package connector

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// runtimeOAuthScopes and subsumedByBroaderScope live in connector.go, next to
// newClient, so a future change to its scope wiring is more likely to keep
// this test's expectations in sync in the same edit.

func bareScope(scope string) string {
	return strings.TrimPrefix(scope, "https://www.googleapis.com/auth/")
}

type capabilitiesFile struct {
	ResourceTypeCapabilities []struct {
		ResourceType struct {
			ID string `json:"id"`
		} `json:"resourceType"`
		Permissions struct {
			Permissions []struct {
				Permission string `json:"permission"`
			} `json:"permissions"`
		} `json:"permissions"`
	} `json:"resourceTypeCapabilities"`
}

// TestBatonCapabilitiesDeclareEveryRuntimeScope guards against a scope
// requested at runtime (newClient, connector.go) with no matching entry in
// any resourceType's capabilityPermissions() (resource_types.go).
// baton_capabilities.json is generated from those annotations (run `go
// build -o connector ./cmd/baton-google-workspace && ./connector
// capabilities > baton_capabilities.json` to regenerate after changing
// them) and is what a customer's domain-wide-delegation setup instructions
// are derived from - an under-declared scope means a tenant granted exactly
// the declared set has the dependent service(s) fail silently against a
// nil client, with no error naming the missing scope.
func TestBatonCapabilitiesDeclareEveryRuntimeScope(t *testing.T) {
	raw, err := os.ReadFile("../../baton_capabilities.json")
	require.NoError(t, err, "baton_capabilities.json must exist at the repo root")

	var caps capabilitiesFile
	require.NoError(t, json.Unmarshal(raw, &caps))

	declared := make(map[string]bool)
	for _, rt := range caps.ResourceTypeCapabilities {
		for _, p := range rt.Permissions.Permissions {
			declared[p.Permission] = true
		}
	}
	require.NotEmpty(t, declared, "expected at least one declared permission in baton_capabilities.json")

	for _, scope := range runtimeOAuthScopes {
		bare := bareScope(scope)
		if declared[bare] {
			continue
		}
		if coveringScope, ok := subsumedByBroaderScope[scope]; ok && declared[coveringScope] {
			continue
		}
		t.Errorf("runtime scope %q (bare: %q) is not declared in any resourceType's "+
			"capabilityPermissions() in resource_types.go, and baton_capabilities.json was not "+
			"regenerated to include it - a tenant granted exactly the declared set will have the "+
			"dependent action(s) fail silently against a nil service", scope, bare)
	}
}
