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

// TestBatonCapabilitiesDeclareEveryRuntimeScope guards against a runtime
// scope with no matching entry in baton_capabilities.json (regenerate via
// `./connector capabilities > baton_capabilities.json` after changing
// resource_types.go's capabilityPermissions()).
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
