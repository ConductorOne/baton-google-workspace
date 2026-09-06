package connector

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAliasRemovalRejectsAnotherOwner(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01"},
		"u2": {Id: "u2", PrimaryEmail: "other@example.com", CustomerId: "C01", Aliases: []string{"old@example.com"}},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	result, _, err := newAliasTestResourceType(t, server).removeUserAliasActionHandler(t.Context(), aliasArgs("u1", "old@example.com", "target@example.com", "C01"))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
	require.Equal(t, "owner_mismatch", result.GetFields()["outcome"].GetStringValue())
	require.Zero(t, state.deleteHits)
}

func TestAliasRemovalRejectsNonEditableAlias(t *testing.T) {
	state := &aliasTestState{users: map[string]*aliasTestUser{
		"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01", NonEditableAliases: []string{"domain-alias@example.com"}},
	}}
	server := newAliasTestServer(state)
	defer server.Close()
	_, _, err := newAliasTestResourceType(t, server).removeUserAliasActionHandler(t.Context(), aliasArgs("u1", "domain-alias@example.com", "target@example.com", "C01"))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, state.deleteHits)
}

func TestAliasReadbackMustMatchAccountAndBeReadable(t *testing.T) {
	for _, test := range []struct {
		name  string
		after *aliasTestUser
		deny  int
	}{
		{"different ID", &aliasTestUser{Id: "another-user", PrimaryEmail: "target@example.com", CustomerId: "C01"}, 0},
		{"different customer", &aliasTestUser{Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C02"}, 0},
		{"denied readback", nil, http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &aliasTestState{users: map[string]*aliasTestUser{
				"u1": {Id: "u1", PrimaryEmail: "target@example.com", CustomerId: "C01", Aliases: []string{"old@example.com"}},
			}, afterDeleteUser: test.after, readbackFailStatus: test.deny}
			server := newAliasTestServer(state)
			defer server.Close()
			result, _, err := newAliasTestResourceType(t, server).removeUserAliasActionHandler(t.Context(), aliasArgs("u1", "old@example.com", "target@example.com", "C01"))
			require.Error(t, err)
			require.False(t, result.GetFields()[fieldSuccess].GetBoolValue())
			require.False(t, result.GetFields()["observation_complete"].GetBoolValue())
			require.NotContains(t, result.GetFields(), "alias_present_after", "failed reads do not prove absence")
			require.Equal(t, 1, state.deleteHits)
		})
	}
}
