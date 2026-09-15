package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCreateAccountSuspendedWithSuppliedPassword(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		requests = append(requests, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"created-id","primaryEmail":"prehire@example.com","name":{"fullName":"Pre Hire"},"suspended":true,"orgUnitPath":"/Prehires"}`))
	}))
	defer server.Close()
	profile, err := structpb.NewStruct(map[string]any{
		"email": "prehire@example.com", "given_name": "Pre", "family_name": "Hire",
		"suspended": true, "org_unit_path": "/Prehires", "changePasswordAtNextLogin": true,
	})
	require.NoError(t, err)
	opts := &v2.LocalCredentialOptions{}
	opts.SetPlaintextPassword(&v2.LocalCredentialOptions_PlaintextPassword{PlaintextPassword: "fixture-only-protected-password"})
	result, secrets, _, err := newTestUserResourceType(t, server).CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, opts)
	require.NoError(t, err)
	require.Len(t, requests, 1, "account must be born suspended, not disabled by a second request")
	require.Equal(t, true, requests[0]["suspended"])
	require.Equal(t, "/Prehires", requests[0]["orgUnitPath"])
	require.Equal(t, true, requests[0]["changePasswordAtNextLogin"])
	require.Equal(t, "fixture-only-protected-password", requests[0]["password"])
	require.Empty(t, secrets, "supplied credential mode must not return the password")
	success, ok := result.(*v2.CreateAccountResponse_SuccessResult)
	require.True(t, ok)
	require.Equal(t, "created-id", success.Resource.GetId().GetResource())
}

func TestCreateAccountRejectsMalformedInitialState(t *testing.T) {
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writes++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	profile, err := structpb.NewStruct(map[string]any{
		"email": "prehire@example.com", "given_name": "Pre", "family_name": "Hire", "suspended": "true",
	})
	require.NoError(t, err)
	opts := &v2.LocalCredentialOptions{}
	opts.SetRandomPassword(&v2.LocalCredentialOptions_RandomPassword{Length: 32})
	_, _, _, err = newTestUserResourceType(t, server).CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, opts)
	require.Error(t, err)
	require.Zero(t, writes, "invalid suspension must not silently create an active account")
}

func TestRotatePasswordPreservesModeAndNextLogin(t *testing.T) {
	for _, generated := range []bool{false, true} {
		t.Run(map[bool]string{false: "supplied", true: "generated"}[generated], func(t *testing.T) {
			var body map[string]any
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, http.MethodPatch, r.Method)
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"stable-user-id"}`))
			}))
			defer server.Close()
			opts := &v2.LocalCredentialOptions{ForceChangeAtNextLogin: generated}
			if generated {
				opts.SetRandomPassword(&v2.LocalCredentialOptions_RandomPassword{Length: 32})
			} else {
				opts.SetPlaintextPassword(&v2.LocalCredentialOptions_PlaintextPassword{PlaintextPassword: "fixture-only-rotated-password"})
			}
			secrets, _, err := newTestUserResourceType(t, server).Rotate(context.Background(), &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "stable-user-id"}, opts)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, generated, body["changePasswordAtNextLogin"], "false must be serialized to clear an earlier requirement")
			require.Len(t, body, 2, "rotation must not overwrite profile or lifecycle state")
			if generated {
				require.Len(t, secrets, 1)
				require.Len(t, secrets[0].Bytes, 32)
				require.Equal(t, string(secrets[0].Bytes), body["password"])
			} else {
				require.Empty(t, secrets)
				require.Equal(t, "fixture-only-rotated-password", body["password"])
			}
		})
	}
}

func TestRotatePasswordDoesNotReplayUnknownMutation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	opts := &v2.LocalCredentialOptions{}
	opts.SetRandomPassword(&v2.LocalCredentialOptions_RandomPassword{Length: 32})
	secrets, _, err := newTestUserResourceType(t, server).Rotate(context.Background(), &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "stable-user-id"}, opts)
	require.Error(t, err)
	require.Empty(t, secrets)
	require.Equal(t, 1, calls)
}
