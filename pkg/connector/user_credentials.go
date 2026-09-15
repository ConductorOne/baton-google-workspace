package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ connectorbuilder.CredentialManagerLimited = (*userResourceType)(nil)

func (o *userResourceType) RotateCapabilityDetails(_ context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error) {
	return &v2.CredentialDetailsCredentialRotation{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_ENCRYPTED_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
	}, nil, nil
}

// Rotate receives only SDK-decrypted credentials. The SDK encrypts generated
// material for the caller's configured destination; supplied material is not returned.
func (o *userResourceType) Rotate(ctx context.Context, resourceID *v2.ResourceId, options *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error) {
	if resourceID.GetResourceType() != resourceTypeUser.Id || resourceID.GetResource() == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "google-workspace: a user resource ID is required")
	}
	if err := o.client.RequireUserProvisioning(); err != nil {
		return nil, nil, err
	}
	password, err := crypto.GeneratePassword(ctx, options)
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to prepare password: %w", err)
	}
	if password == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "google-workspace: password must not be empty")
	}
	// One sparse mutation, without replay on ambiguous provider failure.
	_, err = o.client.PatchUser(ctx, resourceID.GetResource(), &admin.User{
		Password:                  password,
		ChangePasswordAtNextLogin: options.GetForceChangeAtNextLogin(),
		ForceSendFields:           []string{"ChangePasswordAtNextLogin"},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("google-workspace: failed to rotate password: %w", err)
	}
	if options.GetRandomPassword() == nil {
		return nil, nil, nil
	}
	return []*v2.PlaintextData{{
		Name:        "password",
		Description: "Generated password for the account",
		Bytes:       []byte(password),
	}}, nil, nil
}
