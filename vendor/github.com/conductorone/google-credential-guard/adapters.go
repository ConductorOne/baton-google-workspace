package googlecredentialguard

import (
	"errors"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/option"
)

// ClientOption supplies only service-account credentials to Google API clients.
func (s ServiceAccount) ClientOption() (option.ClientOption, error) {
	if len(s.canonical) == 0 {
		return nil, errors.New("google credential: uninitialized service account")
	}
	return option.WithAuthCredentialsJSON(option.ServiceAccount, s.canonical), nil
}

// JWTConfig creates a service-account-only OAuth configuration.
func (s ServiceAccount) JWTConfig(scopes ...string) (*jwt.Config, error) {
	if len(s.canonical) == 0 {
		return nil, errors.New("google credential: uninitialized service account")
	}
	return google.JWTConfigFromJSON(s.canonical, scopes...)
}

// AuthCredentials creates credentials for cloud.google.com/go clients.
func (s ServiceAccount) AuthCredentials(scopes ...string) (*auth.Credentials, error) {
	if len(s.canonical) == 0 {
		return nil, errors.New("google credential: uninitialized service account")
	}
	return credentials.NewCredentialsFromJSON(credentials.ServiceAccount, s.canonical, &credentials.DetectOptions{Scopes: append([]string(nil), scopes...)})
}
