package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google/externalaccount"
)

const (
	jwtSubjectTokenType = "urn:ietf:params:oauth:token-type:jwt"
	cloudPlatformScope  = "https://www.googleapis.com/auth/cloud-platform"
	jwtBearerGrantType  = "urn:ietf:params:oauth:grant-type:jwt-bearer"

	// delegationAssertionLifetime is the lifetime of the domain-wide delegation
	// assertion signed through the IAM Credentials API; Google accepts at most
	// one hour.
	delegationAssertionLifetime = time.Hour

	// maxGoogleResponseBytes bounds the token and signJwt responses read into memory.
	maxGoogleResponseBytes = 1 << 20
)

// Google endpoints of the workload identity chain, replaceable by package tests.
var (
	googleSTSTokenURL        = "https://sts.googleapis.com/v1/token"
	googleIAMCredentialsURL  = "https://iamcredentials.googleapis.com"
	googleOAuth2TokenURL     = "https://oauth2.googleapis.com/token"
	googleIAMAudiencePrefix  = "//iam.googleapis.com/"
	googleIAMProviderURLBase = "https://iam.googleapis.com/"
)

// WorkloadIdentityConfig authenticates without a stored service account key.
// The connector exchanges the JWT ConductorOne issues for it at Google STS
// through the customer's workload identity pool provider, then impersonates the
// service account to sign a domain-wide delegation assertion for the admin.
type WorkloadIdentityConfig struct {
	// SubjectTokenSource returns the ConductorOne JWT as its access token. It is
	// asked again on every STS exchange.
	SubjectTokenSource oauth2.TokenSource
	// Provider is the workload identity pool provider resource name:
	// projects/<project number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>.
	Provider string
	// ServiceAccountEmail is the service account with domain-wide delegation.
	ServiceAccountEmail string
}

// googleWorkloadIdentityProviderPattern matches a provider resource name. Pool
// and provider IDs are 4-32 characters of [a-z0-9-], per the IAM API's
// workloadIdentityPools.create and providers.create references, and the
// default allowed audience embeds the project number, not the project ID.
// ConductorOne validates the field with the same rule before it issues a JWT.
var googleWorkloadIdentityProviderPattern = regexp.MustCompile(
	`^projects/[0-9]+/locations/global/workloadIdentityPools/[a-z0-9-]{4,32}/providers/[a-z0-9-]{4,32}$`,
)

// normalizeWorkloadIdentityProvider returns the provider resource name. It
// trims spaces, one https://iam.googleapis.com/ or //iam.googleapis.com/ prefix
// and one trailing slash, exactly as ConductorOne does when it sets the JWT
// audience, so both sides name the same provider.
func normalizeWorkloadIdentityProvider(raw string) (string, error) {
	provider := strings.TrimSpace(raw)
	for _, prefix := range []string{googleIAMProviderURLBase, googleIAMAudiencePrefix} {
		provider = strings.TrimPrefix(provider, prefix)
	}
	provider = strings.TrimSuffix(provider, "/")
	if provider == "" {
		return "", errors.New("baton-google-workspace: workload-identity-provider is required for workload identity")
	}
	if !googleWorkloadIdentityProviderPattern.MatchString(provider) {
		return "", fmt.Errorf("baton-google-workspace: workload-identity-provider %q must look like "+
			"projects/<project number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>", raw)
	}
	return provider, nil
}

// workloadIdentityError marks a failure in the workload identity chain before
// Google's token endpoint answers: getting the ConductorOne JWT, the STS
// exchange, or signing the delegation assertion. These are configuration
// problems that affect every scope, so they must never be read as a missing
// scope, even when the wrapped error is an *oauth2.RetrieveError.
type workloadIdentityError struct {
	step string
	err  error
}

func (e *workloadIdentityError) Error() string {
	return fmt.Sprintf("baton-google-workspace: workload identity %s failed: %v", e.step, e.err)
}

func (e *workloadIdentityError) Unwrap() error {
	return e.err
}

type workloadIdentity struct {
	// subject is shared by every scope's STS exchange. It wraps the runtime's
	// token source in oauth2.ReuseTokenSource, which serializes calls and reuses
	// the JWT until it expires: the runtime's source has no lock of its own.
	subject        oauth2.TokenSource
	stsAudience    string
	serviceAccount string

	// federated is the cloud-platform token from STS, the same for every scope,
	// so one exchange serves all of them. Built on first use.
	federatedOnce sync.Once
	federated     oauth2.TokenSource
	federatedErr  error
}

func newWorkloadIdentity(c *WorkloadIdentityConfig) (*workloadIdentity, error) {
	if c.SubjectTokenSource == nil {
		return nil, errors.New("baton-google-workspace: workload identity needs the token the ConductorOne-hosted runtime supplies")
	}
	provider, err := normalizeWorkloadIdentityProvider(c.Provider)
	if err != nil {
		return nil, err
	}
	serviceAccount := strings.TrimSpace(c.ServiceAccountEmail)
	if !strings.Contains(serviceAccount, "@") {
		return nil, fmt.Errorf("baton-google-workspace: service-account-email %q must be a service account email", c.ServiceAccountEmail)
	}
	return &workloadIdentity{
		subject:        oauth2.ReuseTokenSource(nil, c.SubjectTokenSource),
		stsAudience:    googleIAMAudiencePrefix + provider,
		serviceAccount: serviceAccount,
	}, nil
}

// tokenSource returns access tokens for admin with the given scopes. ctx must
// carry the HTTP client as oauth2.HTTPClient, which the STS exchange uses.
//
// The returned source outlives ctx: the service it backs is cached for the
// connector's lifetime and refreshes about every hour, long after the task or
// invocation that created it has ended. So the requests it makes keep ctx's
// values but not its cancellation. The service account key path behaves the
// same way, since golang.org/x/oauth2/jwt sends its refresh without a request
// context.
func (w *workloadIdentity) tokenSource(ctx context.Context, client *http.Client, admin string, scopes ...string) (oauth2.TokenSource, error) {
	ctx = context.WithoutCancel(ctx)
	w.federatedOnce.Do(func() {
		w.federated, w.federatedErr = externalaccount.NewTokenSource(ctx, externalaccount.Config{
			Audience:             w.stsAudience,
			SubjectTokenType:     jwtSubjectTokenType,
			TokenURL:             googleSTSTokenURL,
			Scopes:               []string{cloudPlatformScope},
			SubjectTokenSupplier: c1SubjectTokenSupplier{source: w.subject},
		})
	})
	if w.federatedErr != nil {
		return nil, &workloadIdentityError{step: "configuration", err: w.federatedErr}
	}
	return oauth2.ReuseTokenSource(nil, &delegatedTokenSource{
		ctx:            ctx,
		client:         client,
		federated:      w.federated,
		serviceAccount: w.serviceAccount,
		admin:          admin,
		scopes:         scopes,
	}), nil
}

// c1SubjectTokenSupplier hands the ConductorOne JWT to the STS exchange.
type c1SubjectTokenSupplier struct {
	source oauth2.TokenSource
}

func (s c1SubjectTokenSupplier) SubjectToken(context.Context, externalaccount.SupplierOptions) (string, error) {
	token, err := s.source.Token()
	if err != nil {
		return "", &workloadIdentityError{step: "ConductorOne token", err: err}
	}
	if token.AccessToken == "" {
		return "", &workloadIdentityError{step: "ConductorOne token", err: errors.New("empty token")}
	}
	return token.AccessToken, nil
}

// delegatedTokenSource returns domain-wide delegation access tokens for admin.
type delegatedTokenSource struct {
	ctx            context.Context
	client         *http.Client
	federated      oauth2.TokenSource
	serviceAccount string
	admin          string
	scopes         []string
}

// Token signs a delegation assertion with the federated identity and exchanges
// it at Google's token endpoint. That last exchange is the same request the
// service account key flow makes, so its errors are *oauth2.RetrieveError and
// a scope the admin console has not granted classifies the same way.
func (s *delegatedTokenSource) Token() (*oauth2.Token, error) {
	federated, err := s.federated.Token()
	if err != nil {
		var wie *workloadIdentityError
		if errors.As(err, &wie) {
			return nil, err
		}
		return nil, &workloadIdentityError{step: "STS token exchange", err: err}
	}
	assertion, err := s.signDelegationAssertion(federated)
	if err != nil {
		return nil, &workloadIdentityError{step: fmt.Sprintf("signJwt for %s", s.serviceAccount), err: err}
	}
	return exchangeJWTBearer(s.ctx, s.client, assertion)
}

func (s *delegatedTokenSource) signDelegationAssertion(federated *oauth2.Token) (string, error) {
	now := time.Now()
	claims, err := json.Marshal(map[string]any{
		"iss":   s.serviceAccount,
		"sub":   s.admin,
		"scope": strings.Join(s.scopes, " "),
		"aud":   googleOAuth2TokenURL,
		"iat":   now.Unix(),
		"exp":   now.Add(delegationAssertionLifetime).Unix(),
	})
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(map[string]string{"payload": string(claims)})
	if err != nil {
		return "", err
	}

	endpoint := fmt.Sprintf("%s/v1/projects/-/serviceAccounts/%s:signJwt", googleIAMCredentialsURL, url.PathEscape(s.serviceAccount))
	req, err := http.NewRequestWithContext(s.ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	federated.SetAuthHeader(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxGoogleResponseBytes))
	if err != nil {
		return "", fmt.Errorf("read signJwt response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", googleAPIError(resp.StatusCode, respBody)
	}

	var signed struct {
		SignedJWT string `json:"signedJwt"`
	}
	if err := json.Unmarshal(respBody, &signed); err != nil {
		return "", fmt.Errorf("parse signJwt response: %w", err)
	}
	if signed.SignedJWT == "" {
		return "", errors.New("signJwt response has no signedJwt")
	}
	return signed.SignedJWT, nil
}

// googleAPIError names the status and message of a Google API error body, so
// a missing Service Account Token Creator role reads as such in the sync error.
func googleAPIError(statusCode int, body []byte) error {
	var parsed struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil && parsed.Error.Message != "" {
		return fmt.Errorf("status %d %s: %s", statusCode, parsed.Error.Status, parsed.Error.Message)
	}
	return fmt.Errorf("status %d: %s", statusCode, strings.TrimSpace(string(body)))
}

// exchangeJWTBearer exchanges a signed assertion for an access token. A
// non-2xx response is returned as *oauth2.RetrieveError with the response and
// body set, like golang.org/x/oauth2/jwt does, so isScopeUnauthorized applies.
func exchangeJWTBearer(ctx context.Context, client *http.Client, assertion string) (*oauth2.Token, error) {
	form := url.Values{
		"grant_type": {jwtBearerGrantType},
		"assertion":  {assertion},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, googleOAuth2TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("baton-google-workspace: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("baton-google-workspace: token request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxGoogleResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("baton-google-workspace: read token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &oauth2.RetrieveError{Response: resp, Body: body}
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("baton-google-workspace: parse token response: %w", err)
	}
	if parsed.AccessToken == "" {
		return nil, errors.New("baton-google-workspace: token response has no access_token")
	}
	token := &oauth2.Token{AccessToken: parsed.AccessToken, TokenType: parsed.TokenType}
	if parsed.ExpiresIn > 0 {
		token.Expiry = time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second)
	}
	return token, nil
}
