package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cfg "github.com/conductorone/baton-google-workspace/pkg/config"
)

const (
	testWIProvider       = "projects/123456789012/locations/global/workloadIdentityPools/c1-pool/providers/c1-oidc"
	testWIServiceAccount = "gws-sync@test-project.iam.gserviceaccount.com"
	testWIAdmin          = "admin@example.com"
	testC1JWT            = "c1.issued.jwt"
)

// fakeGoogle serves the STS, IAM Credentials and OAuth token endpoints of the
// workload identity chain and records what each one received.
type fakeGoogle struct {
	mu sync.Mutex

	stsStatus, signStatus, tokenStatus int
	stsBody, signBody, tokenBody       string

	stsForms    []url.Values
	signPaths   []string
	signAuth    []string
	signClaims  []map[string]any
	tokenForms  []url.Values
	tokenScopes []string
}

func newFakeGoogle() *fakeGoogle {
	return &fakeGoogle{
		stsStatus:   http.StatusOK,
		stsBody:     `{"access_token":"federated-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}`,
		signStatus:  http.StatusOK,
		signBody:    `{"keyId":"k1","signedJwt":"signed.delegation.assertion"}`,
		tokenStatus: http.StatusOK,
		tokenBody:   `{"access_token":"delegated-token","token_type":"Bearer","expires_in":3600}`,
	}
}

func (f *fakeGoogle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.URL.Path == "/sts":
		form, _ := url.ParseQuery(string(body))
		f.stsForms = append(f.stsForms, form)
		w.WriteHeader(f.stsStatus)
		_, _ = w.Write([]byte(f.stsBody))
	case strings.HasPrefix(r.URL.Path, "/iam/"):
		f.signPaths = append(f.signPaths, r.URL.Path)
		f.signAuth = append(f.signAuth, r.Header.Get("Authorization"))
		var req struct {
			Payload string `json:"payload"`
		}
		_ = json.Unmarshal(body, &req)
		claims := map[string]any{}
		_ = json.Unmarshal([]byte(req.Payload), &claims)
		f.signClaims = append(f.signClaims, claims)
		w.WriteHeader(f.signStatus)
		_, _ = w.Write([]byte(f.signBody))
	case r.URL.Path == "/token":
		form, _ := url.ParseQuery(string(body))
		f.tokenForms = append(f.tokenForms, form)
		if n := len(f.signClaims); n > 0 {
			scope, _ := f.signClaims[n-1]["scope"].(string)
			f.tokenScopes = append(f.tokenScopes, scope)
		}
		w.WriteHeader(f.tokenStatus)
		_, _ = w.Write([]byte(f.tokenBody))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// useFakeGoogle points the workload identity endpoints and the connector's HTTP
// client at fake.
func useFakeGoogle(t *testing.T, fake *fakeGoogle) {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	prevSTS, prevIAM, prevToken, prevClient := googleSTSTokenURL, googleIAMCredentialsURL, googleOAuth2TokenURL, newGoogleHTTPClient
	googleSTSTokenURL = server.URL + "/sts"
	googleIAMCredentialsURL = server.URL + "/iam"
	googleOAuth2TokenURL = server.URL + "/token"
	newGoogleHTTPClient = func(context.Context, ...uhttp.Option) (*http.Client, error) {
		return server.Client(), nil
	}
	t.Cleanup(func() {
		googleSTSTokenURL, googleIAMCredentialsURL, googleOAuth2TokenURL, newGoogleHTTPClient = prevSTS, prevIAM, prevToken, prevClient
	})
}

type countingTokenSource struct {
	mu     sync.Mutex
	calls  int
	token  string
	expiry time.Time
	err    error
}

func (s *countingTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &oauth2.Token{AccessToken: s.token, Expiry: s.expiry}, nil
}

func newTestWorkloadIdentity(t *testing.T, subject oauth2.TokenSource) *workloadIdentity {
	t.Helper()
	wi, err := newWorkloadIdentity(&WorkloadIdentityConfig{
		SubjectTokenSource:  subject,
		Provider:            testWIProvider,
		ServiceAccountEmail: testWIServiceAccount,
	})
	require.NoError(t, err)
	return wi
}

func TestWorkloadIdentityChain(t *testing.T) {
	fake := newFakeGoogle()
	useFakeGoogle(t, fake)
	subject := &countingTokenSource{token: testC1JWT}
	wi := newTestWorkloadIdentity(t, subject)

	_, err := newGWSAdminServiceForWorkloadIdentity(context.Background(), wi, testWIAdmin,
		directoryAdmin.NewService, directoryAdmin.AdminDirectoryDomainReadonlyScope)
	require.NoError(t, err)

	require.Len(t, fake.stsForms, 1)
	sts := fake.stsForms[0]
	require.Equal(t, "urn:ietf:params:oauth:grant-type:token-exchange", sts.Get("grant_type"))
	require.Equal(t, "//iam.googleapis.com/"+testWIProvider, sts.Get("audience"))
	require.Equal(t, testC1JWT, sts.Get("subject_token"))
	require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", sts.Get("subject_token_type"))
	require.Equal(t, cloudPlatformScope, sts.Get("scope"))

	require.Equal(t, []string{"/iam/v1/projects/-/serviceAccounts/" + testWIServiceAccount + ":signJwt"}, fake.signPaths)
	require.Equal(t, []string{"Bearer federated-token"}, fake.signAuth)
	claims := fake.signClaims[0]
	require.Equal(t, testWIServiceAccount, claims["iss"])
	require.Equal(t, testWIAdmin, claims["sub"], "the admin is the domain-wide delegation subject")
	require.Equal(t, directoryAdmin.AdminDirectoryDomainReadonlyScope, claims["scope"])
	require.Equal(t, googleOAuth2TokenURL, claims["aud"])
	iat, _ := claims["iat"].(float64)
	exp, _ := claims["exp"].(float64)
	require.InDelta(t, delegationAssertionLifetime.Seconds(), exp-iat, 1)
	require.LessOrEqual(t, iat, float64(time.Now().Add(-5*time.Second).Unix()), "iat is backdated like the key path's jws")

	require.Len(t, fake.tokenForms, 1)
	require.Equal(t, jwtBearerGrantType, fake.tokenForms[0].Get("grant_type"))
	require.Equal(t, "signed.delegation.assertion", fake.tokenForms[0].Get("assertion"))
	require.Equal(t, 1, subject.calls)
}

// A scope the admin console has not granted must classify exactly as it does
// with a service account key: skipped as unauthorized, and upgraded from
// readonly to full when the full scope is granted.
func TestWorkloadIdentityUngrantedScopeIsAuthorizationError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"401 unauthorized_client", http.StatusUnauthorized, `{"error":"unauthorized_client","error_description":"Client is unauthorized to retrieve access tokens using this method, or client not authorized for any of the scopes requested."}`},
		{"403 access_denied", http.StatusForbidden, `{"error":"access_denied","error_description":"Requested client not authorized."}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeGoogle()
			fake.tokenStatus, fake.tokenBody = tc.status, tc.body
			useFakeGoogle(t, fake)

			_, err := newGWSAdminServiceForWorkloadIdentity(context.Background(), newTestWorkloadIdentity(t, &countingTokenSource{token: testC1JWT}),
				testWIAdmin, directoryAdmin.NewService, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope)
			require.Error(t, err)
			require.True(t, isAuthorizationError(err), "got %T: %v", err, err)
		})
	}
}

func TestWorkloadIdentityScopeUpgrade(t *testing.T) {
	fake := newFakeGoogle()
	useFakeGoogle(t, fake)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		if r.URL.Path == "/token" {
			fake.mu.Lock()
			scope, _ := fake.signClaims[len(fake.signClaims)-1]["scope"].(string)
			if strings.HasSuffix(scope, ".readonly") {
				fake.tokenStatus, fake.tokenBody = http.StatusUnauthorized, `{"error":"unauthorized_client"}`
			} else {
				fake.tokenStatus, fake.tokenBody = http.StatusOK, `{"access_token":"delegated-token","token_type":"Bearer","expires_in":3600}`
			}
			fake.mu.Unlock()
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	googleSTSTokenURL, googleIAMCredentialsURL, googleOAuth2TokenURL = server.URL+"/sts", server.URL+"/iam", server.URL+"/token"
	newGoogleHTTPClient = func(context.Context, ...uhttp.Option) (*http.Client, error) { return server.Client(), nil }

	c := &GoogleWorkspace{
		administratorEmail: testWIAdmin,
		workloadIdentity:   newTestWorkloadIdentity(t, &countingTokenSource{token: testC1JWT}),
		serviceCache:       map[string]any{},
	}
	svc, err := c.getDirectoryService(context.Background(), directoryAdmin.AdminDirectoryGroupReadonlyScope)
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.Equal(t, []string{directoryAdmin.AdminDirectoryGroupReadonlyScope, directoryAdmin.AdminDirectoryGroupScope}, fake.tokenScopes)
	require.Contains(t, c.serviceCache, directoryAdmin.AdminDirectoryGroupScope)
}

// Failures before Google's token endpoint answers affect every scope. They must
// abort the sync with a clear error, never read as a missing scope that is
// skipped: a skipped scope would leave a sync that succeeds empty.
func TestWorkloadIdentityChainFailuresAreNotAuthorizationErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		setup    func(*fakeGoogle)
		subject  *countingTokenSource
		wantStep string
		wantText string
		wantCode codes.Code
	}{
		{
			name: "signJwt without Service Account Token Creator",
			setup: func(f *fakeGoogle) {
				f.signStatus = http.StatusForbidden
				f.signBody = `{"error":{"code":403,"message":"Permission 'iam.serviceAccounts.signJwt' denied on resource (or it may not exist).","status":"PERMISSION_DENIED"}}`
			},
			subject:  &countingTokenSource{token: testC1JWT},
			wantStep: "signJwt for " + testWIServiceAccount,
			wantText: "iam.serviceAccounts.signJwt",
			wantCode: codes.PermissionDenied,
		},
		{
			name: "signJwt rate limited",
			setup: func(f *fakeGoogle) {
				f.signStatus = http.StatusTooManyRequests
				f.signBody = `{"error":{"code":429,"message":"Quota exceeded.","status":"RESOURCE_EXHAUSTED"}}`
			},
			subject:  &countingTokenSource{token: testC1JWT},
			wantStep: "signJwt for " + testWIServiceAccount,
			wantText: "Quota exceeded",
			wantCode: codes.Unavailable,
		},
		{
			name: "STS rate limited",
			setup: func(f *fakeGoogle) {
				f.stsStatus = http.StatusTooManyRequests
				f.stsBody = `{"error":"rate_limited"}`
			},
			subject:  &countingTokenSource{token: testC1JWT},
			wantStep: "STS token exchange",
			wantText: "429",
			wantCode: codes.Unavailable,
		},
		{
			name: "STS unavailable",
			setup: func(f *fakeGoogle) {
				f.stsStatus = http.StatusServiceUnavailable
				f.stsBody = `{"error":"temporarily_unavailable"}`
			},
			subject:  &countingTokenSource{token: testC1JWT},
			wantStep: "STS token exchange",
			wantText: "503",
			wantCode: codes.Unavailable,
		},
		{
			name: "STS rejects the JWT",
			setup: func(f *fakeGoogle) {
				f.stsStatus = http.StatusBadRequest
				f.stsBody = `{"error":"invalid_grant","error_description":"The audience in ID Token does not match the expected audience."}`
			},
			subject:  &countingTokenSource{token: testC1JWT},
			wantStep: "STS token exchange",
			wantText: "audience",
			wantCode: codes.InvalidArgument,
		},
		{
			name:  "ConductorOne token unavailable with a 401",
			setup: func(*fakeGoogle) {},
			subject: &countingTokenSource{err: &oauth2.RetrieveError{
				Response: &http.Response{StatusCode: http.StatusUnauthorized},
				Body:     []byte(`{"error":"unauthorized_client"}`),
			}},
			wantStep: "ConductorOne token",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeGoogle()
			tc.setup(fake)
			useFakeGoogle(t, fake)

			_, err := newGWSAdminServiceForWorkloadIdentity(context.Background(), newTestWorkloadIdentity(t, tc.subject),
				testWIAdmin, directoryAdmin.NewService, directoryAdmin.AdminDirectoryRolemanagementReadonlyScope)
			require.Error(t, err)
			require.False(t, isAuthorizationError(err), "must not be skipped as a missing scope: %v", err)
			var wie *workloadIdentityError
			require.True(t, errors.As(err, &wie), "got %T: %v", err, err)
			require.Equal(t, tc.wantStep, wie.step)
			require.Contains(t, err.Error(), tc.wantText)
			if tc.wantCode != codes.OK {
				require.Equal(t, tc.wantCode, status.Code(err), "same code uhttp gives this HTTP status: %v", err)
			}
			require.Empty(t, fake.tokenForms, "nothing reaches Google's token endpoint")
		})
	}
}

func TestNormalizeWorkloadIdentityProvider(t *testing.T) {
	for _, raw := range []string{
		testWIProvider,
		"//iam.googleapis.com/" + testWIProvider,
		"https://iam.googleapis.com/" + testWIProvider,
		"https://iam.googleapis.com/" + testWIProvider + "/",
		"  " + testWIProvider + "\n",
	} {
		got, err := normalizeWorkloadIdentityProvider(raw)
		require.NoError(t, err, raw)
		require.Equal(t, testWIProvider, got, raw)
	}
	for _, raw := range []string{
		"",
		"projects/my-project/locations/global/workloadIdentityPools/c1-pool/providers/c1-oidc",
		"projects/123456789012/locations/global/workloadIdentityPools/c1-pool",
		"projects/123456789012/locations/global/workloadIdentityPools/C1_Pool/providers/c1-oidc",
		"projects/123456789012/locations/global/workloadIdentityPools/c1-pool/providers/c1.oidc",
		"projects/123456789012/locations/global/workloadIdentityPools/c1p/providers/c1-oidc",
		"projects/123456789012/locations/global/workloadIdentityPools/c1-pool/providers/" + strings.Repeat("a", 33),
		testWIProvider + "?x=1",
	} {
		_, err := normalizeWorkloadIdentityProvider(raw)
		require.Error(t, err, raw)
	}
}

func TestNewSelectsCredentialByAuthMethod(t *testing.T) {
	wiConfig := &cfg.GoogleWorkspace{
		CustomerId:               "C000000",
		AdministratorEmail:       testWIAdmin,
		WorkloadIdentityProvider: testWIProvider,
		ServiceAccountEmail:      testWIServiceAccount,
	}

	t.Run("workload identity with the hosted token source", func(t *testing.T) {
		builder, _, err := New(context.Background(), wiConfig, &cli.ConnectorOpts{
			SelectedAuthMethod: cfg.WorkloadIdentityC1AuthMethod,
			TokenSource:        &countingTokenSource{token: testC1JWT},
		})
		require.NoError(t, err)
		gw, ok := builder.(*GoogleWorkspace)
		require.True(t, ok)
		require.NotNil(t, gw.workloadIdentity)
		require.Nil(t, gw.credentials)
	})

	t.Run("workload identity outside the hosted runtime", func(t *testing.T) {
		_, _, err := New(context.Background(), wiConfig, &cli.ConnectorOpts{SelectedAuthMethod: cfg.WorkloadIdentityC1AuthMethod})
		require.ErrorContains(t, err, "ConductorOne-hosted runtime")
	})

	t.Run("no auth method stored means the service account key", func(t *testing.T) {
		keyConfig := &cfg.GoogleWorkspace{AdministratorEmail: testWIAdmin, CredentialsJson: string(testCredentials(t))}
		for _, opts := range []*cli.ConnectorOpts{nil, {}, {SelectedAuthMethod: cfg.ServiceAccountKeyAuthMethod}} {
			builder, _, err := New(context.Background(), keyConfig, opts)
			require.NoError(t, err)
			gw, ok := builder.(*GoogleWorkspace)
			require.True(t, ok)
			require.Nil(t, gw.workloadIdentity)
			require.NotEmpty(t, gw.credentials)
		}
	})

	t.Run("a stray workload identity token does not switch a key connector", func(t *testing.T) {
		keyConfig := &cfg.GoogleWorkspace{AdministratorEmail: testWIAdmin, CredentialsJson: string(testCredentials(t))}
		builder, _, err := New(context.Background(), keyConfig, &cli.ConnectorOpts{TokenSource: &countingTokenSource{token: testC1JWT}})
		require.NoError(t, err)
		require.Nil(t, builder.(*GoogleWorkspace).workloadIdentity)
	})
}

// ConductorOne matches these names (ductone/c1 pkg/builtin_connectors/google_workspace_v2.go),
// and the SDK falls back to the default group, then the first, for connectors
// configured before the groups existed.
func TestConfigurationFieldGroups(t *testing.T) {
	groups := cfg.Configuration.FieldGroups
	require.Len(t, groups, 2)
	require.Equal(t, cfg.ServiceAccountKeyAuthMethod, groups[0].Name)
	require.True(t, groups[0].Default)
	require.False(t, groups[1].Default)
	require.Equal(t, "workload-identity-c1", groups[1].Name)
	require.Equal(t, "workload-identity-provider", cfg.WorkloadIdentityProviderField.FieldName)
	require.Equal(t, "google-workspace-c1-token", cfg.C1TokenField.FieldName)

	for _, group := range groups {
		_, hasToken := group.FieldMap()[cfg.C1TokenField.FieldName]
		require.Equal(t, group.Name == cfg.WorkloadIdentityC1AuthMethod, hasToken,
			"only the workload identity group may carry the OAuth2 field, so the runtime asks ConductorOne for a token only there")
		for _, common := range []string{"customer-id", "domain", "administrator-email"} {
			require.Contains(t, group.FieldMap(), common, group.Name)
		}
	}
}

// The services are cached for the connector's lifetime and refresh long after
// the task that created them ends, so a refresh must not use that task's
// cancellation.
func TestWorkloadIdentityRefreshAfterCallerContextEnds(t *testing.T) {
	fake := newFakeGoogle()
	// Expiring within oauth2's refresh margin makes every Token() call refresh.
	fake.stsBody = `{"access_token":"federated-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":1}`
	fake.tokenBody = `{"access_token":"delegated-token","token_type":"Bearer","expires_in":1}`
	useFakeGoogle(t, fake)
	wi := newTestWorkloadIdentity(t, &countingTokenSource{token: testC1JWT})

	ctx, cancel := context.WithCancel(context.Background())
	client, err := newGoogleHTTPClient(ctx)
	require.NoError(t, err)
	ts, err := wi.tokenSource(context.WithValue(ctx, oauth2.HTTPClient, client), client, testWIAdmin, directoryAdmin.AdminDirectoryUserReadonlyScope)
	require.NoError(t, err)
	_, err = ts.Token()
	require.NoError(t, err)

	cancel()
	token, err := ts.Token()
	require.NoError(t, err, "refresh after the creating context ended")
	require.Equal(t, "delegated-token", token.AccessToken)
	require.Len(t, fake.stsForms, 2)
	require.Len(t, fake.tokenForms, 2)
}

// One STS exchange and one ConductorOne JWT serve every scope; only the
// delegation assertion is per scope.
func TestWorkloadIdentitySharesFederatedTokenAcrossScopes(t *testing.T) {
	fake := newFakeGoogle()
	useFakeGoogle(t, fake)
	subject := &countingTokenSource{token: testC1JWT}
	c := &GoogleWorkspace{
		administratorEmail: testWIAdmin,
		workloadIdentity:   newTestWorkloadIdentity(t, subject),
		serviceCache:       map[string]any{},
	}
	for _, scope := range []string{
		directoryAdmin.AdminDirectoryUserReadonlyScope,
		directoryAdmin.AdminDirectoryGroupReadonlyScope,
		directoryAdmin.AdminDirectoryDomainReadonlyScope,
	} {
		_, err := c.getDirectoryService(context.Background(), scope)
		require.NoError(t, err)
	}
	require.Len(t, fake.stsForms, 1)
	require.Equal(t, 1, subject.calls)
	require.Len(t, fake.signClaims, 3)
	require.Len(t, fake.tokenForms, 3)
}

// The vendored STS exchange flattens a transport error with %v, dropping
// uhttp's code; it must still come back Unavailable so the syncer retries it.
func TestWorkloadIdentitySTSTransportFailureIsUnavailable(t *testing.T) {
	fake := newFakeGoogle()
	useFakeGoogle(t, fake)
	googleSTSTokenURL = "http://127.0.0.1:1/sts" // nothing listens on port 1

	_, err := newGWSAdminServiceForWorkloadIdentity(context.Background(), newTestWorkloadIdentity(t, &countingTokenSource{token: testC1JWT}),
		testWIAdmin, directoryAdmin.NewService, directoryAdmin.AdminDirectoryUserReadonlyScope)
	require.Error(t, err)
	require.False(t, isAuthorizationError(err))
	var wie *workloadIdentityError
	require.True(t, errors.As(err, &wie), "got %T: %v", err, err)
	require.Equal(t, "STS token exchange", wie.step)
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
}

// The runtime's token source has no lock and C1 mints on every call to it, so
// one ConductorOne JWT must serve every STS refresh until it expires, even
// with scopes refreshing at the same time.
func TestWorkloadIdentityReusesConductorOneToken(t *testing.T) {
	fake := newFakeGoogle()
	// Federated and delegated tokens expire within oauth2's refresh margin, so
	// every Token() call goes back to STS.
	fake.stsBody = `{"access_token":"federated-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":1}`
	fake.tokenBody = `{"access_token":"delegated-token","token_type":"Bearer","expires_in":1}`
	useFakeGoogle(t, fake)
	subject := &countingTokenSource{token: testC1JWT, expiry: time.Now().Add(10 * time.Minute)}
	wi := newTestWorkloadIdentity(t, subject)

	client, err := newGoogleHTTPClient(context.Background())
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	var wg sync.WaitGroup
	for _, scope := range []string{
		directoryAdmin.AdminDirectoryUserReadonlyScope,
		directoryAdmin.AdminDirectoryGroupReadonlyScope,
		directoryAdmin.AdminDirectoryDomainReadonlyScope,
	} {
		ts, err := wi.tokenSource(ctx, client, testWIAdmin, scope)
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				_, err := ts.Token()
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	require.Greater(t, len(fake.stsForms), 1, "STS refreshed more than once")
	require.Equal(t, 1, subject.calls, "one ConductorOne JWT serves every refresh until it expires")
}
