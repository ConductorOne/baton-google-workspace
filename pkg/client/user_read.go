package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	directoryAdmin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// UserService keeps the same scoped HTTP client as the generated service. The
// generated User uses bool fields, which cannot distinguish omitted data from false.
// Only the resource reads use presence-aware decoding; other SDK calls are unchanged.
type UserService struct {
	*directoryAdmin.Service
	HTTPClient *http.Client
}

func NewUserService(ctx context.Context, opts ...option.ClientOption) (*UserService, error) {
	client, _, err := htransport.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	service, err := directoryAdmin.NewService(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &UserService{Service: service, HTTPClient: client}, nil
}

type UserSnapshot struct {
	*directoryAdmin.User
	State map[string]any
}

func (u *UserSnapshot) UnmarshalJSON(data []byte) error {
	type userFields directoryAdmin.User
	user := &directoryAdmin.User{}
	wire := struct {
		*userFields
		Suspended                 *bool     `json:"suspended"`
		Archived                  *bool     `json:"archived"`
		ChangePasswordAtNextLogin *bool     `json:"changePasswordAtNextLogin"`
		IsMailboxSetup            *bool     `json:"isMailboxSetup"`
		Aliases                   *[]string `json:"aliases"`
		NonEditableAliases        *[]string `json:"nonEditableAliases"`
	}{userFields: (*userFields)(user)}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	u.User = user
	u.State = make(map[string]any)
	for _, field := range []struct {
		name   string
		value  *bool
		target *bool
	}{
		{"suspended", wire.Suspended, &user.Suspended},
		{"archived", wire.Archived, &user.Archived},
		{"change_password_at_next_login", wire.ChangePasswordAtNextLogin, &user.ChangePasswordAtNextLogin},
		{"is_mailbox_setup", wire.IsMailboxSetup, &user.IsMailboxSetup},
	} {
		if field.value != nil {
			*field.target = *field.value
			u.State[field.name] = *field.value
		}
	}
	for _, field := range []struct {
		name   string
		value  *[]string
		target *[]string
	}{
		{"aliases", wire.Aliases, &user.Aliases},
		{"non_editable_aliases", wire.NonEditableAliases, &user.NonEditableAliases},
	} {
		if field.value != nil {
			*field.target = *field.value
			values := make([]any, len(*field.value))
			for i, value := range *field.value {
				values[i] = value
			}
			u.State[field.name] = values
		}
	}
	u.State["user_id"] = user.Id
	if user.CustomerId != "" {
		u.State["customer_id"] = user.CustomerId
	}
	if user.PrimaryEmail != "" {
		u.State["primary_email"] = user.PrimaryEmail
	}
	return nil
}

type UserSnapshots struct {
	Users         []*UserSnapshot `json:"users"`
	NextPageToken string          `json:"nextPageToken"`
	googleapi.ServerResponse
}

func (s *UserService) read(ctx context.Context, path string, query url.Values, target any) (*http.Response, error) {
	base, err := url.Parse(s.BasePath)
	if err != nil {
		return nil, err
	}
	endpoint, err := base.Parse(path)
	if err != nil {
		return nil, err
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", googleapi.UserAgent)
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return resp, err
	}
	defer resp.Body.Close()
	if err := googleapi.CheckResponse(resp); err != nil {
		return resp, err
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return resp, fmt.Errorf("google-workspace: failed to decode user response: %w", err)
	}
	return resp, nil
}

func (u *UserSnapshot) observed(at string) {
	if u.State == nil {
		u.State = make(map[string]any)
	}
	u.State["observed_at"] = at
	// State is provider data, not evidence of SSO, licensing, or employee takeover.
	u.State["read_status"] = "observed"
}

func (c *GoogleWorkspaceClient) ListUsers(ctx context.Context, customerID, domain, pageToken string) (*UserSnapshots, error) {
	if c.UserService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	query := url.Values{"orderBy": {"email"}, "projection": {"full"}, "maxResults": {"200"}, "fields": {string(listUsersFields)}}
	if domain != "" {
		query.Set("domain", domain)
	} else {
		query.Set("customer", customerID)
	}
	if pageToken != "" {
		query.Set("pageToken", pageToken)
	}
	result := &UserSnapshots{}
	resp, err := c.UserService.read(ctx, "admin/directory/v1/users", query, result)
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to list users")
	}
	result.ServerResponse = googleapi.ServerResponse{HTTPStatusCode: resp.StatusCode, Header: resp.Header}
	at := time.Now().UTC().Format(time.RFC3339Nano)
	for _, user := range result.Users {
		if user == nil || user.User == nil || user.Id == "" {
			return nil, fmt.Errorf("google-workspace: user listing contains an entry without provider identity")
		}
		user.observed(at)
	}
	return result, nil
}

func (c *GoogleWorkspaceClient) GetUser(ctx context.Context, userID string) (*UserSnapshot, error) {
	if c.UserService == nil {
		return nil, errServiceNotAvailable("user service")
	}
	result := &UserSnapshot{}
	_, err := c.UserService.read(ctx, "admin/directory/v1/users/"+url.PathEscape(userID), url.Values{"projection": {"full"}}, result)
	if err != nil {
		return nil, wrapGoogleApiErrorWithContext(err, "failed to get user")
	}
	if result.User == nil || result.Id == "" {
		return nil, fmt.Errorf("google-workspace: user read returned no provider identity")
	}
	result.observed(time.Now().UTC().Format(time.RFC3339Nano))
	return result, nil
}
