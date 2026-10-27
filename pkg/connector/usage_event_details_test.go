package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/proto"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

func TestNetworkOriginFromActivity(t *testing.T) {
	tests := []struct {
		name     string
		activity *reportsAdmin.Activity
		want     *v2.NetworkOrigin
	}{
		{name: "no network data", activity: &reportsAdmin.Activity{}, want: nil},
		{
			name: "all fields",
			activity: &reportsAdmin.Activity{
				IpAddress:   "203.0.113.7",
				NetworkInfo: &reportsAdmin.ActivityNetworkInfo{IpAsn: []int64{16509}, RegionCode: "US", SubdivisionCode: "US-OR"},
			},
			want: &v2.NetworkOrigin{IpAddress: "203.0.113.7", CountryCode: "US", SubdivisionCode: "US-OR", Asns: []uint32{16509}},
		},
		{
			name:     "ip only",
			activity: &reportsAdmin.Activity{IpAddress: "2001:db8::1"},
			want:     &v2.NetworkOrigin{IpAddress: "2001:db8::1"},
		},
		{
			name: "invalid values dropped",
			activity: &reportsAdmin.Activity{
				IpAddress:   "203.0.113.7:443",
				NetworkInfo: &reportsAdmin.ActivityNetworkInfo{IpAsn: []int64{0, -1, 1 << 33, 13335}, RegionCode: "USA", SubdivisionCode: "OR"},
			},
			want: &v2.NetworkOrigin{Asns: []uint32{13335}},
		},
		{
			name:     "lowercase codes upper-cased",
			activity: &reportsAdmin.Activity{NetworkInfo: &reportsAdmin.ActivityNetworkInfo{RegionCode: "us", SubdivisionCode: "us-or"}},
			want:     &v2.NetworkOrigin{CountryCode: "US", SubdivisionCode: "US-OR"},
		},
		{
			name: "only invalid values",
			activity: &reportsAdmin.Activity{
				IpAddress:   "unknown",
				NetworkInfo: &reportsAdmin.ActivityNetworkInfo{RegionCode: "USA"},
			},
			want: nil,
		},
		{
			name:     "asns capped",
			activity: &reportsAdmin.Activity{NetworkInfo: &reportsAdmin.ActivityNetworkInfo{IpAsn: make64(maxNetworkASNs + 10)}},
			want:     &v2.NetworkOrigin{Asns: make32(maxNetworkASNs)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := networkOriginFromActivity(tt.activity)
			require.True(t, proto.Equal(tt.want, got), "want %v, got %v", tt.want, got)
			require.NoError(t, got.ValidateAll())
		})
	}
}

func TestOAuthUsageFromEvent(t *testing.T) {
	scope := func(values ...string) *reportsAdmin.ActivityEventsParameters {
		return &reportsAdmin.ActivityEventsParameters{Name: "scope", MultiValue: values}
	}
	tests := []struct {
		name  string
		event *reportsAdmin.ActivityEvents
		want  *v2.OAuthUsage
	}{
		{
			name: "authorize",
			event: &reportsAdmin.ActivityEvents{Name: "authorize", Parameters: []*reportsAdmin.ActivityEventsParameters{
				{Name: "client_id", Value: "client-a"},
				{Name: "client_type", Value: "WEB"},
				scope("https://www.googleapis.com/auth/userinfo.email", "openid"),
			}},
			want: &v2.OAuthUsage{
				Action:          v2.OAuthUsage_ACTION_AUTHORIZE,
				SourceEventName: "authorize",
				Scopes:          []string{"https://www.googleapis.com/auth/userinfo.email", "openid"},
				ClientType:      "WEB",
			},
		},
		{
			name:  "revoke",
			event: &reportsAdmin.ActivityEvents{Name: "revoke"},
			want:  &v2.OAuthUsage{Action: v2.OAuthUsage_ACTION_REVOKE, SourceEventName: "revoke"},
		},
		{
			name:  "unmapped event name",
			event: &reportsAdmin.ActivityEvents{Name: "activity"},
			want:  &v2.OAuthUsage{Action: v2.OAuthUsage_ACTION_UNSPECIFIED, SourceEventName: "activity"},
		},
		{
			name: "single-valued scope",
			event: &reportsAdmin.ActivityEvents{Name: "authorize", Parameters: []*reportsAdmin.ActivityEventsParameters{
				{Name: "scope", Value: "openid"},
			}},
			want: &v2.OAuthUsage{Action: v2.OAuthUsage_ACTION_AUTHORIZE, SourceEventName: "authorize", Scopes: []string{"openid"}},
		},
		{
			name: "invalid values dropped",
			event: &reportsAdmin.ActivityEvents{Name: strings.Repeat("a", maxOAuthStringBytes+1), Parameters: []*reportsAdmin.ActivityEventsParameters{
				{Name: "client_type", Value: strings.Repeat("a", maxOAuthStringBytes+1)},
				scope("", strings.Repeat("a", maxOAuthScopeBytes+1), "openid"),
			}},
			want: &v2.OAuthUsage{Scopes: []string{"openid"}},
		},
		{
			name: "values at the byte limits kept",
			event: &reportsAdmin.ActivityEvents{Name: strings.Repeat("a", maxOAuthStringBytes), Parameters: []*reportsAdmin.ActivityEventsParameters{
				{Name: "client_type", Value: strings.Repeat("b", maxOAuthStringBytes)},
				scope(strings.Repeat("c", maxOAuthScopeBytes)),
			}},
			want: &v2.OAuthUsage{
				SourceEventName: strings.Repeat("a", maxOAuthStringBytes),
				Scopes:          []string{strings.Repeat("c", maxOAuthScopeBytes)},
				ClientType:      strings.Repeat("b", maxOAuthStringBytes),
			},
		},
		{
			name: "scopes capped",
			event: &reportsAdmin.ActivityEvents{Name: "authorize", Parameters: []*reportsAdmin.ActivityEventsParameters{
				scope(slices.Repeat([]string{"openid"}, maxOAuthScopes+10)...),
			}},
			want: &v2.OAuthUsage{Action: v2.OAuthUsage_ACTION_AUTHORIZE, SourceEventName: "authorize", Scopes: slices.Repeat([]string{"openid"}, maxOAuthScopes)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := oauthUsageFromEvent(tt.event)
			require.True(t, proto.Equal(tt.want, got), "want %v, got %v", tt.want, got)
			require.NoError(t, got.ValidateAll())
		})
	}
}

// TestUsageEventFeed_EmitsNetworkAndOAuthDetails covers the feed end to end, including an
// upstream value that would fail Event validation if it were copied through.
func TestUsageEventFeed_EmitsNetworkAndOAuthDetails(t *testing.T) {
	const userEmail = "alice@example.com"

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{})
			return
		}
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Users{
			Users: []*directoryAdmin.User{{Id: "profile-alice", PrimaryEmail: userEmail}},
		})
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{
			Items: []*directoryAdmin.Token{{ClientId: "client-a", DisplayText: "App A"}},
		})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		activity := activityItem(1, time.Minute, userEmail, "profile-alice",
			&reportsAdmin.ActivityEventsParameters{Name: "client_id", Value: "client-a"},
			&reportsAdmin.ActivityEventsParameters{Name: "app_name", Value: "App A"},
			&reportsAdmin.ActivityEventsParameters{Name: "client_type", Value: "WEB"},
			&reportsAdmin.ActivityEventsParameters{Name: "scope", MultiValue: []string{"openid", ""}},
		)
		activity.Events[0].Name = "authorize"
		activity.IpAddress = "203.0.113.7"
		activity.NetworkInfo = &reportsAdmin.ActivityNetworkInfo{IpAsn: []int64{16509}, RegionCode: "US", SubdivisionCode: "OR"}
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{Items: []*reportsAdmin.Activity{activity}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newUsageEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}, "customer", "")

	events, _, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NoError(t, events[0].ValidateAll())

	ue := events[0].GetUsageEvent()
	require.True(t, proto.Equal(&v2.NetworkOrigin{IpAddress: "203.0.113.7", CountryCode: "US", Asns: []uint32{16509}}, ue.GetNetwork()), "network: %v", ue.GetNetwork())
	require.True(t, proto.Equal(&v2.OAuthUsage{
		Action:          v2.OAuthUsage_ACTION_AUTHORIZE,
		SourceEventName: "authorize",
		Scopes:          []string{"openid"},
		ClientType:      "WEB",
	}, ue.GetOauth()), "oauth: %v", ue.GetOauth())
}

func make64(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = int64(i + 1)
	}
	return s
}

func make32(n int) []uint32 {
	s := make([]uint32, n)
	for i := range s {
		s[i] = uint32(i + 1)
	}
	return s
}
