package connector

import (
	"context"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/stretchr/testify/require"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

func TestGoogleLoginEventFeed_MarksTargetAsAppAccess(t *testing.T) {
	const userEmail = "alice@example.com"

	users := []*directoryAdmin.User{{Id: "profile-alice", PrimaryEmail: userEmail}}
	server := newDirectoryUsersOnlyServer(t, users, 10, func(string) *reportsAdmin.Activities {
		return &reportsAdmin.Activities{Items: []*reportsAdmin.Activity{
			activityItem(1, time.Minute, userEmail, "profile-alice"),
		}}
	})
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	feed := newGoogleLoginEventFeed(&gwclient.GoogleWorkspaceClient{UserService: dir, ReportService: rep}, "customer", "")

	events, _, _, err := feed.ListEvents(context.Background(), nil, &pagination.StreamToken{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NoError(t, events[0].ValidateAll())

	ue := events[0].GetUsageEvent()
	require.Equal(t, v2.UsageEvent_TARGET_KIND_APP_ACCESS, ue.TargetKind)
	require.Equal(t, googleWorkspaceAppID, ue.GetTargetResource().GetId().GetResource())
}
