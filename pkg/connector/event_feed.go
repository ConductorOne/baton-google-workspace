package connector

import (
	"context"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func RFC3339ToTimestamp(s string) *timestamppb.Timestamp {
	i, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(i)
}

func UnixSecondStringToTimestamp(s string) *timestamppb.Timestamp {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return timestamppb.New(time.Unix(i, 0))
}

func convertIdTimeToTimestamp(s string) *timestamppb.Timestamp {
	if time := RFC3339ToTimestamp(s); time != nil {
		return time
	}
	if time := UnixSecondStringToTimestamp(s); time != nil {
		return time
	}
	return nil
}

func getValueFromParameters(name string, parameters []*reportsAdmin.ActivityEventsParameters) string {
	for _, p := range parameters {
		p := p
		if p.Name == name {
			return p.Value
		}
	}
	return ""
}

func (c *GoogleWorkspace) ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	var streamState *pagination.StreamState
	s, err := c.getReportService(ctx)
	if err != nil {
		return nil, streamState, nil, err
	}

	req := s.Activities.List("all", "token")
	req.MaxResults(int64(pToken.Size))
	if pToken.Token != "" {
		req.PageToken(pToken.Token)
	}
	if earliestEvent != nil {
		req.StartTime(earliestEvent.AsTime().Format(time.RFC3339))
	}

	r, err := req.Do()
	if err != nil {
		return nil, streamState, nil, err
	}

	events := []*v2.Event{}
	for _, activity := range r.Items {
		// There can be multiple events, have not found an example of this yet
		for _, e := range activity.Events {
			userTrait, err := resource.NewUserTrait(resource.WithEmail(activity.Actor.Email, true))
			if err != nil {
				return nil, streamState, nil, err
			}
			event := &v2.Event{
				Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
				OccurredAt: convertIdTimeToTimestamp(activity.Id.Time),
				Event: &v2.Event_UsageEvent{
					UsageEvent: &v2.UsageEvent{
						TargetResource: &v2.Resource{
							Id: &v2.ResourceId{
								ResourceType: enterpriseApplicationResourceType.Id,
								Resource:     getValueFromParameters("client_id", e.Parameters),
							},
							DisplayName: getValueFromParameters("app_name", e.Parameters),
						},
						ActorResource: &v2.Resource{
							Id: &v2.ResourceId{
								ResourceType: resourceTypeUser.Id,
								Resource:     activity.Actor.ProfileId,
							},
							DisplayName: activity.Actor.Email,
							Annotations: annotations.New(userTrait),
						},
					},
				},
				Annotations: nil,
			}
			events = append(events, event)
		}
	}
	streamState = &pagination.StreamState{
		NextPageToken: r.NextPageToken,
		// todo @anthony/logan punting on this for now We actually want to return false if we find an event we have already seen
		HasMore: r.NextPageToken != "",
	}
	return events, streamState, nil, nil
}
