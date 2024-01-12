package connector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	reportsAdmin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func unixStringToTimestamp(s string) *timestamppb.Timestamp {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return timestamppb.New(time.Unix(i, 0))
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

func (c *GoogleWorkspace) ListEvents(ctx context.Context, startingPosition *v2.StartingPosition, pToken *pagination.Token) ([]*v2.Event, string, annotations.Annotations, error) {
	s, err := c.getRoleService(ctx)
	if err != nil {
		return nil, "", nil, err
	}

	r, err := s.Activities.List("all", "token").Do()
	if err != nil {
		return nil, "", nil, err
	}

	events := []*v2.Event{}
	for _, activity := range r.Items {
		// There can be multiple events, have not found an example of this yet
		for _, e := range activity.Events {
			event := &v2.Event{
				Id:         strconv.FormatInt(activity.Id.UniqueQualifier, 10),
				OccurredAt: unixStringToTimestamp(activity.Id.Time),
				Event: &v2.Event_UsageEvent{
					UsageEvent: &v2.UsageEvent{
						TargetResource: &v2.Resource{
							Id: &v2.ResourceId{
								// ResourceType: .Id,
								Resource: getValueFromParameters("client_id", e.Parameters),
							},
							DisplayName: getValueFromParameters("app_name", e.Parameters),
						},
						ActorResource: &v2.Resource{
							Id: &v2.ResourceId{
								// ResourceType: userResourceType.Id,
								Resource: activity.Actor.ProfileId,
							},
							DisplayName: activity.Actor.Email,
						},
					},
				},
				Annotations: nil,
			}
			events = append(events, event)
		}
	}
	for _, event := range events {
		fmt.Printf("%+v\n", event)
	}
	return nil, "", nil, nil
}
