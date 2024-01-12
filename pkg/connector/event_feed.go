package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

func (c *GoogleWorkspace) ListEvents(ctx context.Context, startingPosition *v2.StartingPosition, pToken *pagination.Token) ([]*v2.Event, string, annotations.Annotations, error) {
	s, err := c.getRoleService()
	r, err := s.Activities.List("all", "token").Do()
	if err != nil {
		return nil, "", nil, err
	}

	events := []*v2.Event{}
	for _, activity := range r.Items {
		event := &v2.Event{
			Id:         activity.Id.UniqueQualifier,
			OccurredAt: activity.Id.Time,
			// Event: &v2.Event_UsageEvent{
			// 	UsageEvent: &v2.UsageEvent{
			// 		TargetResource: &v2.Resource{
			// 			Id: &v2.ResourceId{

			// 			}
			// 			DisplayName:
			// Annotations: annotations.Annotations{
			// 	&v2.ExternalLink{
			// 		Url: activity.Id,
			// 	},
			// },
		}
		events = append(events, event)
	}
}
