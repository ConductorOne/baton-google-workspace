package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type usageEventFeed struct {
	connector *GoogleWorkspace
}

func (e *usageEventFeed) ListEvents(ctx context.Context, startAt *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return e.connector.ListEvents(ctx, startAt, pToken)
}

func (e *usageEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "usage_event_feed",
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_USAGE,
		},
		StartAt: v2.EventFeedStartAt_EVENT_FEED_START_AT_TAIL,
	}
}

func newUsageEventFeed(connector *GoogleWorkspace) *usageEventFeed {
	return &usageEventFeed{
		connector: connector,
	}
}
