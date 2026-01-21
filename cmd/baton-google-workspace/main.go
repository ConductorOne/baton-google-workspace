package main

import (
	"context"

	cfg "github.com/conductorone/baton-google-workspace/pkg/config"
	"github.com/conductorone/baton-google-workspace/pkg/connector"
	"github.com/conductorone/baton-sdk/pkg/config"
)

var version = "dev"

func main() {
	ctx := context.Background()
	config.RunConnector(
		ctx,
		"baton-google-workspace",
		version,
		cfg.Configuration,
		connector.New,
	)
}
