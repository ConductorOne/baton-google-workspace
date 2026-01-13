package main

import (
	cfg "github.com/conductorone/baton-google-workspace/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/config"
)

func main() {
	config.Generate("google-workspace", cfg.Configuration)
}
