package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-google-workspace/pkg/connector"
)

var version = "dev"

func main() {
	ctx := context.Background()

	cfg := &config{}
	cmd, err := cli.NewCmd(ctx, "baton-google-workspace", cfg, validateConfig, getConnector)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version

	cmdFlags(cmd)

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	var jsonCredentials []byte

	if cfg.CredentialsJSONFilePath == "" {
		l.Error("no path specified to credentialsJson file")
	} else {
		var err error
		jsonCredentials, err = os.ReadFile(cfg.CredentialsJSONFilePath) // just pass the file name
		if err != nil {
			l.Error("error reading credentialsJson file", zap.String("CredentialsJSONFilePath", cfg.CredentialsJSONFilePath), zap.Error(err))
		}
	}

	config := connector.Config{
		CustomerID:         cfg.CustomerID,
		Domain:             cfg.Domain,
		AdministratorEmail: cfg.AdministratorEmail,
		Credentials:        jsonCredentials,
	}

	cb, err := connector.New(ctx, config)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	connector, err := connectorbuilder.NewConnector(ctx, cb)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return connector, nil
}
