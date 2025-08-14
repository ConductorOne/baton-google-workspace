package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/conductorone/baton-google-workspace/pkg/connector"
)

var version = "dev"

func main() {
	ctx := context.Background()

	_, cmd, err := config.DefineConfiguration(
		ctx,
		"baton-google-workspace",
		getConnector,
		Configuration,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, v *viper.Viper) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	// Get configuration values
	customerID := v.GetString(CustomerIDField.FieldName)
	domain := v.GetString(DomainField.FieldName)
	administratorEmail := v.GetString(AdministratorEmailField.FieldName)
	credentialsJSONFilePath := v.GetString(CredentialsJSONFilePathField.FieldName)
	credentialsJSON := v.GetString(CredentialsJSONField.FieldName)

	var jsonCredentials []byte

	isCapabilitiesCommand := len(os.Args) > 2 && os.Args[1] == "capabilities"

	if !isCapabilitiesCommand {
		if err := ValidateConfig(v); err != nil {
			l.Error("error validating config", zap.Error(err))
			return nil, err
		}
	}

	if credentialsJSONFilePath != "" {
		var err error
		jsonCredentials, err = os.ReadFile(credentialsJSONFilePath)
		if err != nil {
			l.Error("error reading credentialsJson file", zap.String("credentialsJSONFilePath", credentialsJSONFilePath), zap.Error(err))
			return nil, err
		}
	} else if credentialsJSON != "" {
		jsonCredentials = []byte(credentialsJSON)
	}

	config := connector.Config{
		CustomerID:         customerID,
		AdministratorEmail: administratorEmail,
		Domain:             domain,
		Credentials:        jsonCredentials,
	}

	// Create the Google Workspace connector
	googleWorkspaceConnector, err := connector.New(ctx, config)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	// Create the connector builder
	c, err := connectorbuilder.NewConnector(ctx, googleWorkspaceConnector)
	if err != nil {
		l.Error("error creating connector builder", zap.Error(err))
		return nil, err
	}

	return c, nil
}
