package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/spf13/cobra"
)

// config defines the external configuration required for the connector to run.
type config struct {
	cli.BaseConfig `mapstructure:",squash"` // Puts the base config options in the same place as the connector options

	CustomerID              string `mapstructure:"customer-id"`
	Domain                  string `mapstructure:"domain"`
	AdministratorEmail      string `mapstructure:"administrator-email"`
	CredentialsJsonFilePath string `mapstructure:"credentials-json-file-path"`
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, cfg *config) error {
	if cfg.CustomerID == "" {
		return fmt.Errorf("customer id is missing")
	}
	if cfg.Domain == "" {
		return fmt.Errorf("domain is missing")
	}
	if cfg.AdministratorEmail == "" {
		return fmt.Errorf("administrator email is missing")
	}
	if cfg.CredentialsJsonFilePath == "" {
		return fmt.Errorf("credentials json file path is missing")
	}
	return nil
}

// cmdFlags sets the cmdFlags required for the connector.
func cmdFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("customer-id", "", "The customer Id for the google workspace account. ($BATON_CUSTOMER_ID)")
	cmd.PersistentFlags().String("domain", "", "The domain for the google workspace account. ($BATON_DOMAIN)")
	cmd.PersistentFlags().String("administrator-email", "", "An administrator email for the google workspace account. ($BATON_ADMINISTRATOR_EMAIL)")
	cmd.PersistentFlags().String("credentials-json-file-path", "", "Json credentials file name for the google workspace account. ($BATON_CREDENTIALS_JSON_FILE_PATH)")
}
