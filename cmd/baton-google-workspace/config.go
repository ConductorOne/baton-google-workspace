package main

import (
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

// Defining configuration fields for Google Workspace connector.
var (
	// CustomerIDField defines the customer ID for the Google Workspace account.
	CustomerIDField = field.StringField(
		"customer-id",
		field.WithDescription("The customer ID for the Google Workspace account"),
		field.WithRequired(true),
	)

	// DomainField defines the domain for the Google Workspace account.
	DomainField = field.StringField(
		"domain",
		field.WithDescription("The domain for the Google Workspace account"),
	)

	// AdministratorEmailField defines an administrator email for the Google Workspace account.
	AdministratorEmailField = field.StringField(
		"administrator-email",
		field.WithDescription("An administrator email for the Google Workspace account"),
		field.WithRequired(true),
	)

	// CredentialsJSONFilePathField defines the path to JSON credentials file.
	CredentialsJSONFilePathField = field.StringField(
		"credentials-json-file-path",
		field.WithDescription("JSON credentials file name for the Google Workspace account. Mutually exclusive with credentials JSON"),
	)

	// CredentialsJSONField defines the JSON credentials as a string.
	CredentialsJSONField = field.StringField(
		"credentials-json",
		field.WithDescription("JSON credentials for the Google Workspace account. Mutually exclusive with file path"),
	)

	SyncTokensField = field.BoolField(
		"sync-tokens",
		field.WithDescription("Sync third party tokens for the Google Workspace account."),
	)

	// Collection of all configuration fields.
	ConfigurationFields = []field.SchemaField{
		CustomerIDField,
		DomainField,
		AdministratorEmailField,
		CredentialsJSONFilePathField,
		CredentialsJSONField,
		SyncTokensField,
	}

	// Configuration combines fields into a single configuration object.
	Configuration = field.NewConfiguration(
		ConfigurationFields,
	)
)

// ValidateConfig validates that all required configuration is present and valid.
func ValidateConfig(v *viper.Viper) error {
	if err := field.Validate(Configuration, v); err != nil {
		return err
	}

	// Additional validation to ensure either credentials file or credentials JSON is provided
	if v.GetString(CredentialsJSONFilePathField.FieldName) == "" && v.GetString(CredentialsJSONField.FieldName) == "" {
		return fmt.Errorf("either credentials-json-file-path or credentials-json must be provided")
	}

	return nil
}
