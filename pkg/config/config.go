//go:generate go run ./gen
package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

// Defining configuration fields for Google Workspace connector.
var (
	// CustomerIDField defines the customer ID for the Google Workspace account.
	CustomerIDField = field.StringField(
		"customer-id",
		field.WithDisplayName("Customer ID"),
		field.WithDescription("The customer ID for the Google Workspace account"),
		field.WithRequired(true),
	)

	// DomainField defines the domain for the Google Workspace account.
	DomainField = field.StringField(
		"domain",
		field.WithDisplayName("Domain"),
		field.WithDescription("The domain for the Google Workspace account"),
	)

	// AdministratorEmailField defines an administrator email for the Google Workspace account.
	AdministratorEmailField = field.StringField(
		"administrator-email",
		field.WithDisplayName("Administrator Email"),
		field.WithDescription("An administrator email for the Google Workspace account"),
		field.WithRequired(true),
	)

	// CredentialsJSONFilePathField defines the path to JSON credentials file.
	CredentialsJSONFilePathField = field.StringField(
		"credentials-json-file-path",
		field.WithDisplayName("Credentials JSON File Path"),
		field.WithDescription("JSON credentials file name for the Google Workspace account. Mutually exclusive with credentials JSON"),
	)

	// CredentialsJSONField defines the JSON credentials as a string.
	CredentialsJSONField = field.StringField(
		"credentials-json",
		field.WithDisplayName("Credentials JSON"),
		field.WithDescription("JSON credentials for the Google Workspace account. Mutually exclusive with file path"),
		field.WithIsSecret(true),
	)

	// ConfigurationFields is the collection of all configuration fields.
	ConfigurationFields = []field.SchemaField{
		CustomerIDField,
		DomainField,
		AdministratorEmailField,
		CredentialsJSONFilePathField,
		CredentialsJSONField,
	}

	// Configuration combines fields into a single configuration object with connector metadata.
	Configuration = field.NewConfiguration(
		ConfigurationFields,
		field.WithConnectorDisplayName("Google Workspace"),
		field.WithIconUrl("/static/app-icons/google-workspace.svg"),
		field.WithHelpUrl("/docs/baton/google-workspace"),
		field.WithIsDirectory(true),
	)
)
