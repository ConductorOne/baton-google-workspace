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
	CredentialsJSONFilePathField = field.FileUploadField(
		"credentials-json-file-path",
		[]string{".json"},
		field.WithDisplayName("Credentials JSON file"),
		field.WithDescription("JSON credentials file for the Google Workspace account."),
		field.WithIsSecret(true),
	)

	// CredentialsJSONField defines the JSON credentials as a string.
	CredentialsJSONField = field.StringField(
		"credentials-json",
		field.WithDisplayName("Credentials JSON string"),
		field.WithDescription("JSON credentials passed as a string for the Google Workspace account. Mutually exclusive with file path"),
		field.WithExportTarget(field.ExportTargetCLIOnly),
		field.WithIsSecret(true),
	)

	// SyncAppsField enables enterprise application sync.
	// Required scopes:
	//   - admin.directory.user.readonly (list users for OAuth token discovery)
	//   - admin.directory.user.security (read per-user OAuth tokens)
	//   - admin.reports.audit.readonly (SAML + Google sign-in audit logs)
	// Optional scope (SAML app IDs fall back to display names if missing):
	//   - cloud-identity.inboundsso.readonly (stable SAML app IDs)
	SyncAppsField = field.BoolField(
		"sync-apps",
		field.WithDisplayName("Sync Enterprise Apps"),
		field.WithDescription("Sync enterprise applications and user login activity from the Google Workspace account."),
	)

	// Field relationships define constraints between fields.
	fieldRelationships = []field.SchemaFieldRelationship{
		field.FieldsMutuallyExclusive(
			CredentialsJSONFilePathField,
			CredentialsJSONField,
		),
	}

	// ConfigurationFields is the collection of all configuration fields.
	ConfigurationFields = []field.SchemaField{
		CustomerIDField,
		DomainField,
		AdministratorEmailField,
		CredentialsJSONFilePathField,
		CredentialsJSONField,
		SyncAppsField,
	}

	// Configuration combines fields into a single configuration object with connector metadata.
	Configuration = field.NewConfiguration(
		ConfigurationFields,
		field.WithConstraints(fieldRelationships...),
		field.WithConnectorDisplayName("Google Workspace"),
		field.WithIconUrl("/static/app-icons/google-workspace.svg"),
		field.WithHelpUrl("/docs/baton/google-workspace-v2"),
		field.WithIsDirectory(true),
	)
)
