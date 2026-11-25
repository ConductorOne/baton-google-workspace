//go:generate go run ./gen
package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

// Auth methods, the names of the field groups below. ConductorOne matches
// WorkloadIdentityC1AuthMethod and WorkloadIdentityProviderField by name, so
// they must not change.
const (
	ServiceAccountKeyAuthMethod  = "service-account-key"
	WorkloadIdentityC1AuthMethod = "workload-identity-c1"
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

	// WorkloadIdentityProviderField is the customer's Google workload identity
	// pool provider, which trusts the ConductorOne issuer. ConductorOne reads the
	// same field to set the audience of the JWT it issues.
	WorkloadIdentityProviderField = field.StringField(
		"workload-identity-provider",
		field.WithDisplayName("Workload identity provider"),
		field.WithDescription("Workload identity only: resource name of the Google workload identity pool provider that trusts ConductorOne: "+
			"projects/<project number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>"),
		// Required only within the workload identity group: validation skips
		// fields the selected group does not list.
		field.WithRequired(true),
	)

	// ServiceAccountEmailField is the service account the connector impersonates.
	// It must have domain-wide delegation for the connector's scopes.
	ServiceAccountEmailField = field.StringField(
		"service-account-email",
		field.WithDisplayName("Service account email"),
		field.WithDescription("Workload identity only: email of the service account with domain-wide delegation. "+
			"The workload identity pool's principal needs the Service Account Token Creator role on it."),
		field.WithRequired(true),
	)

	// C1TokenField makes the hosted runtime attach the token source that
	// delivers the JWT ConductorOne issues for this connector. It is never set
	// by hand.
	C1TokenField = field.Oauth2Field(
		"google-workspace-c1-token",
		field.WithDisplayName("ConductorOne token"),
		field.WithDescription("JWT ConductorOne issues for a hosted connector. The hosted runtime supplies it; leave it unset anywhere else."),
		field.WithHidden(true),
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
		WorkloadIdentityProviderField,
		ServiceAccountEmailField,
		C1TokenField,
	}

	// commonFields apply to every auth method. Validation skips any field the
	// selected group does not list, so each group repeats them.
	commonFields = []field.SchemaField{
		CustomerIDField,
		DomainField,
		AdministratorEmailField,
	}

	// The service account key group is the default and is listed first: the
	// SDK falls back to the default group, then the first one, when no auth
	// method is stored, which is the case for every connector configured before
	// these groups existed.
	fieldGroups = []field.SchemaFieldGroup{
		{
			Name:        ServiceAccountKeyAuthMethod,
			DisplayName: "Service account key",
			HelpText:    "Authenticate with a service account JSON key that has domain-wide delegation.",
			Default:     true,
			Fields:      append(append([]field.SchemaField{}, commonFields...), CredentialsJSONFilePathField, CredentialsJSONField),
		},
		{
			Name:        WorkloadIdentityC1AuthMethod,
			DisplayName: "Workload identity (ConductorOne)",
			HelpText: "For a ConductorOne-hosted connector, without a stored key. ConductorOne issues a short-lived JWT for this connector, " +
				"Google exchanges it through your workload identity pool provider, and the connector impersonates the service account " +
				"with domain-wide delegation.",
			Fields: append(append([]field.SchemaField{}, commonFields...), WorkloadIdentityProviderField, ServiceAccountEmailField, C1TokenField),
		},
	}

	// Configuration combines fields into a single configuration object with connector metadata.
	Configuration = field.NewConfiguration(
		ConfigurationFields,
		field.WithFieldGroups(fieldGroups),
		field.WithConstraints(fieldRelationships...),
		field.WithConnectorDisplayName("Google Workspace"),
		field.WithIconUrl("/static/app-icons/google-workspace.svg"),
		field.WithHelpUrl("/docs/baton/google-workspace-v2"),
		field.WithIsDirectory(true),
	)
)
