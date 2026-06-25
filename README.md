![Baton Logo](./docs/images/baton-logo.png)

# `baton-google-workspace` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-google-workspace.svg)](https://pkg.go.dev/github.com/conductorone/baton-google-workspace) ![ci](https://github.com/conductorone/baton-google-workspace/actions/workflows/ci.yaml/badge.svg) ![verify](https://github.com/conductorone/baton-google-workspace/actions/workflows/verify.yaml/badge.svg)

`baton-google-workspace` is a connector for Google Workspace built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It syncs users, groups, admin roles, and enterprise applications from the Google Admin SDK (Directory, Reports, Data Transfer), and supports provisioning for user accounts, group membership, and role assignments, plus a set of connector actions for user lifecycle and profile management.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

- A Google Workspace account with **Super Admin** access.
- A **Google Cloud project** with the **Admin SDK API** enabled (and **Cloud Identity API**; **Groups Settings API** is optional, only needed for the group-settings action).
- A **service account** with a downloaded JSON key, authorized for **domain-wide delegation** against your Workspace.
- The Workspace **Customer ID** and a **super-admin email** for the service account to impersonate.
- The relevant OAuth scopes authorized on the delegation (read-only for sync, read/write for provisioning + actions).

See [Credentials Setup](#credentials-setup) below for step-by-step instructions.

# Getting Started

## brew

```bash
brew install conductorone/baton/baton conductorone/baton/baton-google-workspace

baton-google-workspace \
  --administrator-email="$ADMIN_EMAIL" \
  --customer-id="$CUSTOMER_ID" \
  --domain="$DOMAIN" \
  --credentials-json-file-path="$CREDENTIALS_JSON_FILE_PATH"

baton resources
baton entitlements
baton grants
```

## docker

```bash
docker run --rm -v $(pwd):/out \
  -e BATON_CUSTOMER_ID="$CUSTOMER_ID" \
  -e BATON_ADMINISTRATOR_EMAIL="$ADMIN_EMAIL" \
  -e BATON_DOMAIN="$DOMAIN" \
  -e BATON_CREDENTIALS_JSON_FILE_PATH="$CREDENTIALS_JSON_FILE_PATH" \
  ghcr.io/conductorone/baton-google-workspace:latest -f "/out/sync.c1z"

docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```bash
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-google-workspace/cmd/baton-google-workspace@main

BATON_CUSTOMER_ID="$CUSTOMER_ID" \
BATON_ADMINISTRATOR_EMAIL="$ADMIN_EMAIL" \
BATON_DOMAIN="$DOMAIN" \
BATON_CREDENTIALS_JSON_FILE_PATH="$CREDENTIALS_JSON_FILE_PATH" \
baton-google-workspace

baton resources
```

# Data Model

`baton-google-workspace` syncs the following resources:

| Resource                | Description                                                                                                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Users                   | Workspace users via the Directory API (status, emails, name, org unit, manager, recovery details, custom-schema values)               |
| Groups                  | Google Groups with a `member` entitlement for membership                                                                              |
| Roles                   | Admin roles via the Directory API role-management endpoints, with a `member` entitlement for role assignment                          |
| Enterprise Applications | SAML/OIDC apps (Cloud Identity API) and OAuth apps (per-user token listing), with an assignment entitlement. Read-only (no provision) |

`baton-google-workspace` supports the following provisioning operations:

| Operation                     | Description                                                          |
| ----------------------------- | ------------------------------------------------------------------- |
| Create/Delete user            | Directory API `users.insert` / `users.delete`                       |
| Grant/Revoke group membership | Directory API `members.insert` / `members.delete`                   |
| Grant/Revoke role assignment  | Directory API `roleAssignments.insert` / `roleAssignments.delete`   |

## Connector actions

Connector actions are custom operations invoked on demand from C1 automations:

| Action | Key arguments | Description |
| ------ | ------------- | ----------- |
| `update_user_status` / `disable_user` / `enable_user` | `user_id` / `is_suspended` | Suspend or activate a user (idempotent) |
| `update_user_profile` | `user_id`, plus any of `given_name`, `family_name`, `recovery_email`, `recovery_phone`, `custom_schemas` | Partial profile update (patch semantics); supports custom-schema attribute values |
| `update_user` | `user_id` (resource ID), `user_profile` (JSON) | Profile update from a JSON object; consumed by C1 push rules for automated profile sync |
| `update_user_manager` | `user_id`, `manager_email` | Set the user's `manager` relation |
| `make_admin` | `user_id`, `status` (bool) | Promote/demote a user to/from super administrator |
| `change_user_org_unit` | `user_id`, `org_unit_path` | Move a user to a different organizational unit |
| `change_user_primary_email` | `resource_id`, `new_primary_email` | Change a user's primary email address |
| `offboarding_profile_update` | `user_id`, `archive_account` (bool) | Remove from GAL, clear recovery details, delete addresses/phones, optionally archive |
| `sign_out_user` | `user_id` | Sign the user out of all sessions and reset sign-in cookies |
| `delete_all_oauth_tokens` | `user_id` | Revoke all third-party app authorizations |
| `delete_all_application_passwords` | `user_id` | Delete all app-specific passwords |
| `transfer_user_drive_files` | `resource_id`, `target_resource_id`, `privacy_levels` | Transfer Google Drive ownership to another user |
| `transfer_user_calendar` | `resource_id`, `target_resource_id`, `release_resources` | Transfer Google Calendar data to another user |
| `create_group` | `email`, `name`, `description` | Create a new Google Group |
| `modify_group_settings` | `group_key`, plus settings flags | Update settings of an existing group |

> **Custom schemas:** `update_user_profile` and `update_user` can write values into custom-schema attributes (Directory API `customSchemas`). The connector only sets values — the schema **definitions must already exist** in the tenant (the connector does not request the `admin.directory.userschema` scope).

# Credentials Setup

A user with the **Super Admin** role in Google Workspace must perform this setup.

1. Sign in to the [Google Cloud Console](https://console.cloud.google.com) and create a project (e.g. "C1 Integration").
2. In **APIs & Services > Library**, enable the **Admin SDK API** and **Cloud Identity API** (and **Groups Settings API** if you plan to use the group-settings action).
3. In **APIs & Services > Credentials**, create a **service account**. Under **Keys > Add key > Create new key**, choose **JSON** and download it — this is `--credentials-json-file-path`. Note the service account's **Unique ID (Client ID)**.
4. In the [Admin Console](https://admin.google.com) (as Super Admin), go to **Security > Access and data control > API Controls > Manage Domain Wide Delegation > Add new**, enter the service account's **Client ID** and authorize the scopes below.
5. Copy your **Customer ID** from **Account > Account settings** (`--customer-id`).
6. (Optional) Find your **primary domain** under **Account > Domains > Manage Domains** (`--domain`).

### Required scopes

**Read-only (sync):**

```
https://www.googleapis.com/auth/admin.directory.domain.readonly, https://www.googleapis.com/auth/admin.directory.group.readonly, https://www.googleapis.com/auth/admin.directory.group.member.readonly, https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly, https://www.googleapis.com/auth/admin.directory.user.readonly, https://www.googleapis.com/auth/admin.reports.audit.readonly, https://www.googleapis.com/auth/admin.directory.user.security, https://www.googleapis.com/auth/cloud-identity.inboundsso.readonly
```

**Read/Write (sync + provisioning + actions):**

```
https://www.googleapis.com/auth/admin.directory.domain.readonly, https://www.googleapis.com/auth/admin.directory.group.readonly, https://www.googleapis.com/auth/admin.directory.group.member, https://www.googleapis.com/auth/admin.directory.rolemanagement, https://www.googleapis.com/auth/admin.directory.user, https://www.googleapis.com/auth/admin.reports.audit.readonly, https://www.googleapis.com/auth/admin.datatransfer, https://www.googleapis.com/auth/admin.directory.group, https://www.googleapis.com/auth/admin.directory.user.security, https://www.googleapis.com/auth/apps.groups.settings, https://www.googleapis.com/auth/cloud-identity.inboundsso.readonly
```

| Flag                                 | Env Var                              | Description                                                                                              | Required             |
| ------------------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------ | -------------------- |
| `--credentials-json-file-path`       | `BATON_CREDENTIALS_JSON_FILE_PATH`   | Path to the service-account JSON key file. Mutually exclusive with `--credentials-json`.                | Yes (one of the two) |
| `--credentials-json`                 | `BATON_CREDENTIALS_JSON`             | Inline service-account JSON. Mutually exclusive with the file path.                                     | Yes (one of the two) |
| `--administrator-email`              | `BATON_ADMINISTRATOR_EMAIL`          | Super-admin email the service account impersonates (domain-wide delegation subject).                    | Yes                  |
| `--customer-id`                      | `BATON_CUSTOMER_ID`                  | Google Workspace customer ID.                                                                           | Yes                  |
| `--domain`                           | `BATON_DOMAIN`                       | Primary domain to sync. If omitted, all available domains are synced.                                   | No                   |

# API Documentation

- [Admin SDK Directory API](https://developers.google.com/workspace/admin/directory/reference/rest)
- [Users: patch](https://developers.google.com/workspace/admin/directory/reference/rest/v1/users/patch)
- [Users: update](https://developers.google.com/workspace/admin/directory/reference/rest/v1/users/update)
- [Users: makeAdmin](https://developers.google.com/workspace/admin/directory/reference/rest/v1/users/makeAdmin)
- [Custom schemas](https://developers.google.com/workspace/admin/directory/reference/rest/v1/schemas)
- [Reports API](https://developers.google.com/workspace/admin/reports/reference/rest)
- [Data Transfer API](https://developers.google.com/workspace/admin/data-transfer/reference/rest)
- [Groups Settings API](https://developers.google.com/workspace/admin/groups-settings/v1/reference/groups)
- [Cloud Identity API](https://cloud.google.com/identity/docs/reference/rest)

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-google-workspace` Command Line Usage

```
baton-google-workspace

Usage:
  baton-google-workspace [flags]
  baton-google-workspace [command]

Available Commands:
  capabilities       Get connector capabilities
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --administrator-email string          An administrator email for the google workspace account. ($BATON_ADMINISTRATOR_EMAIL)
      --client-id string                    The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string                The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --credentials-json string             Json credentials for the google workspace account. Mutual exclusive with file path. ($BATON_CREDENTIALS_JSON)
      --credentials-json-file-path string   Json credentials file name for the google workspace account. Mutual exclusive with credentials JSON. ($BATON_CREDENTIALS_JSON_FILE_PATH)
      --customer-id string                  The customer Id for the google workspace account. ($BATON_CUSTOMER_ID)
      --domain string                       The domain for the google workspace account. ($BATON_DOMAIN)
  -f, --file string                         The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                                help for baton-google-workspace
      --log-format string                   The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string                    The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
  -p, --provisioning                        This must be set in order for provisioning actions to be enabled. ($BATON_PROVISIONING)
  -v, --version                             version for baton-google-workspace

Use "baton-google-workspace [command] --help" for more information about a command.

```
