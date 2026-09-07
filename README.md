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
| Create/Delete user            | Directory API `users.insert` / `users.delete`; creation accepts initial `suspended` and `org_unit_path` |
| Delete group                  | Directory API `groups.delete` (group creation is the `create_group` connector action, below) |
| Grant/Revoke group membership | Directory API `members.insert` / `members.delete`                   |
| Grant/Revoke role assignment  | Directory API `roleAssignments.insert` / `roleAssignments.delete`   |
| Rotate user password          | SDK credential interface with encrypted supplied or generated passwords and force-change-at-next-login handling |

Creation and rotation use protected SDK credential inputs/results, never ordinary password action arguments. Configure encrypted result recipients before requesting generated credentials. A next-login password requirement applies to direct Google authentication, not third-party SSO.

User `Get` and sync pages expose presence-aware `google_user_state` facts: separate suspension/archive flags, aliases, password-change requirement and mailbox-setup state. Omitted fields are unknown; unchanged facts do not gain a wall-clock profile value. Configured filter exclusions carry qualified `NotFound` (`ErrorInfo.RESOURCE_FILTERED`), preserving targeted-sync skips without claiming provider absence. Targeted group `Get` optionally exposes `group_settings`; normal listing does not fetch settings per group.

## Connector actions

Connector actions are custom operations invoked on demand from C1 automations:

| Action | Key arguments | Description |
| ------ | ------------- | ----------- |
| `update_user_status` / `disable_user` / `enable_user` | `user_id` / `is_suspended` | Suspend or activate a user (idempotent) |
| `update_user_profile` | `user_id`, plus any of `given_name`, `family_name`, `recovery_email`, `recovery_phone`, `department`, `job_title`, `cost_center`, `employee_type`, `employee_id`, `manager_email`, `custom_schemas` | Partial profile update (patch semantics); supports Employee Information attributes and custom-schema attribute values. Exception: an `employee_id` change that reduces the number of external IDs on the account (clearing it, or consolidating duplicate entries down to the new value) uses a full-object update instead, since Google does not reliably shrink a repeated field via patch. An empty or malformed `manager_email` does not fail the whole call — see the partial-success note below. |
| `update_user` | `user_id` (resource ID), `user_profile` (JSON string; same keys as `update_user_profile` above) | Profile update from a JSON object; consumed by C1 push rules for automated profile sync. Same partial-success behavior as `update_user_profile` for `manager_email`. |
| `update_user_manager` | `user_id`, `manager_email` | Set the user's `manager` relation |
| `make_admin` | `user_id`, `status` (bool) | Promote/demote a user to/from super administrator |
| `change_user_org_unit` | `user_id`, `org_unit_path` | Move a user to a different organizational unit |
| `change_user_primary_email` | `resource_id`, `new_primary_email` | Change a user's primary email address |
| `offboarding_profile_update` | `user_id`, `archive_account` (bool) | Remove from GAL, clear recovery details, delete addresses/phones, optionally archive |
| `sign_out_user` | `user_id` | Sign the user out of all sessions and reset sign-in cookies |
| `delete_all_oauth_tokens` | `user_id` | Revoke authorizations and enumerate remaining entries; retain failed/skipped IDs and incomplete-read evidence |
| `delete_all_application_passwords` | `user_id` | Delete app-specific passwords and enumerate remaining entries; partial cleanup remains an error |
| `transfer_user_drive_files` | `resource_id`, `target_resource_id`, `privacy_levels` | Transfer Google Drive ownership to another user |
| `transfer_user_calendar` | `resource_id`, `target_resource_id`, `release_resources` | Transfer Google Calendar data to another user |
| `get_user_data_transfer` | `transfer_id`, expected `resource_id`, `target_resource_id`, `application_id`, and application parameters | Read a saved transfer; verify owners and complete parameters; return overall/per-application status without mutation |
| `remove_user_alias` | `user_id`, `alias`, `expected_primary_email`, `expected_customer_id` | Verify the exact owner and account preconditions; remove an editable alias and read back; never claim global address availability |
| `add_user_alias` | stable `user_id`, `alias`, `expected_primary_email`, `expected_customer_id` | Add an alias to a pinned account in the configured customer and verify attachment; same-account replay is idempotent, foreign-user/group collisions never move or remove an alias |
| `create_group` | `email`, `name`, `description` | Create a new Google Group |
| `modify_group_settings` | `group_key`, plus settings flags | Update supplied privacy/membership/discovery/join/GAL settings and return an independently observed group resource |

> **Custom schemas:** `update_user_profile` and `update_user` can write values into custom-schema attributes (Directory API `customSchemas`). The connector only sets values — the schema **definitions must already exist** in the tenant (the connector does not request the `admin.directory.userschema` scope).

> **Job title round-trip:** the synced user profile exposes the job title under both `title` and `job_title` for backward compatibility. `update_user`'s `user_profile` JSON object accepts any of `job_title`, `jobTitle`, or `title` as the source key. `update_user_profile` has a fixed schema and only exposes `job_title` as an argument name — pass the value under that key.

> **Partial success and `manager_email`:** `update_user_profile`/`update_user` never clear an assigned manager through this action (matching `update_user_manager`), so an empty or invalid `manager_email` is not applied — but unlike other invalid fields, it does not fail the whole call when at least one other field in the same payload is valid. The response's `success: true` only means the call completed; check the `skipped_fields` return field (a comma-separated list naming any provided field that wasn't applied, and why) to detect this — a caller that checks `success` alone will not be told that `manager_email` specifically was skipped.

> **Read-modify-write safety:** profile changes that preserve existing names or array entries send the observed ETag as `If-Match`. A missing version or concurrent change fails rather than overwriting unrelated changes. `updated_fields` describes requested changes, not independently verified state.

> **Credential cleanup evidence:** check `inventory_complete` before interpreting `remaining_ids`. Failed security actions retain their per-item results. Empty results after a failed enumeration are not absence, and login-derived application grants are not a live credential inventory.

> **Transfers:** submission is acknowledgement, not completion. Keep the provider transfer ID and approved parameters, then use `get_user_data_transfer`; never poll by replaying a mutation. Drive wire privacy values are uppercase and preserve the existing default of both private/shared. Calendar retain-resources uses the documented empty parameter set. Conflicting, unknown, or truncated discovery never triggers a new insert.

### Requestable alias creation

Both alias actions require a concrete Google customer ID in connector configuration, not the `my_customer` selector, and compare it with the observed account independently of submitted pins. No additional scope or selector-resolution service is introduced. `add_user_alias` is a user-resource action whose target must be a stable Google user ID, not an email/alias selector; the primary-email and customer preconditions must also match the observed account. The provider validates alias namespace ownership, including secondary domains in that customer; the configured sync-selection domain is not an alias write allowlist.

Check `outcome`, `alias_present_before`, `alias_present_after`, and `observation_complete`. Presence is omitted when unreadable, not defaulted to false. `insert_attempted` and `insert_acknowledged` distinguish a provider call/acknowledgment from verified attachment; no acknowledgment does not prove no mutation. A same-account alias verified in the dedicated collection is an attachment-only, zero-write `already_present` result even if also listed as noneditable; this does not make it editable. New insertion/replacement of a noneditable-only alias, foreign-user/group ownership, primary-address misuse, and mismatched account preconditions fail without moving/removing anything. Unknown inserts are not retried automatically.

**Self-service authorization is separate.** Publishing this connector action does not create or enable a customer-facing C1 requestable Action. Administrators must configure its audience, approval policy, and trusted form/resource bindings so the target comes from the authorized requester/resource relationship and the alias/domain is allowed. Arbitrary submitted `user_id` or `expected_customer_id` values are not ownership or authorization evidence. Verifying that requesters cannot substitute another account or escape alias/domain policy is a separate C1 configuration acceptance gate; this connector implements no new C1 UI/backend or approval system.

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
