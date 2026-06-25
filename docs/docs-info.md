## Connector capabilities

1. What resources does the connector sync?
   - **Users** (`user`) — All Google Workspace users via the Admin SDK Directory API (`users.list`, projection `full`), including status (active/suspended), primary and alias emails, name, organizational unit, manager relation, recovery email/phone, and custom-schema attribute values.
   - **Groups** (`group`) — All Google Groups via the Directory API (`groups.list`, `members.list`). Each group exposes a `member` entitlement representing membership (granted to users and nested groups).
   - **Roles** (`role`) — All admin roles via the Directory API role-management endpoints (`roles.list`, `roleAssignments.list`). Each role exposes a `member` entitlement representing role assignment (granted to users and groups).
   - **Enterprise Applications** (`application`) — SAML/OIDC apps discovered via the Cloud Identity API and OAuth apps discovered via per-user token listing (`tokens.list`). Each application exposes an assignment entitlement granted to users. **Read-only** (no provisioning).

2. Can the connector provision any resources? If so, which ones?

   Yes:
   - **Create/Delete user accounts** — via Directory API `users.insert` and `users.delete`. New accounts are created with a generated random password.
   - **Grant/Revoke group membership** — via Directory API `members.insert` and `members.delete`.
   - **Grant/Revoke role assignment** — via Directory API `roleAssignments.insert` and `roleAssignments.delete`.
   - **Create groups** — via the `create_group` connector action (Directory API `groups.insert`).

   In addition, the connector ships a set of **connector actions** (custom operations invoked on demand from C1 automations):

   | Action | API endpoint(s) | Purpose |
   | --- | --- | --- |
   | `update_user_status` / `disable_user` / `enable_user` | `users.update` | Suspend / activate a user (idempotent) |
   | `update_user_profile` | `users.patch` | Partial profile update (name, recovery details, custom-schema values) with patch semantics |
   | `update_user` | `users.patch` | Profile update from a `user_profile` JSON object; consumed by C1 push rules for automated profile sync |
   | `update_user_manager` | `users.update` | Set the user's `manager` relation |
   | `make_admin` | `users.makeAdmin` | Promote/demote a user to/from super administrator |
   | `change_user_org_unit` | `users.update` | Move a user to a different organizational unit |
   | `change_user_primary_email` | `users.update` | Change a user's primary email address |
   | `offboarding_profile_update` | `users.patch` | Remove from GAL, clear recovery details, delete addresses/phones, optionally archive |
   | `sign_out_user` | `users.signOut` | Sign the user out of all sessions and reset sign-in cookies |
   | `delete_all_oauth_tokens` | `tokens.delete` | Revoke all third-party app authorizations |
   | `delete_all_application_passwords` | `asps.delete` | Delete all app-specific passwords |
   | `transfer_user_drive_files` | Data Transfer API | Transfer Google Drive ownership to another user |
   | `transfer_user_calendar` | Data Transfer API | Transfer Google Calendar data to another user |
   | `create_group` | `groups.insert` | Create a new Google Group |
   | `modify_group_settings` | Groups Settings API | Update settings of an existing group |

   **Custom schemas:** the profile-update actions (`update_user_profile`, `update_user`) can write values into custom-schema attributes via the Directory API `customSchemas` field. The connector only **sets values**; the schema **definitions must already exist** in the Workspace tenant (managed by the customer in Admin Console — the connector does not create or delete schema definitions and does not request the `admin.directory.userschema` scope).

## Connector credentials

1. What credentials or information are needed to set up the connector?

   | Flag | Env Var | Description | Required |
   | --- | --- | --- | --- |
   | `--credentials-json-file-path` | `BATON_CREDENTIALS_JSON_FILE_PATH` | Path to the Google service-account JSON key file. Mutually exclusive with `--credentials-json`. | Yes (one of the two) |
   | `--credentials-json` | `BATON_CREDENTIALS_JSON` | Service-account JSON credentials inline. Mutually exclusive with the file path. | Yes (one of the two) |
   | `--administrator-email` | `BATON_ADMINISTRATOR_EMAIL` | Super-admin email the service account impersonates via domain-wide delegation (the delegation `subject`). | Yes |
   | `--customer-id` | `BATON_CUSTOMER_ID` | Google Workspace customer ID. | Yes |
   | `--domain` | `BATON_DOMAIN` | Primary domain to sync. If omitted, all available domains are synced. | No |

2. For each item in the list above:

   ### How to obtain the credentials (step by step)

   The connector authenticates as a **Google Cloud service account** with **domain-wide delegation**, impersonating a Workspace super admin. A user with the **Super Admin** role in Google Workspace must perform this setup.

   **Step 1: Create a Google Cloud project**

   Sign in to [https://console.cloud.google.com](https://console.cloud.google.com) and create a new project (e.g. "C1 Integration").

   **Step 2: Enable the required APIs**

   In **APIs & Services > Library**, enable:
   - **Admin SDK API** (required — Directory + Reports)
   - **Cloud Identity API** (used to resolve SAML app IDs to stable identifiers)
   - **Groups Settings API** (optional — only needed for the `modify_group_settings` action)

   **Step 3: Create a service account and JSON key**

   In **APIs & Services > Credentials**, create a service account, then under **Keys > Add key > Create new key**, choose **JSON** and download the file. This file is the value for `--credentials-json-file-path`. Note the service account's **Unique ID (Client ID)** — you need it for delegation.

   **Step 4: Authorize domain-wide delegation**

   In [https://admin.google.com](https://admin.google.com) as Super Admin, go to **Security > Access and data control > API Controls > Manage Domain Wide Delegation > Add new**. Enter the service account's **Client ID** and the OAuth scopes (see table below).

   **Step 5: Collect the remaining values**

   - **Customer ID** — Admin Console > **Account > Account settings** (`--customer-id`).
   - **Administrator email** — a super-admin email the service account will impersonate (`--administrator-email`).
   - **Primary domain** — Admin Console > **Account > Domains > Manage Domains** (`--domain`, optional).

   ### Required scopes

   Authorize these scopes on the service account's domain-wide delegation.

   **Read-only (sync only):**

   | Scope | Purpose |
   | --- | --- |
   | `admin.directory.domain.readonly` | Identify the primary domain |
   | `admin.directory.group.readonly` | Sync groups |
   | `admin.directory.group.member.readonly` | Sync group membership |
   | `admin.directory.rolemanagement.readonly` | Sync roles and assignments |
   | `admin.directory.user.readonly` | Sync users |
   | `admin.reports.audit.readonly` | Sync usage/admin events (incremental sync) |
   | `admin.directory.user.security` | Discover OAuth apps via per-user token listing |
   | `cloud-identity.inboundsso.readonly` | (Optional) Resolve SAML app IDs to stable identifiers |

   **Read/Write (sync + provisioning + actions):** all of the above (with the write variants below) plus:

   | Scope | Purpose |
   | --- | --- |
   | `admin.directory.user` | Create/delete/update users; profile and custom-schema updates; `makeAdmin` |
   | `admin.directory.group` | Provision groups |
   | `admin.directory.group.member` | Manage group membership |
   | `admin.directory.rolemanagement` | Manage role assignments |
   | `admin.datatransfer` | Transfer Drive/Calendar data between users |
   | `admin.directory.user.security` | Sign out users, delete OAuth tokens / app passwords |
   | `apps.groups.settings` | Edit group settings (`modify_group_settings`) |

   > Setting custom-schema **values** uses `admin.directory.user`. Managing schema **definitions** (`admin.directory.userschema`) is intentionally out of scope — the connector assumes definitions already exist in the tenant.

   ### API documentation

   - Admin SDK Directory API: https://developers.google.com/workspace/admin/directory/reference/rest
   - Users: `patch` — https://developers.google.com/workspace/admin/directory/reference/rest/v1/users/patch
   - Users: `update` — https://developers.google.com/workspace/admin/directory/reference/rest/v1/users/update
   - Users: `makeAdmin` — https://developers.google.com/workspace/admin/directory/reference/rest/v1/users/makeAdmin
   - Custom schemas: https://developers.google.com/workspace/admin/directory/reference/rest/v1/schemas
   - Reports API (audit/usage events): https://developers.google.com/workspace/admin/reports/reference/rest
   - Data Transfer API: https://developers.google.com/workspace/admin/data-transfer/reference/rest
   - Groups Settings API: https://developers.google.com/workspace/admin/groups-settings/v1/reference/groups
   - Cloud Identity API: https://cloud.google.com/identity/docs/reference/rest

   ### Rate limits

   The Admin SDK Directory API enforces per-project and per-user quotas (default ~2,400 queries/minute/project for the Directory API). The connector relies on the Baton SDK's HTTP client for retry/backoff on `429`/`5xx` responses.
