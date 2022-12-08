![Baton Logo](./docs/images/baton-logo.png)

# `baton-google-workspace` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-google-workspace.svg)](https://pkg.go.dev/github.com/conductorone/baton-google-workspace) ![main ci](https://github.com/conductorone/baton-google-workspace/actions/workflows/main.yaml/badge.svg)

`baton-google-workspace` is a connector for Google Workspace built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It communicates with the Google Workspace API to sync data about groups, roles, and users.

Check out [Baton](https://github.com/conductorone/baton) to learn more the project in general.

# Getting Started

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-google-workspace
baton-google-workspace
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_CUSTOMER_ID=customerID -e BATON_ADMINISTRATOR_EMAIL=administratorEmail -e BATON_CREDENTIALS_JSON_FILE_PATH=credentialsJsonFilePath
-e BATON_DOMAIN=domain ghcr.io/conductorone/baton-google-workspace:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-google-workspace/cmd/baton-google-workspace@main

BATON_CUSTOMER_ID=customerID BATON_ADMINISTRATOR_EMAIL=administratorEmail BATON_CREDENTIALS_JSON_FILE_PATH=credentialsJsonFilePath BATON_DOMAIN=domain
baton resources
```

# Data Model

`baton-google-workspace` will pull down information about the following Google Workspace resources:
- Groups
- Users
- Roles

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
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
  -f, --file string                         The path to the c1z file to sync with ($C1_FILE) (default "sync.c1z")
      --customer-id string                  The customer Id for the google workspace account. ($BATON_CUSTOMER_ID)
      --domain string                       The domain for the google workspace account. ($BATON_DOMAIN)
      --administrator-email string          An administrator email for the google workspace account. ($BATON_ADMINISTRATOR_EMAIL)
      --credentials-json-file-path string   Json credentials file name for the google workspace account. ($BATON_CREDENTIALS_JSON_FILE_PATH)
  -h, --help                                help for baton-google-workspace
      --log-format string                   The output format for logs: json, console ($C1_LOG_FORMAT) (default "json")
      --log-level string                    The log level: debug, info, warn, error ($C1_LOG_LEVEL) (default "info")
  -v, --version                             version for baton-google-workspace

Use "baton-google-workspace [command] --help" for more information about a command.

```
