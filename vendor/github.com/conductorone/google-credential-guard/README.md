# google-credential-guard

Validate Google service-account JSON supplied by an external party before using it in a Google SDK. This module intentionally accepts only service-account keys. It does not accept arbitrary ADC or Workload Identity Federation configurations.

```go
key, err := googlecredentialguard.ParseServiceAccountJSON(uploadedJSON)
if err != nil { return err }
opt, err := key.ClientOption()
if err != nil { return err }
client, err := bigquery.NewClient(ctx, projectID, opt)
```

`ParseServiceAccountJSON` enforces a 64 KiB limit, rejects duplicate and unsupported fields, checks the service-account key and email, and pins the OAuth token endpoint. `CanonicalJSON()` returns a copy for APIs that only accept bytes; prefer the type-specific adapters. No method reads a file or makes a network request. Validate at both configuration admission and use time so stored legacy configurations are covered.

This package accepts JSON bytes and never opens a path. A CLI whose local operator chooses a credential file may read at most `MaxJSONBytes` from that file and pass those bytes to `ParseServiceAccountJSON`. A hosted service must accept uploaded JSON bytes; it must never let a tenant choose a path on the server. Application Default Credentials, if supported, must be a separate, explicit auth mode rather than a fallback from invalid JSON.
