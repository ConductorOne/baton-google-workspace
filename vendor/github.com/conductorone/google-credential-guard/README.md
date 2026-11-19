# google-credential-guard

Validate Google service-account JSON supplied by an external party before using it in a Google SDK. This module intentionally accepts only service-account keys. It does not accept arbitrary ADC or Workload Identity Federation configurations.

```go
key, err := googlecredentialguard.ParseServiceAccountJSON(uploadedJSON)
if err != nil { return err }
opt, err := key.ClientOption()
if err != nil { return err }
client, err := bigquery.NewClient(ctx, projectID, opt)
```

`ParseServiceAccountJSON` enforces a 64 KiB limit, rejects duplicate and unsupported fields, and checks the service-account key and email. It accepts RSA private keys in Google's PKCS#8 format and older PKCS#1 format, normalizing the latter to PKCS#8. It accepts only Google's two documented service-account `token_uri` values (`https://oauth2.googleapis.com/token` and `https://accounts.google.com/o/oauth2/token`) and rewrites either to the pinned `https://oauth2.googleapis.com/token` endpoint in canonical JSON. Unknown fields are still rejected; simple field names appear in errors to help diagnose format changes, while unusual names and all values remain redacted. `CanonicalJSON()` returns a copy for APIs that only accept bytes; prefer the type-specific adapters. No method reads a file or makes a network request. Validate at both configuration admission and use time so stored configurations are covered.

This package accepts JSON bytes and never opens a path. A CLI whose local operator chooses a credential file may read at most `MaxJSONBytes` from that file and pass those bytes to `ParseServiceAccountJSON`. A hosted service must accept uploaded JSON bytes; it must never let a tenant choose a path on the server. Application Default Credentials, if supported, must be a separate, explicit auth mode rather than a fallback from invalid JSON.

The two accepted token URI spellings appear in Google's current [service-account key format examples](https://docs.cloud.google.com/iam/docs/keys-create-delete). The guard accepts those exact values as input and uses the pinned modern endpoint for every exchange.
