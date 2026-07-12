---
name: config-security
description: Change ServiceHost profiles, configuration precedence, directory layout, secret references or backends, control tokens, permission policy, environment handling, or secure defaults.
---

# Config And Security

- Keep precedence deterministic: defaults, profile, local config, environment, then CLI overrides.
- Store secret keys/references in product config and inject values at Host boundaries. A dedicated local secret file must be explicitly referenced, strict, ignored by version control, and excluded from serialization, debug output, manifests and logs.
- Resolve secret precedence as environment over the configured secret file; fail on missing files, malformed TOML, empty values or duplicate normalized keys.
- Isolate plugin, data, log and run directories and validate paths before use.
- Default control endpoints to local authenticated access and runner environments to deny-by-default allowlists.
- Fail startup on missing required secret or invalid security configuration.

Test precedence, missing secrets, redaction, path isolation and environment allowlists.
