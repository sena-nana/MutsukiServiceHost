---
name: plugin-loader
description: Change ServiceHost plugin.toml discovery, manifest validation, builtin registries, native or ABI loading, external plugin deployment, RuntimeLoadPlan inputs, or reload orchestration.
---

# Plugin Loader

- Map `plugin.toml` to Core `PluginManifest` without Host-private capability semantics.
- Register builtin capabilities only from real upstream crates; otherwise report unavailable.
- Validate deployment, API version, artifacts, capabilities and secret references before boot.
- Route reload through scan, validate, surface comparison, drain and generation swap.
- Never copy StdPlugins, AgentKit, BotPlugins or business implementations into the loader.
- Keep the builtin registry manifest-only; never attach an arbitrary host-call facade to a domain plugin.

Test discovery, invalid manifests, missing artifacts/capabilities and breaking reload rejection.
