use std::collections::BTreeMap;
use std::sync::Arc;

use mutsuki_runtime_contracts::{
    ArtifactType, LifecyclePolicy, PermissionGrant, PluginArtifact, PluginManifest, PluginProvides,
};
use mutsuki_service_control::TerminalTuiStatus;
use mutsuki_service_plugin_loader::{HostPlugin, HostPluginCallError, HostPluginCallResult};
use serde_json::Value;

pub const PLUGIN_ID: &str = "mutsuki.terminal.tui";

pub fn plugin() -> Arc<dyn HostPlugin> {
    Arc::new(TerminalTuiPlugin {
        manifest: manifest(),
    })
}

pub struct TerminalTuiPlugin {
    manifest: PluginManifest,
}

impl HostPlugin for TerminalTuiPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn call(&self, operation: &str, _payload: Value) -> HostPluginCallResult<Value> {
        match operation {
            "status" => serde_json::to_value(TerminalTuiStatus {
                available: true,
                renderer: "ratatui".into(),
                conversation_plugin_id: mutsuki_service_plugin_conversation_sim::PLUGIN_ID.into(),
            })
            .map_err(|error| HostPluginCallError::Failed(error.to_string())),
            other => Err(HostPluginCallError::UnsupportedOperation(other.into())),
        }
    }
}

fn manifest() -> PluginManifest {
    PluginManifest {
        plugin_id: PLUGIN_ID.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        api_version: "mutsuki-plugin-v1".into(),
        artifact: PluginArtifact {
            artifact_type: ArtifactType::Native,
            path: "<builtin>".into(),
            sha256: String::new(),
        },
        provides: PluginProvides {
            effects: vec!["terminal.tui".into()],
            ..PluginProvides::default()
        },
        requires: vec![mutsuki_service_plugin_conversation_sim::PLUGIN_ID.into()],
        permissions: PermissionGrant {
            effects: Vec::new(),
            resources: Vec::new(),
        },
        lifecycle: LifecyclePolicy {
            reload_policy: "restart".into(),
            unload_timeout_ms: 1_000,
            supports_cancel: false,
            supports_dispose: true,
            supports_snapshot: false,
        },
        metadata: BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_reports_attached_renderer_and_conversation_dependency() {
        let plugin = plugin();
        let value = plugin.call("status", Value::Null).expect("status succeeds");
        let status: TerminalTuiStatus = serde_json::from_value(value).expect("valid status");

        assert!(status.available);
        assert_eq!(
            status.conversation_plugin_id,
            mutsuki_service_plugin_conversation_sim::PLUGIN_ID
        );
    }
}
