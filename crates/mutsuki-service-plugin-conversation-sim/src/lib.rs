use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mutsuki_runtime_contracts::{
    ArtifactType, LifecyclePolicy, PermissionGrant, PluginArtifact, PluginManifest, PluginProvides,
};
use mutsuki_service_control::{
    ConversationHistoryResponse, ConversationSendParams, ConversationSendResponse, ConversationTurn,
};
use mutsuki_service_plugin_loader::{HostPlugin, HostPluginCallError, HostPluginCallResult};
use serde_json::{Value, json};

pub const PLUGIN_ID: &str = "mutsuki.conversation.sim";

pub fn plugin() -> Arc<dyn HostPlugin> {
    Arc::new(ConversationSimPlugin::new())
}

pub struct ConversationSimPlugin {
    manifest: PluginManifest,
    turns: Mutex<Vec<ConversationTurn>>,
}

impl ConversationSimPlugin {
    pub fn new() -> Self {
        Self {
            manifest: manifest(),
            turns: Mutex::new(Vec::new()),
        }
    }

    fn send(&self, payload: Value) -> HostPluginCallResult<Value> {
        let params = serde_json::from_value::<ConversationSendParams>(payload)
            .map_err(|error| HostPluginCallError::BadRequest(error.to_string()))?;
        let message = params.message.trim();
        if message.is_empty() {
            return Err(HostPluginCallError::BadRequest(
                "message must not be empty".into(),
            ));
        }

        let mut turns = self
            .turns
            .lock()
            .map_err(|_| HostPluginCallError::Failed("conversation state lock poisoned".into()))?;
        let user_turn = ConversationTurn {
            sequence: turns.len() as u64 + 1,
            role: "user".into(),
            content: message.to_string(),
        };
        turns.push(user_turn);
        let reply = ConversationTurn {
            sequence: turns.len() as u64 + 1,
            role: "assistant".into(),
            content: simulated_reply(message, turns.len().div_ceil(2)),
        };
        turns.push(reply.clone());
        serde_json::to_value(ConversationSendResponse {
            reply,
            turns: turns.clone(),
        })
        .map_err(|error| HostPluginCallError::Failed(error.to_string()))
    }

    fn history(&self) -> HostPluginCallResult<Value> {
        let turns = self
            .turns
            .lock()
            .map_err(|_| HostPluginCallError::Failed("conversation state lock poisoned".into()))?
            .clone();
        serde_json::to_value(ConversationHistoryResponse { turns })
            .map_err(|error| HostPluginCallError::Failed(error.to_string()))
    }

    fn clear(&self) -> HostPluginCallResult<Value> {
        self.turns
            .lock()
            .map_err(|_| HostPluginCallError::Failed("conversation state lock poisoned".into()))?
            .clear();
        Ok(json!({ "cleared": true }))
    }
}

impl Default for ConversationSimPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl HostPlugin for ConversationSimPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn call(&self, operation: &str, payload: Value) -> HostPluginCallResult<Value> {
        // Dev/mock control facade: in-memory turns only, not ServiceHost business state.
        match operation {
            "send" => self.send(payload),
            "history" => self.history(),
            "clear" => self.clear(),
            other => Err(HostPluginCallError::UnsupportedOperation(other.into())),
        }
    }
}

fn simulated_reply(message: &str, exchange: usize) -> String {
    format!("Simulated reply {exchange}: received \"{message}\".")
}

fn manifest() -> PluginManifest {
    PluginManifest {
        plugin_id: PLUGIN_ID.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        api_version: "mutsuki-plugin-v1".into(),
        artifact: PluginArtifact {
            artifact_type: ArtifactType::Native,
            path: "<builtin-dev-mock>".into(),
            sha256: String::new(),
        },
        provides: PluginProvides {
            effects: vec!["conversation.sim".into()],
            ..PluginProvides::default()
        },
        requires: Vec::new(),
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
    fn send_appends_user_and_assistant_turns() {
        let plugin = ConversationSimPlugin::new();

        let value = plugin
            .call("send", json!({ "message": "hello" }))
            .expect("send succeeds");
        let response: ConversationSendResponse =
            serde_json::from_value(value).expect("valid send response");

        assert_eq!(response.turns.len(), 2);
        assert_eq!(response.turns[0].role, "user");
        assert_eq!(response.reply.role, "assistant");
        assert_eq!(response.reply.sequence, response.turns[1].sequence);
    }

    #[test]
    fn history_and_clear_reflect_state() {
        let plugin = ConversationSimPlugin::new();
        plugin
            .call("send", json!({ "message": "one" }))
            .expect("send succeeds");

        let history: ConversationHistoryResponse = serde_json::from_value(
            plugin
                .call("history", Value::Null)
                .expect("history succeeds"),
        )
        .expect("valid history");
        assert_eq!(history.turns.len(), 2);

        plugin.call("clear", Value::Null).expect("clear succeeds");
        let history: ConversationHistoryResponse = serde_json::from_value(
            plugin
                .call("history", Value::Null)
                .expect("history succeeds"),
        )
        .expect("valid history");
        assert!(history.turns.is_empty());
    }

    #[test]
    fn empty_message_is_rejected() {
        let plugin = ConversationSimPlugin::new();
        let error = plugin
            .call("send", json!({ "message": " " }))
            .expect_err("empty message rejected");
        assert!(matches!(error, HostPluginCallError::BadRequest(_)));
    }
}
