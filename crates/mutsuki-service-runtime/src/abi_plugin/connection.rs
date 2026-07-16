mod io;
mod v1;
mod v2;

use std::path::Path;
use std::sync::Arc;

use mutsuki_runtime_contracts::{PluginDeploymentKind, PluginManifest, RuntimeError, ScalarValue};
use mutsuki_runtime_core::{RuntimeFailure, RuntimeResult};
use mutsuki_runtime_host::TypedRequestTransport;
use mutsuki_runtime_sdk::abi::{ABI_BRIDGE_ID, ABI_CODEC_ID, ABI_V2_BRIDGE_ID, ABI_V2_CODEC_ID};
use mutsuki_runtime_wire::{InitializeRequest, ProtocolHello, ProtocolHelloAck, WireRequest};
use serde_json::Value;

use crate::{DeferredRuntimeClient, ServiceRuntimeError, ServiceRuntimeResult};

use self::v1::V1Connection;
use self::v2::V2Connection;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AbiTransport {
    JsonlV1,
    BinaryV2,
}

pub(super) enum AbiConnection {
    V1(V1Connection),
    V2(V2Connection),
}

unsafe impl Send for AbiConnection {}
unsafe impl Sync for AbiConnection {}

impl AbiConnection {
    pub(super) fn hello(&self) -> ProtocolHello {
        match self {
            Self::V1(_) => ProtocolHello::debug_jsonl(),
            Self::V2(_) => ProtocolHello::binary(),
        }
    }

    pub(super) fn initialize(&self, config: Option<Value>) -> RuntimeResult<ProtocolHelloAck> {
        match self {
            Self::V1(connection) => connection.request(&InitializeRequest {
                hello: ProtocolHello::debug_jsonl(),
                config,
            }),
            Self::V2(connection) => connection.initialize(config),
        }
    }
}

impl TypedRequestTransport for AbiConnection {
    fn request<R: WireRequest>(&self, request: &R) -> RuntimeResult<R::Response> {
        match self {
            Self::V1(connection) => connection.request(request),
            Self::V2(connection) => connection.request(request),
        }
    }
}

pub(super) fn open_connection(
    manifest: &PluginManifest,
    staged: &Path,
    runtime: Arc<DeferredRuntimeClient>,
) -> ServiceRuntimeResult<Arc<AbiConnection>> {
    let transport = select_transport(manifest)?;
    let connection = match transport {
        AbiTransport::JsonlV1 => {
            AbiConnection::V1(V1Connection::open(&manifest.plugin_id, staged, runtime)?)
        }
        AbiTransport::BinaryV2 => {
            AbiConnection::V2(V2Connection::open(&manifest.plugin_id, staged, runtime)?)
        }
    };
    Ok(Arc::new(connection))
}

fn select_transport(manifest: &PluginManifest) -> ServiceRuntimeResult<AbiTransport> {
    let backends = manifest
        .provides
        .plugin_backends
        .iter()
        .filter(|backend| backend.deployment_kind == PluginDeploymentKind::Abi)
        .collect::<Vec<_>>();
    let [backend] = backends.as_slice() else {
        return Err(abi_load_error(
            &manifest.plugin_id,
            "manifest must declare exactly one ABI plugin backend",
        ));
    };
    let pair = (backend.codec_id.as_deref(), backend.bridge_id.as_deref());
    let transport = match pair {
        (Some(ABI_CODEC_ID), Some(ABI_BRIDGE_ID)) => AbiTransport::JsonlV1,
        (Some(ABI_V2_CODEC_ID), Some(ABI_V2_BRIDGE_ID)) => AbiTransport::BinaryV2,
        _ => {
            return Err(abi_load_error(
                &manifest.plugin_id,
                format!("unsupported ABI codec/bridge pair: {pair:?}"),
            ));
        }
    };
    Ok(transport)
}

pub(super) fn abi_load_error(plugin_id: &str, detail: impl ToString) -> ServiceRuntimeError {
    ServiceRuntimeError::AbiPlugin {
        plugin_id: plugin_id.into(),
        detail: detail.to_string(),
    }
}

pub(super) fn abi_failure(
    plugin_id: &str,
    route: impl Into<String>,
    detail: impl Into<String>,
) -> RuntimeFailure {
    let mut error = RuntimeError::new(
        mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
        format!("plugin:{plugin_id}"),
        route,
    );
    error
        .evidence
        .insert("detail".into(), ScalarValue::String(detail.into()));
    RuntimeFailure::new(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_selects_exact_versioned_transport_without_fallback() {
        let v2 = mutsuki_service_abi_fixture::fixture_manifest("test.so", "sha256:test");
        assert_eq!(select_transport(&v2).unwrap(), AbiTransport::BinaryV2);

        let mut v1 = v2.clone();
        v1.plugin_id = "test.v1".into();
        v1.provides.plugin_backends[0].codec_id = Some(ABI_CODEC_ID.into());
        v1.provides.plugin_backends[0].bridge_id = Some(ABI_BRIDGE_ID.into());
        assert_eq!(select_transport(&v1).unwrap(), AbiTransport::JsonlV1);

        v1.provides.plugin_backends[0].codec_id = Some("unknown".into());
        assert!(select_transport(&v1).is_err());
    }
}
