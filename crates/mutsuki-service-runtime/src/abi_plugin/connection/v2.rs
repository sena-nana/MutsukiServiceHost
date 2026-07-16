use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use libloading::Library;
use mutsuki_runtime_core::RuntimeResult;
use mutsuki_runtime_host::BinaryTransport;
use mutsuki_runtime_sdk::abi::{
    ABI_V2_ENTRY_SYMBOL, ABI_V2_TRANSPORT_VERSION, AbiEntryV2, AbiHostV2, AbiPluginV2,
};
use mutsuki_runtime_wire::{DEFAULT_WIRE_LIMITS, ProtocolHelloAck, WireRequest};
use serde_json::Value;

use super::abi_load_error;
use super::io::{CallbackReader, CallbackWriter, callback_io};
use crate::abi_plugin::callbacks::{HostCallbackContext, host_release, host_request_v2};
use crate::{DeferredRuntimeClient, ServiceRuntimeResult};

pub(super) struct PluginLifetime {
    pub(super) api: AbiPluginV2,
    _library: Library,
    _host_context: Box<HostCallbackContext>,
}

unsafe impl Send for PluginLifetime {}
unsafe impl Sync for PluginLifetime {}

impl Drop for PluginLifetime {
    fn drop(&mut self) {
        if let Some(close) = self.api.close {
            unsafe { close(self.api.context) };
        }
    }
}

pub(in crate::abi_plugin) struct V2Connection {
    transport: BinaryTransport<CallbackReader, CallbackWriter>,
    _lifetime: Arc<PluginLifetime>,
}

impl V2Connection {
    pub(super) fn request<R: WireRequest>(&self, request: &R) -> RuntimeResult<R::Response> {
        self.transport.request(request)
    }

    pub(super) fn initialize(&self, config: Option<Value>) -> RuntimeResult<ProtocolHelloAck> {
        self.transport.initialize(config)
    }

    pub(super) fn open(
        plugin_id: &str,
        staged: &Path,
        runtime: Arc<DeferredRuntimeClient>,
    ) -> ServiceRuntimeResult<Self> {
        let host_context = Box::new(HostCallbackContext { runtime });
        let host = AbiHostV2 {
            context: (&*host_context as *const HostCallbackContext)
                .cast_mut()
                .cast(),
            request: Some(host_request_v2),
            release: Some(host_release),
        };
        let library =
            unsafe { Library::new(staged) }.map_err(|error| abi_load_error(plugin_id, error))?;
        let entry: AbiEntryV2 = unsafe {
            *library
                .get::<AbiEntryV2>(ABI_V2_ENTRY_SYMBOL)
                .map_err(|error| missing_symbol(plugin_id, error))?
        };
        let api = unsafe { entry(host) };
        if api.transport_version != ABI_V2_TRANSPORT_VERSION
            || api.context.is_null()
            || api.request.is_none()
            || api.release.is_none()
            || api.close.is_none()
        {
            if !api.context.is_null()
                && let Some(close) = api.close
            {
                unsafe { close(api.context) };
            }
            return Err(abi_load_error(
                plugin_id,
                format!(
                    "invalid ABI v2 entry or transport version {}, expected {}",
                    api.transport_version, ABI_V2_TRANSPORT_VERSION
                ),
            ));
        }
        let lifetime = Arc::new(PluginLifetime {
            api,
            _library: library,
            _host_context: host_context,
        });
        let (reader, writer) = callback_io(lifetime.clone());
        let transport = BinaryTransport::with_limits(
            reader,
            writer,
            DEFAULT_WIRE_LIMITS,
            Duration::from_secs(30),
        )
        .map_err(|error| abi_load_error(plugin_id, error))?;
        Ok(Self {
            transport,
            _lifetime: lifetime,
        })
    }
}

fn missing_symbol(plugin_id: &str, error: libloading::Error) -> crate::ServiceRuntimeError {
    abi_load_error(
        plugin_id,
        format!(
            "missing {}: {error}",
            String::from_utf8_lossy(ABI_V2_ENTRY_SYMBOL)
        ),
    )
}
