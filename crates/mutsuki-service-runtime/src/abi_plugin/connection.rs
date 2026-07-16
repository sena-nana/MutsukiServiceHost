use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use libloading::Library;
use mutsuki_runtime_contracts::{RuntimeError, ScalarValue};
use mutsuki_runtime_core::{RuntimeFailure, RuntimeResult};
use mutsuki_runtime_host::TypedRequestTransport;
use mutsuki_runtime_sdk::abi::{
    ABI_ENTRY_SYMBOL, ABI_TRANSPORT_VERSION, AbiEntryV1, AbiHostV1, AbiPluginV1,
};
use mutsuki_runtime_wire::{
    DEFAULT_WIRE_LIMITS, WireRequest, decode_jsonl_response, encode_jsonl_request,
};

use super::callbacks::{HostCallbackContext, host_release, host_request};
use crate::{DeferredRuntimeClient, ServiceRuntimeError, ServiceRuntimeResult};

pub(super) struct AbiConnection {
    plugin_id: String,
    api: AbiPluginV1,
    _library: Library,
    _host_context: Box<HostCallbackContext>,
    request_lock: Mutex<()>,
    next_request: AtomicU64,
}

// ABI v1 serializes guest calls through request_lock. The library and callback context remain
// owned by the connection for every adapter and callback lifetime.
unsafe impl Send for AbiConnection {}
unsafe impl Sync for AbiConnection {}

impl Drop for AbiConnection {
    fn drop(&mut self) {
        if let Some(close) = self.api.close {
            // SAFETY: context was returned by the paired entry and is closed exactly once here.
            unsafe { close(self.api.context) };
        }
    }
}

impl TypedRequestTransport for AbiConnection {
    fn request<R: WireRequest>(&self, request: &R) -> RuntimeResult<R::Response> {
        request.validate(DEFAULT_WIRE_LIMITS).map_err(|error| {
            abi_failure(&self.plugin_id, "abi.request_invalid", error.to_string())
        })?;
        let request_id = self.next_request.fetch_add(1, Ordering::Relaxed) + 1;
        let bytes = encode_jsonl_request(request_id, request, DEFAULT_WIRE_LIMITS)
            .map_err(|error| abi_failure(&self.plugin_id, "abi.encode", error.to_string()))?;
        let response = self.call(&bytes)?;
        decode_jsonl_response::<R>(&response, request_id, DEFAULT_WIRE_LIMITS)
            .map_err(RuntimeFailure::new)
    }
}

impl AbiConnection {
    fn call(&self, bytes: &[u8]) -> RuntimeResult<Vec<u8>> {
        let _guard = self
            .request_lock
            .lock()
            .expect("ABI request mutex poisoned");
        let request = self.api.request.ok_or_else(|| {
            abi_failure(
                &self.plugin_id,
                "abi.entry_invalid",
                "request callback is missing",
            )
        })?;
        // SAFETY: plugin context and callback remain valid while this connection owns the library.
        let result = unsafe { request(self.api.context, bytes.as_ptr(), bytes.len()) };
        // SAFETY: plugin owns the response until its paired release callback is invoked below.
        let response = unsafe { result.payload.as_slice() }.to_vec();
        if let Some(release) = self.api.release {
            // SAFETY: release is paired with the plugin-owned response buffer.
            unsafe { release(result.payload) };
        }
        if result.status != 0 {
            return Err(abi_failure(
                &self.plugin_id,
                "abi.request_failed",
                String::from_utf8_lossy(&response),
            ));
        }
        Ok(response)
    }
}

pub(super) fn open_connection(
    plugin_id: &str,
    staged: &Path,
    runtime: Arc<DeferredRuntimeClient>,
) -> ServiceRuntimeResult<Arc<AbiConnection>> {
    let host_context = Box::new(HostCallbackContext { runtime });
    let host = AbiHostV1 {
        context: (&*host_context as *const HostCallbackContext)
            .cast_mut()
            .cast(),
        request: Some(host_request),
        release: Some(host_release),
    };
    // SAFETY: artifact hash/path/extension were validated before loading. AbiConnection retains
    // the library for every callback and adapter lifetime.
    let library =
        unsafe { Library::new(staged) }.map_err(|error| abi_load_error(plugin_id, error))?;
    // SAFETY: the symbol and function layout are the versioned Core ABI v1 contract.
    let entry: AbiEntryV1 = unsafe {
        *library
            .get::<AbiEntryV1>(ABI_ENTRY_SYMBOL)
            .map_err(|error| {
                abi_load_error(
                    plugin_id,
                    format!(
                        "missing {}: {error}",
                        String::from_utf8_lossy(ABI_ENTRY_SYMBOL)
                    ),
                )
            })?
    };
    // SAFETY: host callbacks and context remain valid in the resulting connection.
    let api = unsafe { entry(host) };
    if api.transport_version != ABI_TRANSPORT_VERSION
        || api.context.is_null()
        || api.request.is_none()
        || api.release.is_none()
        || api.close.is_none()
    {
        return Err(abi_load_error(
            plugin_id,
            format!(
                "invalid entry surface or transport version {}, expected {}",
                api.transport_version, ABI_TRANSPORT_VERSION
            ),
        ));
    }
    Ok(Arc::new(AbiConnection {
        plugin_id: plugin_id.into(),
        api,
        _library: library,
        _host_context: host_context,
        request_lock: Mutex::new(()),
        next_request: AtomicU64::new(0),
    }))
}

fn abi_load_error(plugin_id: &str, detail: impl ToString) -> ServiceRuntimeError {
    ServiceRuntimeError::AbiPlugin {
        plugin_id: plugin_id.into(),
        detail: detail.to_string(),
    }
}

fn abi_failure(
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
