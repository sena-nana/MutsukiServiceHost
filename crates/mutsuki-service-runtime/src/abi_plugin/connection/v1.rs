use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use libloading::Library;
use mutsuki_runtime_core::{RuntimeFailure, RuntimeResult};
use mutsuki_runtime_sdk::abi::{
    ABI_ENTRY_SYMBOL, ABI_TRANSPORT_VERSION, AbiEntryV1, AbiHostV1, AbiPluginV1,
};
use mutsuki_runtime_wire::{
    DEFAULT_WIRE_LIMITS, WireRequest, decode_jsonl_response, encode_jsonl_request,
};

use super::{abi_failure, abi_load_error};
use crate::abi_plugin::callbacks::{HostCallbackContext, host_release, host_request_v1};
use crate::{DeferredRuntimeClient, ServiceRuntimeResult};

pub(in crate::abi_plugin) struct V1Connection {
    plugin_id: String,
    api: AbiPluginV1,
    _library: Library,
    _host_context: Box<HostCallbackContext>,
    request_lock: Mutex<()>,
    next_request: AtomicU64,
}

unsafe impl Send for V1Connection {}
unsafe impl Sync for V1Connection {}

impl Drop for V1Connection {
    fn drop(&mut self) {
        if let Some(close) = self.api.close {
            unsafe { close(self.api.context) };
        }
    }
}

impl V1Connection {
    pub(super) fn request<R: WireRequest>(&self, request: &R) -> RuntimeResult<R::Response> {
        request.validate(DEFAULT_WIRE_LIMITS).map_err(|error| {
            abi_failure(&self.plugin_id, "abi.v1.request_invalid", error.to_string())
        })?;
        let request_id = self.next_request.fetch_add(1, Ordering::Relaxed) + 1;
        let bytes = encode_jsonl_request(request_id, request, DEFAULT_WIRE_LIMITS)
            .map_err(|error| abi_failure(&self.plugin_id, "abi.v1.encode", error.to_string()))?;
        let response = self.call(&bytes)?;
        decode_jsonl_response::<R>(&response, request_id, DEFAULT_WIRE_LIMITS)
            .map_err(RuntimeFailure::new)
    }

    fn call(&self, bytes: &[u8]) -> RuntimeResult<Vec<u8>> {
        let _guard = self
            .request_lock
            .lock()
            .expect("ABI v1 request mutex poisoned");
        let request = self.api.request.expect("validated ABI v1 request callback");
        let result = unsafe { request(self.api.context, bytes.as_ptr(), bytes.len()) };
        let valid = (result.payload.len == 0 && result.payload.ptr.is_null())
            || (result.payload.len > 0 && !result.payload.ptr.is_null());
        if !valid {
            return Err(abi_failure(
                &self.plugin_id,
                "abi.v1.response_invalid",
                "invalid payload pointer/length pair",
            ));
        }
        let response = unsafe { result.payload.as_slice() }.to_vec();
        if result.payload.len > 0 {
            unsafe { self.api.release.expect("validated ABI v1 release")(result.payload) };
        }
        match result.status {
            0 => Ok(response),
            1 => Err(abi_failure(
                &self.plugin_id,
                "abi.v1.request_failed",
                String::from_utf8_lossy(&response),
            )),
            _ => Err(abi_failure(
                &self.plugin_id,
                "abi.v1.status_invalid",
                "callback returned a status other than 0 or 1",
            )),
        }
    }

    pub(super) fn open(
        plugin_id: &str,
        staged: &Path,
        runtime: Arc<DeferredRuntimeClient>,
    ) -> ServiceRuntimeResult<Self> {
        let host_context = Box::new(HostCallbackContext { runtime });
        let host = AbiHostV1 {
            context: (&*host_context as *const HostCallbackContext)
                .cast_mut()
                .cast(),
            request: Some(host_request_v1),
            release: Some(host_release),
        };
        let library =
            unsafe { Library::new(staged) }.map_err(|error| abi_load_error(plugin_id, error))?;
        let entry: AbiEntryV1 = unsafe {
            *library
                .get::<AbiEntryV1>(ABI_ENTRY_SYMBOL)
                .map_err(|error| missing_symbol(plugin_id, error))?
        };
        let api = unsafe { entry(host) };
        if api.transport_version != ABI_TRANSPORT_VERSION
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
                    "invalid ABI v1 entry or transport version {}, expected {}",
                    api.transport_version, ABI_TRANSPORT_VERSION
                ),
            ));
        }
        Ok(Self {
            plugin_id: plugin_id.into(),
            api,
            _library: library,
            _host_context: host_context,
            request_lock: Mutex::new(()),
            next_request: AtomicU64::new(0),
        })
    }
}

fn missing_symbol(plugin_id: &str, error: libloading::Error) -> crate::ServiceRuntimeError {
    abi_load_error(
        plugin_id,
        format!(
            "missing {}: {error}",
            String::from_utf8_lossy(ABI_ENTRY_SYMBOL)
        ),
    )
}
