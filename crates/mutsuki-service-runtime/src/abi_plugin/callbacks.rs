use std::ffi::c_void;
use std::ptr;
use std::sync::Arc;

use mutsuki_runtime_sdk::abi::{AbiBuffer, AbiCallResult};
use mutsuki_runtime_sdk::{dispatch_binary_host_request, dispatch_host_request};

use crate::DeferredRuntimeClient;

pub(super) struct HostCallbackContext {
    pub(super) runtime: Arc<DeferredRuntimeClient>,
}

pub(super) unsafe extern "C" fn host_request_v1(
    context: *mut c_void,
    request: *const u8,
    request_len: usize,
) -> AbiCallResult {
    if context.is_null() || (request.is_null() && request_len != 0) {
        return AbiCallResult::failed(b"invalid host callback pointers".to_vec());
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: context is owned by AbiConnection and request is valid for this callback.
        let context = unsafe { &*context.cast::<HostCallbackContext>() };
        // SAFETY: pointer/length are validated above and borrowed only for this call.
        let request = if request_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(request, request_len) }
        };
        dispatch_host_request(context.runtime.as_ref(), context.runtime.as_ref(), request)
    }));
    match result {
        Ok(response) => AbiCallResult::ok(response),
        Err(_) => AbiCallResult::failed(b"host ABI callback panicked".to_vec()),
    }
}

pub(super) unsafe extern "C" fn host_request_v2(
    context: *mut c_void,
    request: *const u8,
    request_len: usize,
) -> AbiCallResult {
    if context.is_null() || (request.is_null() && request_len != 0) {
        return AbiCallResult::failed(b"invalid host callback pointers".to_vec());
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = unsafe { &*context.cast::<HostCallbackContext>() };
        let request = if request_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(request, request_len) }
        };
        dispatch_binary_host_request(context.runtime.as_ref(), context.runtime.as_ref(), request)
    }));
    match result {
        Ok(response) => AbiCallResult::ok(response),
        Err(_) => AbiCallResult::failed(b"host ABI v2 callback panicked".to_vec()),
    }
}

pub(super) unsafe extern "C" fn host_release(buffer: AbiBuffer) {
    if buffer.ptr.is_null() || buffer.len == 0 {
        return;
    }
    let slice = ptr::slice_from_raw_parts_mut(buffer.ptr, buffer.len);
    // SAFETY: host callback responses are allocated as Box<[u8]> by AbiBuffer::from_bytes.
    unsafe { drop(Box::from_raw(slice)) };
}
