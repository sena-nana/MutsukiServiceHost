use mutsuki_service_control::{ControlMethod, ControlRequest, ControlResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{IpcError, IpcResult};
use crate::frame::{FrameFlags, OPCODE_CANCEL, encode_frame, encode_frame_into};
use crate::limits::ControlIpcLimits;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ControlRequestBody {
    pub token: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Serialize)]
struct ControlRequestBodyRef<'a> {
    token: &'a str,
    params: &'a Value,
}

pub fn encode_binary_request_with_scratch(
    request_id: u64,
    request: &ControlRequest,
    limits: ControlIpcLimits,
    frame_buf: &mut Vec<u8>,
    payload_buf: &mut Vec<u8>,
) -> IpcResult<()> {
    let body = ControlRequestBodyRef {
        token: &request.token,
        params: &request.params,
    };
    payload_buf.clear();
    encode_messagepack_into(&body, payload_buf, limits)?;
    encode_frame_into(
        frame_buf,
        request.method.opcode(),
        FrameFlags::REQUEST,
        request_id,
        payload_buf,
        limits,
    )
}

pub fn encode_binary_response_with_scratch(
    request_id: u64,
    method: ControlMethod,
    response: &ControlResponse,
    limits: ControlIpcLimits,
    frame_buf: &mut Vec<u8>,
    payload_buf: &mut Vec<u8>,
) -> IpcResult<()> {
    payload_buf.clear();
    encode_messagepack_into(response, payload_buf, limits)?;
    let flags = if response.ok {
        FrameFlags::RESPONSE
    } else {
        FrameFlags::RESPONSE | FrameFlags::ERROR
    };
    encode_frame_into(
        frame_buf,
        method.opcode(),
        flags,
        request_id,
        payload_buf,
        limits,
    )
}

pub fn encode_binary_cancel(request_id: u64, limits: ControlIpcLimits) -> IpcResult<Vec<u8>> {
    encode_frame(
        OPCODE_CANCEL,
        FrameFlags::CANCEL,
        request_id,
        Vec::new(),
        limits,
    )
}

pub fn encode_jsonl_request(
    request: &ControlRequest,
    limits: ControlIpcLimits,
    encode_buf: &mut Vec<u8>,
) -> IpcResult<()> {
    encode_jsonl(request, limits, encode_buf)
}

pub fn encode_jsonl_response(
    response: &ControlResponse,
    limits: ControlIpcLimits,
    encode_buf: &mut Vec<u8>,
) -> IpcResult<()> {
    encode_jsonl(response, limits, encode_buf)
}

pub fn decode_jsonl_request(line: &str, limits: ControlIpcLimits) -> IpcResult<ControlRequest> {
    decode_jsonl(line, limits)
}

pub fn decode_jsonl_response(line: &str, limits: ControlIpcLimits) -> IpcResult<ControlResponse> {
    decode_jsonl(line, limits)
}

fn encode_jsonl<T: Serialize>(
    value: &T,
    limits: ControlIpcLimits,
    encode_buf: &mut Vec<u8>,
) -> IpcResult<()> {
    encode_buf.clear();
    serde_json::to_writer(&mut *encode_buf, value)?;
    encode_buf.push(b'\n');
    if encode_buf.len() > limits.max_jsonl_line_bytes {
        return Err(IpcError::JsonlLineOversized {
            actual: encode_buf.len(),
            limit: limits.max_jsonl_line_bytes,
        });
    }
    Ok(())
}

fn decode_jsonl<T: for<'de> Deserialize<'de>>(
    line: &str,
    limits: ControlIpcLimits,
) -> IpcResult<T> {
    if line.len() > limits.max_jsonl_line_bytes {
        return Err(IpcError::JsonlLineOversized {
            actual: line.len(),
            limit: limits.max_jsonl_line_bytes,
        });
    }
    Ok(serde_json::from_str(line)?)
}

fn encode_messagepack_into<T: Serialize>(
    value: &T,
    buf: &mut Vec<u8>,
    limits: ControlIpcLimits,
) -> IpcResult<()> {
    let mut serializer = rmp_serde::Serializer::new(&mut *buf).with_struct_map();
    serializer.unstable_set_max_depth(limits.max_msgpack_nesting_depth);
    value.serialize(&mut serializer)?;
    if buf.len() > limits.max_payload_bytes {
        return Err(IpcError::PayloadOversized {
            actual: buf.len(),
            limit: limits.max_payload_bytes,
        });
    }
    Ok(())
}
