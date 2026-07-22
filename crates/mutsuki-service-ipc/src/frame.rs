use crate::error::{IpcError, IpcResult};
use crate::limits::ControlIpcLimits;

pub const CONTROL_WIRE_MAGIC: u32 = 0x4d_53_48_43; // "MSHC"
pub const CONTROL_WIRE_MAJOR: u16 = 1;
pub const CONTROL_WIRE_MINOR: u16 = 0;
pub const BINARY_LENGTH_PREFIX_LEN: usize = 4;
pub const BINARY_HEADER_LEN: usize = 24;
pub const OPCODE_CANCEL: u16 = 0x7FFF;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameFlags(u16);

impl FrameFlags {
    pub const REQUEST: Self = Self(0x0001);
    pub const RESPONSE: Self = Self(0x0002);
    pub const ERROR: Self = Self(0x0004);
    pub const CANCEL: Self = Self(0x0008);
    const KNOWN: u16 = Self::REQUEST.0 | Self::RESPONSE.0 | Self::ERROR.0 | Self::CANCEL.0;

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn from_bits(bits: u16) -> IpcResult<Self> {
        if bits & !Self::KNOWN != 0 {
            return Err(IpcError::UnknownFlags(bits));
        }
        let flags = Self(bits);
        let is_request = flags.contains(Self::REQUEST);
        let is_response = flags.contains(Self::RESPONSE);
        let is_cancel = flags.contains(Self::CANCEL);
        if is_cancel {
            if is_response || flags.contains(Self::ERROR) {
                return Err(IpcError::UnknownFlags(bits));
            }
            return Ok(flags);
        }
        if is_request == is_response {
            return Err(IpcError::UnknownFlags(bits));
        }
        if flags.contains(Self::ERROR) && !is_response {
            return Err(IpcError::UnknownFlags(bits));
        }
        Ok(flags)
    }
}

impl std::ops::BitOr for FrameFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub opcode: u16,
    pub flags: FrameFlags,
    pub request_id: u64,
    pub payload_len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryFrame {
    pub header: FrameHeader,
    pub payload: Vec<u8>,
}

pub fn encode_frame(
    opcode: u16,
    flags: FrameFlags,
    request_id: u64,
    payload: Vec<u8>,
    limits: ControlIpcLimits,
) -> IpcResult<Vec<u8>> {
    let mut encoded = Vec::new();
    encode_frame_into(&mut encoded, opcode, flags, request_id, &payload, limits)?;
    Ok(encoded)
}

pub fn encode_frame_into(
    out: &mut Vec<u8>,
    opcode: u16,
    flags: FrameFlags,
    request_id: u64,
    payload: &[u8],
    limits: ControlIpcLimits,
) -> IpcResult<()> {
    if request_id == 0 {
        return Err(IpcError::InvalidRequestId);
    }
    if payload.len() > limits.max_payload_bytes {
        return Err(IpcError::PayloadOversized {
            actual: payload.len(),
            limit: limits.max_payload_bytes,
        });
    }
    let body_len = BINARY_HEADER_LEN + payload.len();
    validate_frame_length(body_len, limits)?;
    out.clear();
    out.reserve(BINARY_LENGTH_PREFIX_LEN + body_len);
    out.extend_from_slice(&(body_len as u32).to_be_bytes());
    out.extend_from_slice(&CONTROL_WIRE_MAGIC.to_be_bytes());
    out.extend_from_slice(&CONTROL_WIRE_MAJOR.to_be_bytes());
    out.extend_from_slice(&CONTROL_WIRE_MINOR.to_be_bytes());
    out.extend_from_slice(&opcode.to_be_bytes());
    out.extend_from_slice(&flags.bits().to_be_bytes());
    out.extend_from_slice(&request_id.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

pub fn decode_frame_header(body: &[u8], limits: ControlIpcLimits) -> IpcResult<FrameHeader> {
    if body.len() < BINARY_HEADER_LEN {
        return Err(IpcError::Truncated {
            expected: BINARY_HEADER_LEN,
            actual: body.len(),
        });
    }
    let magic = u32::from_be_bytes(body[0..4].try_into().expect("magic"));
    if magic != CONTROL_WIRE_MAGIC {
        return Err(IpcError::InvalidMagic(magic));
    }
    let major = u16::from_be_bytes(body[4..6].try_into().expect("major"));
    if major != CONTROL_WIRE_MAJOR {
        return Err(IpcError::Protocol(format!(
            "unsupported control wire major {major}"
        )));
    }
    let opcode = u16::from_be_bytes(body[8..10].try_into().expect("opcode"));
    let flags = FrameFlags::from_bits(u16::from_be_bytes(body[10..12].try_into().expect("flags")))?;
    let request_id = u64::from_be_bytes(body[12..20].try_into().expect("request id"));
    if request_id == 0 {
        return Err(IpcError::InvalidRequestId);
    }
    let payload_len = u32::from_be_bytes(body[20..24].try_into().expect("payload len"));
    if payload_len as usize > limits.max_payload_bytes {
        return Err(IpcError::PayloadOversized {
            actual: payload_len as usize,
            limit: limits.max_payload_bytes,
        });
    }
    Ok(FrameHeader {
        opcode,
        flags,
        request_id,
        payload_len,
    })
}

#[cfg(test)]
pub fn decode_binary_frame(bytes: &[u8], limits: ControlIpcLimits) -> IpcResult<BinaryFrame> {
    if bytes.len() < BINARY_LENGTH_PREFIX_LEN {
        return Err(IpcError::Truncated {
            expected: BINARY_LENGTH_PREFIX_LEN,
            actual: bytes.len(),
        });
    }
    let declared = u32::from_be_bytes(bytes[..4].try_into().expect("prefix")) as usize;
    validate_frame_length(declared, limits)?;
    let actual = bytes.len() - BINARY_LENGTH_PREFIX_LEN;
    if declared != actual {
        return Err(IpcError::Truncated {
            expected: declared,
            actual,
        });
    }
    let body = &bytes[BINARY_LENGTH_PREFIX_LEN..];
    let header = decode_frame_header(body, limits)?;
    let payload = &body[BINARY_HEADER_LEN..];
    if payload.len() != header.payload_len as usize {
        return Err(IpcError::Truncated {
            expected: header.payload_len as usize,
            actual: payload.len(),
        });
    }
    Ok(BinaryFrame {
        header,
        payload: payload.to_vec(),
    })
}

pub fn validate_frame_length(body_len: usize, limits: ControlIpcLimits) -> IpcResult<()> {
    if body_len == 0 || body_len > limits.max_frame_bytes {
        return Err(IpcError::FrameOversized {
            actual: body_len,
            limit: limits.max_frame_bytes,
        });
    }
    if body_len < BINARY_HEADER_LEN {
        return Err(IpcError::Truncated {
            expected: BINARY_HEADER_LEN,
            actual: body_len,
        });
    }
    Ok(())
}

pub async fn discard_bytes<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    mut len: usize,
) -> IpcResult<()> {
    use tokio::io::AsyncReadExt;
    let mut scratch = [0_u8; 8 * 1024];
    while len > 0 {
        let chunk = len.min(scratch.len());
        reader.read_exact(&mut scratch[..chunk]).await?;
        len -= chunk;
    }
    Ok(())
}
