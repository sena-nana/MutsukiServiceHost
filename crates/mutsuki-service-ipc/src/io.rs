use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{IpcError, IpcResult};
use crate::frame::{
    BINARY_HEADER_LEN, BINARY_LENGTH_PREFIX_LEN, BinaryFrame, FrameHeader, decode_frame_header,
    discard_bytes, validate_frame_length,
};
use crate::limits::ControlIpcLimits;

/// Read one length-prefixed binary frame into reusable buffers.
pub async fn read_binary_frame_into<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ControlIpcLimits,
    header_buf: &mut [u8; BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
    payload_buf: &mut Vec<u8>,
) -> IpcResult<Option<FrameHeader>> {
    match reader
        .read_exact(&mut header_buf[..BINARY_LENGTH_PREFIX_LEN])
        .await
    {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let declared = u32::from_be_bytes(
        header_buf[..BINARY_LENGTH_PREFIX_LEN]
            .try_into()
            .expect("prefix"),
    ) as usize;
    validate_frame_length(declared, limits)?;
    reader
        .read_exact(
            &mut header_buf[BINARY_LENGTH_PREFIX_LEN..BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
        )
        .await?;
    let header = decode_frame_header(
        &header_buf[BINARY_LENGTH_PREFIX_LEN..BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
        limits,
    )?;
    let payload_len = header.payload_len as usize;
    let expected_body = BINARY_HEADER_LEN + payload_len;
    if declared != expected_body {
        return Err(IpcError::Truncated {
            expected: declared,
            actual: expected_body,
        });
    }
    payload_buf.clear();
    payload_buf.resize(payload_len, 0);
    if payload_len > 0 {
        reader.read_exact(payload_buf).await?;
    }
    Ok(Some(header))
}

pub async fn read_binary_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ControlIpcLimits,
    header_buf: &mut [u8; BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
) -> IpcResult<Option<BinaryFrame>> {
    let mut payload = Vec::new();
    let Some(header) = read_binary_frame_into(reader, limits, header_buf, &mut payload).await?
    else {
        return Ok(None);
    };
    Ok(Some(BinaryFrame { header, payload }))
}

pub async fn read_frame_prefix<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ControlIpcLimits,
    header_buf: &mut [u8; BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
) -> IpcResult<Option<(usize, FrameHeader)>> {
    match reader
        .read_exact(&mut header_buf[..BINARY_LENGTH_PREFIX_LEN])
        .await
    {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let declared = u32::from_be_bytes(
        header_buf[..BINARY_LENGTH_PREFIX_LEN]
            .try_into()
            .expect("prefix"),
    ) as usize;
    validate_frame_length(declared, limits)?;
    reader
        .read_exact(
            &mut header_buf[BINARY_LENGTH_PREFIX_LEN..BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
        )
        .await?;
    let header = decode_frame_header(
        &header_buf[BINARY_LENGTH_PREFIX_LEN..BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN],
        limits,
    )?;
    Ok(Some((declared, header)))
}

pub async fn read_payload_or_discard<R: AsyncRead + Unpin>(
    reader: &mut R,
    declared_body: usize,
    header: &FrameHeader,
    allocate: bool,
) -> IpcResult<Option<Vec<u8>>> {
    let payload_len = header.payload_len as usize;
    let expected_body = BINARY_HEADER_LEN + payload_len;
    if declared_body != expected_body {
        return Err(IpcError::Truncated {
            expected: declared_body,
            actual: expected_body,
        });
    }
    if !allocate {
        discard_bytes(reader, payload_len).await?;
        return Ok(None);
    }
    let mut payload = vec![0_u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }
    Ok(Some(payload))
}

pub async fn write_all_flush<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8]) -> IpcResult<()> {
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_jsonl_line<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ControlIpcLimits,
    line_buf: &mut Vec<u8>,
) -> IpcResult<Option<String>> {
    line_buf.clear();
    let mut byte = [0_u8; 1];
    loop {
        match reader.read_exact(&mut byte).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                if line_buf.is_empty() {
                    return Ok(None);
                }
                return Err(IpcError::Protocol(
                    "truncated jsonl line without trailing newline".into(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        if byte[0] == b'\n' {
            break;
        }
        if line_buf.len() + 1 > limits.max_jsonl_line_bytes {
            // Drain remainder until newline/EOF without growing the buffer.
            loop {
                match reader.read_exact(&mut byte).await {
                    Ok(_) if byte[0] == b'\n' => break,
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => return Err(error.into()),
                }
            }
            return Err(IpcError::JsonlLineOversized {
                actual: limits.max_jsonl_line_bytes + 1,
                limit: limits.max_jsonl_line_bytes,
            });
        }
        line_buf.push(byte[0]);
    }
    if !line_buf.is_empty() && line_buf.last() == Some(&b'\r') {
        line_buf.pop();
    }
    Ok(Some(
        String::from_utf8(line_buf.clone())
            .map_err(|error| IpcError::Protocol(error.to_string()))?,
    ))
}
