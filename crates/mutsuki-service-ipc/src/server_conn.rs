use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mutsuki_service_control::{
    ControlError, ControlHandler, ControlMethod, ControlRequest, ControlResponse,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore, watch};
use tokio::task::JoinHandle;

use crate::codec::{
    ControlRequestBody, decode_jsonl_request, encode_binary_response_with_scratch,
    encode_jsonl_response,
};
use crate::error::{IpcError, IpcResult};
use crate::frame::{FrameFlags, OPCODE_CANCEL};
use crate::io::{read_frame_prefix, read_jsonl_line, read_payload_or_discard, write_all_flush};
use crate::limits::ControlIpcProfile;

struct PendingEntry {
    abort: tokio::sync::watch::Sender<bool>,
    task: JoinHandle<()>,
}

pub async fn serve_stream<S>(
    stream: S,
    handler: Arc<dyn ControlHandler>,
    profile: ControlIpcProfile,
    drain_rx: watch::Receiver<bool>,
) -> IpcResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(writer));
    let mut reader = reader;
    let mut peek = [0_u8; 1];
    match reader.read_exact(&mut peek).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    match peek[0] {
        b'{' => {
            serve_jsonl(
                FirstByteReader {
                    first: Some(peek[0]),
                    inner: reader,
                },
                writer,
                handler,
                profile,
                drain_rx,
            )
            .await
        }
        _ => {
            serve_binary(
                FirstByteReader {
                    first: Some(peek[0]),
                    inner: reader,
                },
                writer,
                handler,
                profile,
                drain_rx,
            )
            .await
        }
    }
}

struct FirstByteReader<R> {
    first: Option<u8>,
    inner: R,
}

impl<R: AsyncRead + Unpin> AsyncRead for FirstByteReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(byte) = self.first.take() {
            if buf.remaining() > 0 {
                buf.put_slice(&[byte]);
                return std::task::Poll::Ready(Ok(()));
            }
            self.first = Some(byte);
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

async fn serve_jsonl<R, W>(
    mut reader: R,
    writer: Arc<Mutex<W>>,
    handler: Arc<dyn ControlHandler>,
    profile: ControlIpcProfile,
    mut drain_rx: watch::Receiver<bool>,
) -> IpcResult<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let limits = profile.limits;
    let idle = Duration::from_millis(limits.idle_timeout_ms.max(1));
    let mutate_lock = Arc::new(Mutex::new(()));
    let mut line_buf = Vec::new();
    let mut encode_buf = Vec::new();

    loop {
        if *drain_rx.borrow() {
            return Ok(());
        }
        let line = tokio::select! {
            biased;
            changed = drain_rx.changed() => {
                if changed.is_err() || *drain_rx.borrow() {
                    return Ok(());
                }
                continue;
            }
            line = tokio::time::timeout(idle, read_jsonl_line(&mut reader, limits, &mut line_buf)) => {
                match line {
                    Ok(Ok(Some(line))) => line,
                    Ok(Ok(None)) => return Ok(()),
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Ok(()),
                }
            }
        };

        if *drain_rx.borrow() {
            return Err(IpcError::Draining);
        }

        let response = match decode_jsonl_request(&line, limits) {
            Ok(request) => {
                let (_abort_tx, abort_rx) = watch::channel(false);
                dispatch_request(handler.clone(), request, mutate_lock.clone(), abort_rx).await
            }
            Err(error) => ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        encode_jsonl_response(&response, limits, &mut encode_buf)?;
        let mut guard = writer.lock().await;
        write_all_flush(&mut *guard, &encode_buf).await?;
    }
}

async fn serve_binary<R, W>(
    mut reader: R,
    writer: Arc<Mutex<W>>,
    handler: Arc<dyn ControlHandler>,
    profile: ControlIpcProfile,
    mut drain_rx: watch::Receiver<bool>,
) -> IpcResult<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let limits = profile.limits;
    let idle = Duration::from_millis(limits.idle_timeout_ms.max(1));
    let mutate_lock = Arc::new(Mutex::new(()));
    let pending = Arc::new(Mutex::new(HashMap::<u64, PendingEntry>::new()));
    let pending_slots = Arc::new(Semaphore::new(limits.max_in_flight));
    let mut header_buf =
        [0_u8; crate::frame::BINARY_LENGTH_PREFIX_LEN + crate::frame::BINARY_HEADER_LEN];
    let response_encode = Arc::new(Mutex::new((Vec::new(), Vec::new())));

    loop {
        if *drain_rx.borrow() {
            wait_pending_drain(&pending).await;
            return Ok(());
        }
        let prefix = tokio::select! {
            biased;
            changed = drain_rx.changed() => {
                if changed.is_err() || *drain_rx.borrow() {
                    wait_pending_drain(&pending).await;
                    return Ok(());
                }
                continue;
            }
            frame = tokio::time::timeout(idle, read_frame_prefix(&mut reader, limits, &mut header_buf)) => {
                match frame {
                    Ok(Ok(Some(prefix))) => prefix,
                    Ok(Ok(None)) => {
                        wait_pending_drain(&pending).await;
                        return Ok(());
                    }
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        wait_pending_drain(&pending).await;
                        return Ok(());
                    }
                }
            }
        };

        let (declared, header) = prefix;
        if header.flags.contains(FrameFlags::CANCEL) || header.opcode == OPCODE_CANCEL {
            let _ = read_payload_or_discard(&mut reader, declared, &header, false).await?;
            cancel_pending(&pending, header.request_id).await;
            continue;
        }
        if !header.flags.contains(FrameFlags::REQUEST) {
            let _ = read_payload_or_discard(&mut reader, declared, &header, false).await?;
            return Err(IpcError::UnknownFlags(header.flags.bits()));
        }
        if *drain_rx.borrow() {
            let _ = read_payload_or_discard(&mut reader, declared, &header, false).await?;
            return Err(IpcError::Draining);
        }

        let permit = match pending_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let _ = read_payload_or_discard(&mut reader, declared, &header, false).await?;
                let method =
                    ControlMethod::from_opcode(header.opcode).unwrap_or(ControlMethod::HealthCheck);
                let response = ControlResponse::err(ControlError::Failed(format!(
                    "pending request limit exceeded ({})",
                    limits.max_in_flight
                )));
                let mut encode_buf = Vec::new();
                let mut payload_buf = Vec::new();
                encode_binary_response_with_scratch(
                    header.request_id,
                    method,
                    &response,
                    limits,
                    &mut encode_buf,
                    &mut payload_buf,
                )?;
                let mut guard = writer.lock().await;
                write_all_flush(&mut *guard, &encode_buf).await?;
                continue;
            }
        };

        let Some(payload) = read_payload_or_discard(&mut reader, declared, &header, true).await?
        else {
            drop(permit);
            continue;
        };
        let method = match ControlMethod::from_opcode(header.opcode) {
            Some(method) => method,
            None => {
                drop(permit);
                return Err(IpcError::UnknownOpcode(header.opcode));
            }
        };
        let body: ControlRequestBody = {
            let mut deserializer = rmp_serde::Deserializer::new(payload.as_slice());
            deserializer.set_max_depth(limits.max_msgpack_nesting_depth);
            match serde::Deserialize::deserialize(&mut deserializer) {
                Ok(body) => body,
                Err(error) => {
                    drop(permit);
                    let response =
                        ControlResponse::err(ControlError::BadRequest(error.to_string()));
                    let mut encode_buf = Vec::new();
                    let mut payload_buf = Vec::new();
                    encode_binary_response_with_scratch(
                        header.request_id,
                        method,
                        &response,
                        limits,
                        &mut encode_buf,
                        &mut payload_buf,
                    )?;
                    let mut guard = writer.lock().await;
                    write_all_flush(&mut *guard, &encode_buf).await?;
                    continue;
                }
            }
        };
        let request = ControlRequest {
            token: body.token,
            method,
            params: body.params,
        };
        let request_id = header.request_id;
        let (abort_tx, abort_rx) = watch::channel(false);
        let handler = handler.clone();
        let writer = writer.clone();
        let mutate_lock = mutate_lock.clone();
        let pending_map = pending.clone();
        let response_encode = response_encode.clone();
        let task = tokio::spawn(async move {
            let _permit = permit;
            let response = dispatch_request(handler, request, mutate_lock, abort_rx).await;
            {
                let mut buffers = response_encode.lock().await;
                let (encode_buf, payload_buf) = &mut *buffers;
                if encode_binary_response_with_scratch(
                    request_id,
                    method,
                    &response,
                    limits,
                    encode_buf,
                    payload_buf,
                )
                .is_ok()
                {
                    let mut guard = writer.lock().await;
                    let _ = write_all_flush(&mut *guard, encode_buf).await;
                }
            }
            let mut map = pending_map.lock().await;
            map.remove(&request_id);
        });
        pending.lock().await.insert(
            request_id,
            PendingEntry {
                abort: abort_tx,
                task,
            },
        );
    }
}

async fn dispatch_request(
    handler: Arc<dyn ControlHandler>,
    request: ControlRequest,
    mutate_lock: Arc<Mutex<()>>,
    mut abort_rx: watch::Receiver<bool>,
) -> ControlResponse {
    let _guard: Option<OwnedMutexGuard<()>> = if request.method.is_mutating() {
        Some(mutate_lock.lock_owned().await)
    } else {
        None
    };
    let work = handler.handle(request);
    tokio::select! {
        response = work => response,
        _ = abort_rx.changed() => {
            ControlResponse::err(ControlError::Failed("request cancelled".into()))
        }
    }
}

async fn cancel_pending(pending: &Arc<Mutex<HashMap<u64, PendingEntry>>>, request_id: u64) {
    let mut map = pending.lock().await;
    if let Some(entry) = map.remove(&request_id) {
        let _ = entry.abort.send(true);
        entry.task.abort();
    }
}

async fn wait_pending_drain(pending: &Arc<Mutex<HashMap<u64, PendingEntry>>>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let empty = {
            let map = pending.lock().await;
            map.is_empty()
        };
        if empty || tokio::time::Instant::now() >= deadline {
            let mut map = pending.lock().await;
            for (_, entry) in map.drain() {
                let _ = entry.abort.send(true);
                entry.task.abort();
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
