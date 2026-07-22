use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mutsuki_service_config::IpcCodec;
use mutsuki_service_control::{ControlMethod, ControlRequest, ControlResponse};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

use crate::codec::{
    decode_jsonl_response, encode_binary_cancel, encode_binary_request_with_scratch,
    encode_jsonl_request,
};
use crate::error::{IpcError, IpcResult};
use crate::frame::{BINARY_HEADER_LEN, BINARY_LENGTH_PREFIX_LEN, FrameFlags};
use crate::io::{read_binary_frame, read_jsonl_line, write_all_flush};
use crate::limits::{ControlIpcLimits, ControlIpcProfile};
use crate::transport::{ControlStream, connect_transport};
use mutsuki_service_config::IpcTransport;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlClientConfig {
    pub transport: IpcTransport,
    pub endpoint: String,
    pub token: String,
    pub codec: IpcCodec,
    pub limits: ControlIpcLimits,
}

impl ControlClientConfig {
    pub fn new(
        transport: IpcTransport,
        endpoint: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            transport,
            endpoint: endpoint.into(),
            token: token.into(),
            codec: IpcCodec::Binary,
            limits: ControlIpcLimits::default(),
        }
    }

    pub fn with_profile(mut self, profile: ControlIpcProfile) -> Self {
        self.codec = profile.codec;
        self.limits = profile.limits;
        self
    }
}

impl From<&mutsuki_service_config::ServiceConfig> for ControlClientConfig {
    fn from(config: &mutsuki_service_config::ServiceConfig) -> Self {
        Self::new(
            config.ipc.transport.clone(),
            config.ipc_endpoint(),
            config.control_token(),
        )
        .with_profile(ControlIpcProfile::from_config(config))
    }
}

struct PendingResponse {
    tx: oneshot::Sender<IpcResult<ControlResponse>>,
}

/// Persistent control session with multiplexed in-flight requests.
pub struct ControlSession {
    config: ControlClientConfig,
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
    pending: Arc<Mutex<HashMap<u64, PendingResponse>>>,
    next_id: AtomicU64,
    reader_task: JoinHandle<()>,
    closed: Arc<std::sync::atomic::AtomicBool>,
    encode_buf: Mutex<Vec<u8>>,
    payload_buf: Mutex<Vec<u8>>,
    connections: Arc<AtomicU64>,
}

impl ControlSession {
    pub async fn connect(config: ControlClientConfig) -> IpcResult<Self> {
        let stream = connect_transport(config.transport.clone(), &config.endpoint).await?;
        Self::from_stream(config, stream, Arc::new(AtomicU64::new(1))).await
    }

    async fn from_stream(
        config: ControlClientConfig,
        stream: ControlStream,
        connections: Arc<AtomicU64>,
    ) -> IpcResult<Self> {
        let (reader, writer) = tokio::io::split(stream);
        let writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>> =
            Arc::new(Mutex::new(Box::new(writer)));
        let pending: Arc<Mutex<HashMap<u64, PendingResponse>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_task = spawn_reader(
            reader,
            config.codec,
            config.limits,
            pending.clone(),
            closed.clone(),
        );
        Ok(Self {
            config,
            writer,
            pending,
            next_id: AtomicU64::new(1),
            reader_task,
            closed,
            encode_buf: Mutex::new(Vec::new()),
            payload_buf: Mutex::new(Vec::new()),
            connections,
        })
    }

    pub fn connection_count(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }

    pub async fn request(
        &self,
        method: ControlMethod,
        params: serde_json::Value,
    ) -> IpcResult<ControlResponse> {
        self.send(ControlRequest {
            token: self.config.token.clone(),
            method,
            params,
        })
        .await
    }

    pub async fn send(&self, request: ControlRequest) -> IpcResult<ControlResponse> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(IpcError::Closed);
        }
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if request_id == 0 {
            return Err(IpcError::InvalidRequestId);
        }
        {
            let pending = self.pending.lock().await;
            if pending.len() >= self.config.limits.max_in_flight {
                return Err(IpcError::PendingLimitExceeded(
                    self.config.limits.max_in_flight,
                ));
            }
        }
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .await
            .insert(request_id, PendingResponse { tx });

        let mut encode_buf = self.encode_buf.lock().await;
        let mut payload_buf = self.payload_buf.lock().await;
        let write_result = match self.config.codec {
            IpcCodec::Binary => {
                encode_binary_request_with_scratch(
                    request_id,
                    &request,
                    self.config.limits,
                    &mut encode_buf,
                    &mut payload_buf,
                )?;
                let mut writer = self.writer.lock().await;
                write_all_flush(&mut *writer, &encode_buf).await
            }
            IpcCodec::Jsonl => {
                encode_jsonl_request(&request, self.config.limits, &mut encode_buf)?;
                let mut writer = self.writer.lock().await;
                write_all_flush(&mut *writer, &encode_buf).await
            }
        };
        drop(payload_buf);
        drop(encode_buf);
        if let Err(error) = write_result {
            self.pending.lock().await.remove(&request_id);
            return Err(error);
        }

        let timeout = Duration::from_millis(self.config.limits.request_timeout_ms.max(1));
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(IpcError::Closed),
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                let _ = self.cancel(request_id).await;
                Err(IpcError::Timeout)
            }
        }
    }

    pub async fn cancel(&self, request_id: u64) -> IpcResult<()> {
        if self.config.codec != IpcCodec::Binary {
            if let Some(pending) = self.pending.lock().await.remove(&request_id) {
                let _ = pending.tx.send(Err(IpcError::Cancelled));
            }
            return Ok(());
        }
        let bytes = encode_binary_cancel(request_id, self.config.limits)?;
        let mut writer = self.writer.lock().await;
        write_all_flush(&mut *writer, &bytes).await?;
        if let Some(pending) = self.pending.lock().await.remove(&request_id) {
            let _ = pending.tx.send(Err(IpcError::Cancelled));
        }
        Ok(())
    }

    pub async fn close(self) -> IpcResult<()> {
        self.closed.store(true, Ordering::Relaxed);
        {
            let mut pending = self.pending.lock().await;
            for (_, entry) in pending.drain() {
                let _ = entry.tx.send(Err(IpcError::Closed));
            }
        }
        self.reader_task.abort();
        let _ = self.reader_task.await;
        let mut writer = self.writer.lock().await;
        let _ = tokio::io::AsyncWriteExt::shutdown(&mut *writer).await;
        Ok(())
    }
}

fn spawn_reader<R: AsyncRead + Send + Unpin + 'static>(
    mut reader: R,
    codec: IpcCodec,
    limits: ControlIpcLimits,
    pending: Arc<Mutex<HashMap<u64, PendingResponse>>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let result = match codec {
            IpcCodec::Binary => read_binary_loop(&mut reader, limits, &pending).await,
            IpcCodec::Jsonl => read_jsonl_loop(&mut reader, limits, &pending).await,
        };
        closed.store(true, Ordering::Relaxed);
        let mut pending = pending.lock().await;
        for (_, entry) in pending.drain() {
            let err = match &result {
                Ok(()) => IpcError::Closed,
                Err(error) => IpcError::Protocol(error.to_string()),
            };
            let _ = entry.tx.send(Err(err));
        }
    })
}

async fn read_binary_loop<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ControlIpcLimits,
    pending: &Arc<Mutex<HashMap<u64, PendingResponse>>>,
) -> IpcResult<()> {
    let mut header_buf = [0_u8; BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN];
    let mut payload_buf = Vec::new();
    loop {
        let Some(header) =
            crate::io::read_binary_frame_into(reader, limits, &mut header_buf, &mut payload_buf)
                .await?
        else {
            return Ok(());
        };
        if !header.flags.contains(FrameFlags::RESPONSE) {
            return Err(IpcError::UnknownFlags(header.flags.bits()));
        }
        let request_id = header.request_id;
        let response: ControlResponse = {
            let mut deserializer = rmp_serde::Deserializer::new(payload_buf.as_slice());
            deserializer.set_max_depth(limits.max_msgpack_nesting_depth);
            serde::Deserialize::deserialize(&mut deserializer)?
        };
        let mut map = pending.lock().await;
        match map.remove(&request_id) {
            Some(entry) => {
                let _ = entry.tx.send(Ok(response));
            }
            None => {
                return Err(IpcError::LateResponse(request_id));
            }
        }
    }
}

async fn read_jsonl_loop<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ControlIpcLimits,
    pending: &Arc<Mutex<HashMap<u64, PendingResponse>>>,
) -> IpcResult<()> {
    let mut line_buf = Vec::new();
    // JSONL has no request id; pair responses FIFO with the oldest pending request.
    loop {
        let Some(line) = read_jsonl_line(reader, limits, &mut line_buf).await? else {
            return Ok(());
        };
        let response = decode_jsonl_response(&line, limits)?;
        let mut map = pending.lock().await;
        let Some(request_id) = map.keys().copied().min() else {
            return Err(IpcError::LateResponse(0));
        };
        if let Some(entry) = map.remove(&request_id) {
            let _ = entry.tx.send(Ok(response));
        } else {
            return Err(IpcError::LateResponse(request_id));
        }
    }
}

/// One-shot compatibility helper for benchmarks and migration callers.
pub async fn request_oneshot(
    config: &ControlClientConfig,
    request: ControlRequest,
) -> IpcResult<ControlResponse> {
    let stream = connect_transport(config.transport.clone(), &config.endpoint).await?;
    match config.codec {
        IpcCodec::Jsonl => oneshot_jsonl(stream, config.limits, request).await,
        IpcCodec::Binary => oneshot_binary(stream, config.limits, request).await,
    }
}

async fn oneshot_jsonl(
    stream: ControlStream,
    limits: ControlIpcLimits,
    request: ControlRequest,
) -> IpcResult<ControlResponse> {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut encode_buf = Vec::new();
    encode_jsonl_request(&request, limits, &mut encode_buf)?;
    write_all_flush(&mut writer, &encode_buf).await?;
    let mut line_buf = Vec::new();
    let line = read_jsonl_line(&mut reader, limits, &mut line_buf)
        .await?
        .ok_or(IpcError::Closed)?;
    decode_jsonl_response(&line, limits)
}

async fn oneshot_binary(
    stream: ControlStream,
    limits: ControlIpcLimits,
    request: ControlRequest,
) -> IpcResult<ControlResponse> {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut frame_buf = Vec::new();
    let mut payload_buf = Vec::new();
    encode_binary_request_with_scratch(1, &request, limits, &mut frame_buf, &mut payload_buf)?;
    write_all_flush(&mut writer, &frame_buf).await?;
    let mut header_buf = [0_u8; BINARY_LENGTH_PREFIX_LEN + BINARY_HEADER_LEN];
    let frame = read_binary_frame(&mut reader, limits, &mut header_buf)
        .await?
        .ok_or(IpcError::Closed)?;
    if frame.header.request_id != 1 || !frame.header.flags.contains(FrameFlags::RESPONSE) {
        return Err(IpcError::Protocol(
            "oneshot binary response mismatch".into(),
        ));
    }
    let mut deserializer = rmp_serde::Deserializer::new(frame.payload.as_slice());
    deserializer.set_max_depth(limits.max_msgpack_nesting_depth);
    Ok(serde::Deserialize::deserialize(&mut deserializer)?)
}
