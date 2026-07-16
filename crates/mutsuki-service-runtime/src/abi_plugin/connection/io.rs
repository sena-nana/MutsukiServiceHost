use std::io::{BufRead, Cursor, Read, Write};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};

use mutsuki_runtime_wire::{DEFAULT_WIRE_LIMITS, WireFlags, decode_binary_frame};

use super::v2::PluginLifetime;

const WORK_QUEUE_LIMIT: usize = 56;
const MANAGEMENT_QUEUE_LIMIT: usize = 8;

pub(super) struct CallbackReader {
    receiver: Receiver<Vec<u8>>,
    current: Cursor<Vec<u8>>,
}

pub(super) struct CallbackWriter {
    work: SyncSender<Vec<u8>>,
    management: SyncSender<Vec<u8>>,
    buffer: Vec<u8>,
}

pub(super) fn callback_io(lifetime: Arc<PluginLifetime>) -> (CallbackReader, CallbackWriter) {
    let (work_tx, work_rx) = mpsc::sync_channel(WORK_QUEUE_LIMIT);
    let (management_tx, management_rx) = mpsc::sync_channel(MANAGEMENT_QUEUE_LIMIT);
    let (response_tx, response_rx) = mpsc::channel();
    spawn_workers(4, work_rx, response_tx.clone(), lifetime.clone());
    spawn_workers(1, management_rx, response_tx, lifetime);
    (
        CallbackReader {
            receiver: response_rx,
            current: Cursor::new(Vec::new()),
        },
        CallbackWriter {
            work: work_tx,
            management: management_tx,
            buffer: Vec::new(),
        },
    )
}

fn spawn_workers(
    count: usize,
    receiver: Receiver<Vec<u8>>,
    response: mpsc::Sender<Vec<u8>>,
    lifetime: Arc<PluginLifetime>,
) {
    let receiver = Arc::new(Mutex::new(receiver));
    for index in 0..count {
        let receiver = receiver.clone();
        let response = response.clone();
        let lifetime = lifetime.clone();
        std::thread::Builder::new()
            .name(format!("mutsuki-abi-v2-callback-{index}"))
            .spawn(move || {
                loop {
                    let frame = {
                        let receiver = receiver.lock().expect("ABI v2 queue poisoned");
                        receiver.recv()
                    };
                    let Ok(frame) = frame else { break };
                    if response.send(invoke(&lifetime, &frame)).is_err() {
                        break;
                    }
                }
            })
            .expect("spawn ABI v2 callback worker");
    }
}

fn invoke(lifetime: &PluginLifetime, frame: &[u8]) -> Vec<u8> {
    let api = lifetime.api;
    let callback = api.request.expect("validated ABI v2 request callback");
    let result = unsafe { callback(api.context, frame.as_ptr(), frame.len()) };
    let valid = (result.payload.len == 0 && result.payload.ptr.is_null())
        || (result.payload.len > 0 && !result.payload.ptr.is_null());
    if !valid {
        return Vec::new();
    }
    let response = unsafe { result.payload.as_slice() }.to_vec();
    if result.payload.len > 0 {
        unsafe { api.release.expect("validated ABI v2 release")(result.payload) };
    }
    if result.status == 0 {
        response
    } else {
        Vec::new()
    }
}

impl CallbackReader {
    fn refill(&mut self) -> std::io::Result<()> {
        if self.current.position() as usize >= self.current.get_ref().len() {
            let frame = self.receiver.recv().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "ABI v2 response closed")
            })?;
            self.current = Cursor::new(frame);
        }
        Ok(())
    }
}

impl Read for CallbackReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.refill()?;
        self.current.read(buffer)
    }
}

impl BufRead for CallbackReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.refill()?;
        self.current.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.current.consume(amount);
    }
}

impl Write for CallbackWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let frame = std::mem::take(&mut self.buffer);
        let decoded = decode_binary_frame(&frame, DEFAULT_WIRE_LIMITS)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let sender = if decoded.header.flags.contains(WireFlags::MANAGEMENT) {
            &self.management
        } else {
            &self.work
        };
        sender
            .send(frame)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "ABI v2 queue closed"))
    }
}
