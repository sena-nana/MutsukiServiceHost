use std::time::Duration;

use mutsuki_link_core::{Connection, TransportErrorKind};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub(crate) async fn send_json(
    connection: &mut impl Connection,
    value: &impl Serialize,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match connection.try_send(&bytes) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind == TransportErrorKind::WouldBlock => {
                if tokio::time::Instant::now() >= deadline {
                    return Err("send timed out".into());
                }
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

pub(crate) async fn recv_json<T: DeserializeOwned>(
    connection: &mut impl Connection,
) -> Result<T, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match connection.try_receive() {
            Ok(Some(bytes)) => {
                return serde_json::from_slice(&bytes).map_err(|error| error.to_string());
            }
            Ok(None) => return Err("connection closed before frame".into()),
            Err(error) if error.kind == TransportErrorKind::WouldBlock => {
                if tokio::time::Instant::now() >= deadline {
                    return Err("receive timed out".into());
                }
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}
