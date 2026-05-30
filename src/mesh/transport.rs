use serde::Serialize;

use crate::mesh::envelope::ProtocolEnvelope;
use crate::mesh::identity::{PeerIdentity, TransportEndpoint};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportCapacity {
    pub max_message_bytes: usize,
    pub recommended_chunk_bytes: usize,
    pub max_in_flight: usize,
    pub estimated_throughput_kbps: u32,
}

impl TransportCapacity {
    pub fn local_http() -> Self {
        Self {
            max_message_bytes: 64 * 1024,
            recommended_chunk_bytes: 4 * 1024,
            max_in_flight: 8,
            estimated_throughput_kbps: 1600,
        }
    }

    pub fn unavailable() -> Self {
        Self {
            max_message_bytes: 0,
            recommended_chunk_bytes: 0,
            max_in_flight: 0,
            estimated_throughput_kbps: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransportReceipt {
    pub transport: String,
}

#[derive(Debug, Clone)]
pub struct TransportError {
    pub code: String,
    pub message: String,
    pub retry_after_ms: Option<u64>,
}

impl TransportError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retry_after_ms: None,
        }
    }
}

pub trait MeshTransport: Send + Sync {
    fn kind(&self) -> &'static str;
    fn start(&self) -> Result<(), TransportError>;
    fn stop(&self) -> Result<(), TransportError>;
    fn send(
        &self,
        envelope: &ProtocolEnvelope,
        target_peer: &PeerIdentity,
    ) -> Result<TransportReceipt, TransportError>;
    fn broadcast(
        &self,
        _envelope: &ProtocolEnvelope,
    ) -> Result<Vec<TransportReceipt>, TransportError> {
        Err(TransportError::new(
            "broadcast_unavailable",
            format!("{} does not support broadcast yet", self.kind()),
        ))
    }
    fn status(&self) -> TransportEndpoint;
    fn capacity(&self) -> TransportCapacity;
    fn capabilities(&self) -> Vec<String>;
}
