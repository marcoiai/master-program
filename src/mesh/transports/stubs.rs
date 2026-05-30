use crate::mesh::envelope::ProtocolEnvelope;
use crate::mesh::identity::{PeerIdentity, TransportEndpoint};
use crate::mesh::transport::{MeshTransport, TransportCapacity, TransportError, TransportReceipt};

#[derive(Debug)]
pub struct StubTransport {
    kind: &'static str,
}

impl StubTransport {
    pub fn web_rtc() -> Self {
        Self { kind: "webrtc" }
    }

    pub fn bluetooth() -> Self {
        Self { kind: "bluetooth" }
    }

    pub fn serial() -> Self {
        Self { kind: "serial" }
    }

    pub fn store_forward() -> Self {
        Self {
            kind: "store_forward",
        }
    }
}

impl MeshTransport for StubTransport {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn start(&self) -> Result<(), TransportError> {
        Err(TransportError::new(
            "transport_unavailable",
            format!("{} transport is not implemented in v0", self.kind),
        ))
    }

    fn stop(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn send(
        &self,
        _envelope: &ProtocolEnvelope,
        _target_peer: &PeerIdentity,
    ) -> Result<TransportReceipt, TransportError> {
        Err(TransportError::new(
            "transport_unavailable",
            format!("{} transport is not implemented in v0", self.kind),
        ))
    }

    fn status(&self) -> TransportEndpoint {
        TransportEndpoint {
            kind: self.kind.to_string(),
            status: "unavailable".to_string(),
            url: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn capacity(&self) -> TransportCapacity {
        TransportCapacity::unavailable()
    }

    fn capabilities(&self) -> Vec<String> {
        vec![]
    }
}
