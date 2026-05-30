use crate::mesh::envelope::ProtocolEnvelope;
use crate::mesh::identity::{PeerIdentity, TransportEndpoint};
use crate::mesh::transport::{MeshTransport, TransportCapacity, TransportError, TransportReceipt};
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct LoopbackTransport {
    messages: Mutex<Vec<ProtocolEnvelope>>,
}

impl LoopbackTransport {
    pub fn drain(&self) -> Vec<ProtocolEnvelope> {
        let mut messages = self.messages.lock().expect("loopback lock");
        std::mem::take(&mut *messages)
    }
}

impl MeshTransport for LoopbackTransport {
    fn kind(&self) -> &'static str {
        "loopback"
    }

    fn start(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn stop(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn send(
        &self,
        envelope: &ProtocolEnvelope,
        _target_peer: &PeerIdentity,
    ) -> Result<TransportReceipt, TransportError> {
        let mut messages = self.messages.lock().expect("loopback lock");
        messages.push(envelope.clone());
        Ok(TransportReceipt {
            transport: self.kind().to_string(),
        })
    }

    fn broadcast(
        &self,
        envelope: &ProtocolEnvelope,
    ) -> Result<Vec<TransportReceipt>, TransportError> {
        let mut messages = self.messages.lock().expect("loopback lock");
        messages.push(envelope.clone());
        Ok(vec![TransportReceipt {
            transport: self.kind().to_string(),
        }])
    }

    fn status(&self) -> TransportEndpoint {
        TransportEndpoint {
            kind: self.kind().to_string(),
            status: "available".to_string(),
            url: None,
            metadata: serde_json::Value::Null,
        }
    }

    fn capacity(&self) -> TransportCapacity {
        TransportCapacity {
            max_message_bytes: 64 * 1024,
            recommended_chunk_bytes: 8 * 1024,
            max_in_flight: 64,
            estimated_throughput_kbps: 100_000,
        }
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["mesh.test".to_string(), "mesh.control".to_string()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::envelope::PayloadType;

    #[test]
    fn stores_loopback_message() {
        let transport = LoopbackTransport::default();
        let peer = PeerIdentity::registered_stub("self", "loopback");
        let env = ProtocolEnvelope::new(
            "self",
            Some("self".to_string()),
            "test",
            PayloadType::Text,
            serde_json::json!("hi"),
        );
        transport.send(&env, &peer).unwrap();
        assert_eq!(transport.drain().len(), 1);
    }
}
