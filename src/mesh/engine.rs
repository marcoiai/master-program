use crate::mesh::envelope::{PayloadType, ProtocolEnvelope, now_rfc3339};
use crate::mesh::identity::{
    ConnectivityPolicy, MeshPeer, NodeIdentity, NodeRole, PeerIdentity, RegisterPeerRequest,
    TransportEndpoint,
};
use crate::mesh::peer_registry::PeerRegistry;
use crate::mesh::rate_limit::{RateLane, RateLimiter};
use crate::mesh::transport::{MeshTransport, TransportCapacity, TransportError};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

const SEEN_MESSAGE_LIMIT: usize = 1024;
const SEEN_MESSAGE_TTL: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct MeshSendOutcome {
    pub ok: bool,
    pub peer: PeerIdentity,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MeshTransportStatus {
    pub endpoint: TransportEndpoint,
    pub capacity: TransportCapacity,
    pub capabilities: Vec<String>,
}

pub struct MeshEngine {
    node: NodeIdentity,
    peers: Mutex<PeerRegistry>,
    transports: Mutex<HashMap<String, Arc<dyn MeshTransport>>>,
    seen_messages: Mutex<VecDeque<(Uuid, Instant)>>,
    limiter: Mutex<RateLimiter>,
}

impl MeshEngine {
    pub fn new(node_id: String, display_name: Option<String>) -> Self {
        Self::new_with_role(
            node_id,
            display_name,
            NodeRole::Peer,
            ConnectivityPolicy::desktop_default(),
        )
    }

    pub fn new_with_role(
        node_id: String,
        display_name: Option<String>,
        role: NodeRole,
        connectivity_policy: ConnectivityPolicy,
    ) -> Self {
        let mut capabilities = vec![
            "mesh.control".to_string(),
            "mesh.stream".to_string(),
            "scene.layers".to_string(),
            "catalog.consumer".to_string(),
        ];
        capabilities.push(role.capability().to_string());
        if matches!(
            role,
            NodeRole::Bootstrap | NodeRole::Coordinator | NodeRole::Catalog
        ) {
            capabilities.push("catalog.publisher".to_string());
            capabilities.push("resource.suggest".to_string());
        }
        if matches!(role, NodeRole::Relay | NodeRole::Coordinator) {
            capabilities.push("mesh.relay".to_string());
        }
        capabilities.sort();
        capabilities.dedup();

        Self {
            node: NodeIdentity {
                node_id,
                role,
                display_name,
                capabilities,
                connectivity_policy,
                known_transports: vec![],
                last_seen: None,
                transmission_profile: None,
                metadata: Value::Null,
            },
            peers: Mutex::new(PeerRegistry::default()),
            transports: Mutex::new(HashMap::new()),
            seen_messages: Mutex::new(VecDeque::new()),
            limiter: Mutex::new(RateLimiter::default()),
        }
    }

    pub fn node(&self) -> NodeIdentity {
        self.node.clone()
    }

    pub fn register_transport(&self, transport: Arc<dyn MeshTransport>) {
        let mut transports = self.transports.lock().expect("transport lock");
        transports.insert(transport.kind().to_string(), transport);
    }

    pub fn transport_statuses(&self) -> Vec<MeshTransportStatus> {
        let transports = self.transports.lock().expect("transport lock");
        transports
            .values()
            .map(|transport| MeshTransportStatus {
                endpoint: transport.status(),
                capacity: transport.capacity(),
                capabilities: transport.capabilities(),
            })
            .collect()
    }

    pub fn register_peer_request(&self, req: RegisterPeerRequest) -> Result<PeerIdentity, String> {
        let id = req.id.trim();
        if id.is_empty() {
            return Err("peer_id_required".to_string());
        }

        let mut peer = if let Some(url) = req
            .url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            PeerIdentity::registered_http(id, url.trim_end_matches('/'))
        } else {
            let kind = req
                .transport
                .clone()
                .or_else(|| {
                    req.transports
                        .first()
                        .map(|transport| transport.kind.clone())
                })
                .unwrap_or_else(|| "unavailable".to_string());
            PeerIdentity::registered_stub(id, kind)
        };

        peer.display_name = req.display_name.filter(|value| !value.trim().is_empty());
        if !req.capabilities.is_empty() {
            peer.capabilities = req.capabilities;
        }
        if !req.transports.is_empty() {
            peer.transports = req.transports;
        }
        if let Some(profile) = req.transmission_profile {
            peer.transmission_profile = Some(profile);
        }
        if let Some(role) = req.role {
            peer.role = role;
            if !peer
                .capabilities
                .iter()
                .any(|item| item == peer.role.capability())
            {
                peer.capabilities.push(peer.role.capability().to_string());
            }
        }
        if let Some(policy) = req.connectivity_policy {
            peer.connectivity_policy = policy;
        }
        peer.metadata = req.metadata;

        let mut peers = self.peers.lock().expect("peer lock");
        Ok(peers.register(peer))
    }

    pub fn list_peers(&self) -> Vec<MeshPeer> {
        let peers = self.peers.lock().expect("peer lock");
        peers.list().into_iter().map(MeshPeer::from).collect()
    }

    pub fn ping_peer(&self, peer_id: &str) -> Result<MeshSendOutcome, String> {
        let peer = {
            let peers = self.peers.lock().expect("peer lock");
            peers
                .get(peer_id)
                .ok_or_else(|| "peer_not_found".to_string())?
        };
        let envelope = ProtocolEnvelope::new(
            self.node.node_id.clone(),
            Some(peer.id.clone()),
            "core.ping",
            PayloadType::Json,
            serde_json::json!({
                "message": format!("mesh ping from {}", self.node.node_id),
            }),
        );
        self.send_to_peer(envelope, peer)
    }

    pub fn send_to_peer(
        &self,
        envelope: ProtocolEnvelope,
        mut peer: PeerIdentity,
    ) -> Result<MeshSendOutcome, String> {
        envelope.validate()?;
        if self.mark_seen(envelope.message_id) {
            return Err("duplicate_message".to_string());
        }
        let bytes = envelope.serialized_len();
        {
            let mut limiter = self.limiter.lock().expect("rate limiter lock");
            if !limiter.allow_outbound(&peer.id, lane_for_message(&envelope.message_type), bytes) {
                peer.telemetry.rate_limited_count += 1;
                peer.escalation_state = "throttle".to_string();
                self.update_peer(peer.clone());
                return Err("peer_rate_limited".to_string());
            }
        }

        let transport = self.select_transport(&peer)?;
        let started = Instant::now();
        let receipt = transport
            .send(&envelope, &peer)
            .map_err(|error| self.apply_transport_error(peer.clone(), error))?;

        peer.status = "online".to_string();
        peer.last_seen = Some(now_rfc3339());
        peer.latency_ms = Some(started.elapsed().as_millis());
        peer.last_transport = Some(receipt.transport);
        peer.telemetry.rtt_ms = peer.latency_ms;
        peer.escalation_state = escalation_state(&peer);
        let peer = self.update_peer(peer);
        Ok(MeshSendOutcome { ok: true, peer })
    }

    pub fn receive(&self, envelope: ProtocolEnvelope) -> Result<bool, String> {
        envelope.validate()?;
        let peer_id = envelope.source_node_id.clone();
        let bytes = envelope.serialized_len();
        {
            let mut limiter = self.limiter.lock().expect("rate limiter lock");
            if !limiter.allow_inbound(&peer_id, lane_for_message(&envelope.message_type), bytes) {
                return Err("peer_rate_limited".to_string());
            }
        }
        if self.mark_seen(envelope.message_id) {
            return Ok(false);
        }
        if envelope.hop_count >= envelope.ttl {
            return Err("ttl_exhausted".to_string());
        }
        Ok(true)
    }

    fn select_transport(&self, peer: &PeerIdentity) -> Result<Arc<dyn MeshTransport>, String> {
        let transports = self.transports.lock().expect("transport lock");
        for endpoint in &peer.transports {
            if let Some(transport) = transports.get(&endpoint.kind) {
                return Ok(transport.clone());
            }
        }
        if peer.url.is_some() {
            if let Some(transport) = transports.get("local_http") {
                return Ok(transport.clone());
            }
        }
        Err("peer_transport_unavailable".to_string())
    }

    fn update_peer(&self, peer: PeerIdentity) -> PeerIdentity {
        let mut peers = self.peers.lock().expect("peer lock");
        peers.update(peer)
    }

    fn apply_transport_error(&self, mut peer: PeerIdentity, error: TransportError) -> String {
        peer.status = format!("offline: {}", error.code);
        peer.telemetry.dropped_messages += 1;
        peer.escalation_state = "warn".to_string();
        peer.last_transport = None;
        self.update_peer(peer);
        error.code
    }

    fn mark_seen(&self, message_id: Uuid) -> bool {
        let mut seen = self.seen_messages.lock().expect("seen lock");
        let now = Instant::now();
        while let Some((_, at)) = seen.front() {
            if now.duration_since(*at) <= SEEN_MESSAGE_TTL && seen.len() <= SEEN_MESSAGE_LIMIT {
                break;
            }
            seen.pop_front();
        }
        if seen.iter().any(|(id, _)| *id == message_id) {
            return true;
        }
        seen.push_back((message_id, now));
        false
    }
}

fn lane_for_message(message_type: &str) -> RateLane {
    if message_type == "stream.chunk" {
        RateLane::Stream
    } else {
        RateLane::Control
    }
}

fn escalation_state(peer: &PeerIdentity) -> String {
    let rtt = peer.telemetry.rtt_ms.unwrap_or(0);
    if rtt > 500 || peer.telemetry.dropped_messages > 8 {
        "fail_safe".to_string()
    } else if rtt > 240 || peer.telemetry.queue_depth > 32 {
        "degrade".to_string()
    } else if rtt > 120 || peer.telemetry.queue_depth > 16 {
        "throttle".to_string()
    } else if rtt > 80 {
        "warn".to_string()
    } else {
        "normal".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::envelope::PayloadType;
    use crate::mesh::transports::loopback::LoopbackTransport;

    #[test]
    fn receives_envelope_once_and_deduplicates_replay() {
        let engine = MeshEngine::new("node-a".to_string(), None);
        let env = ProtocolEnvelope::new(
            "node-b",
            Some("node-a".to_string()),
            "core.ping",
            PayloadType::Json,
            serde_json::json!({ "message": "hi" }),
        );

        assert_eq!(engine.receive(env.clone()).unwrap(), true);
        assert_eq!(engine.receive(env).unwrap(), false);
    }

    #[test]
    fn rejects_ttl_exhausted_receive() {
        let engine = MeshEngine::new("node-a".to_string(), None);
        let mut env = ProtocolEnvelope::new(
            "node-b",
            Some("node-a".to_string()),
            "core.ping",
            PayloadType::Json,
            serde_json::json!({ "message": "hi" }),
        );
        env.ttl = 1;
        env.hop_count = 1;

        assert_eq!(engine.receive(env).unwrap_err(), "ttl_exhausted");
    }

    #[test]
    fn sends_ping_over_loopback_transport() {
        let loopback = Arc::new(LoopbackTransport::default());
        let engine = MeshEngine::new("node-a".to_string(), None);
        engine.register_transport(loopback.clone());
        engine
            .register_peer_request(RegisterPeerRequest {
                id: "node-b".to_string(),
                url: None,
                display_name: None,
                transport: Some("loopback".to_string()),
                transports: vec![],
                capabilities: vec![],
                transmission_profile: None,
                role: None,
                connectivity_policy: None,
                metadata: Value::Null,
            })
            .unwrap();

        let outcome = engine.ping_peer("node-b").unwrap();
        assert!(outcome.ok);
        assert_eq!(loopback.drain().len(), 1);
    }
}
