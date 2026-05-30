use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NodeRole {
    Peer,
    Bootstrap,
    Relay,
    Coordinator,
    Catalog,
    Mobile,
}

impl NodeRole {
    pub fn from_env_value(value: Option<String>) -> Self {
        match value
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "bootstrap" => Self::Bootstrap,
            "relay" => Self::Relay,
            "coordinator" | "central" => Self::Coordinator,
            "catalog" => Self::Catalog,
            "mobile" | "cell" | "phone" => Self::Mobile,
            _ => Self::Peer,
        }
    }

    pub fn capability(&self) -> &'static str {
        match self {
            Self::Peer => "mesh.peer",
            Self::Bootstrap => "mesh.bootstrap",
            Self::Relay => "mesh.relay",
            Self::Coordinator => "scene.coordinator",
            Self::Catalog => "catalog.publisher",
            Self::Mobile => "mesh.mobile",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectivityPolicy {
    pub allow_inbound: bool,
    pub allow_relay: bool,
    pub allow_background_discovery: bool,
    pub requires_user_consent: bool,
    pub max_scan_window_seconds: u16,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl ConnectivityPolicy {
    pub fn desktop_default() -> Self {
        Self {
            allow_inbound: true,
            allow_relay: false,
            allow_background_discovery: false,
            requires_user_consent: true,
            max_scan_window_seconds: 30,
            notes: vec!["local-first peer; relay must be explicitly enabled".to_string()],
        }
    }

    pub fn mobile_default() -> Self {
        Self {
            allow_inbound: false,
            allow_relay: false,
            allow_background_discovery: false,
            requires_user_consent: true,
            max_scan_window_seconds: 10,
            notes: vec![
                "mobile nodes start as lightweight clients".to_string(),
                "continuous discovery and silent app updates are disabled".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Destination {
    Direct,
    Broadcast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransmissionProfile {
    pub id: String,
    pub intentional_artifacts: bool,
    pub budget_class: String,
    pub target_fps: u16,
    pub target_video_kbps: u32,
    pub max_video_kbps: u32,
    pub audio_mode: String,
    #[serde(default)]
    pub visual_masking: Vec<String>,
}

impl TransmissionProfile {
    pub fn clean_arcade() -> Self {
        Self {
            id: "clean_arcade".to_string(),
            intentional_artifacts: false,
            budget_class: "medium".to_string(),
            target_fps: 30,
            target_video_kbps: 1800,
            max_video_kbps: 2600,
            audio_mode: "stereo".to_string(),
            visual_masking: vec![],
        }
    }

    pub fn pirate_broadcast() -> Self {
        Self {
            id: "pirate_broadcast".to_string(),
            intentional_artifacts: true,
            budget_class: "low".to_string(),
            target_fps: 18,
            target_video_kbps: 900,
            max_video_kbps: 1400,
            audio_mode: "mono_or_off".to_string(),
            visual_masking: vec![
                "scanlines".to_string(),
                "color_crawl".to_string(),
                "jitter".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportEndpoint {
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

impl TransportEndpoint {
    pub fn http(url: impl Into<String>) -> Self {
        Self {
            kind: "local_http".to_string(),
            status: "registered".to_string(),
            url: Some(url.into()),
            metadata: Value::Null,
        }
    }

    pub fn unavailable(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            status: "unavailable".to_string(),
            url: None,
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeIdentity {
    pub node_id: String,
    pub role: NodeRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub connectivity_policy: ConnectivityPolicy,
    #[serde(default)]
    pub known_transports: Vec<TransportEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transmission_profile: Option<TransmissionProfile>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerTelemetry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rtt_ms: Option<u128>,
    pub ack_delay_ms: u128,
    pub queue_depth: usize,
    pub dropped_messages: u64,
    pub retransmit_count: u64,
    pub rate_limited_count: u64,
}

impl Default for PeerTelemetry {
    fn default() -> Self {
        Self {
            rtt_ms: None,
            ack_delay_ms: 0,
            queue_depth: 0,
            dropped_messages: 0,
            retransmit_count: 0,
            rate_limited_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerIdentity {
    pub id: String,
    pub role: NodeRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub status: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub transports: Vec<TransportEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transport: Option<String>,
    pub escalation_state: String,
    pub telemetry: PeerTelemetry,
    pub connectivity_policy: ConnectivityPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transmission_profile: Option<TransmissionProfile>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

impl PeerIdentity {
    pub fn registered_http(id: impl Into<String>, url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            id: id.into(),
            role: NodeRole::Peer,
            display_name: None,
            url: Some(url.clone()),
            status: "registered".to_string(),
            capabilities: vec!["core.ping".to_string(), "mesh.control".to_string()],
            transports: vec![TransportEndpoint::http(url)],
            last_seen: None,
            latency_ms: None,
            last_transport: None,
            escalation_state: "normal".to_string(),
            telemetry: PeerTelemetry::default(),
            connectivity_policy: ConnectivityPolicy::desktop_default(),
            transmission_profile: Some(TransmissionProfile::clean_arcade()),
            metadata: Value::Null,
        }
    }

    pub fn registered_stub(id: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            role: NodeRole::Peer,
            display_name: None,
            url: None,
            status: "registered".to_string(),
            capabilities: vec!["mesh.control".to_string()],
            transports: vec![TransportEndpoint::unavailable(kind)],
            last_seen: None,
            latency_ms: None,
            last_transport: None,
            escalation_state: "normal".to_string(),
            telemetry: PeerTelemetry::default(),
            connectivity_policy: ConnectivityPolicy::desktop_default(),
            transmission_profile: Some(TransmissionProfile::pirate_broadcast()),
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeer {
    pub id: String,
    pub role: NodeRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub status: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub transports: Vec<TransportEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transport: Option<String>,
    pub escalation_state: String,
    pub telemetry: PeerTelemetry,
    pub connectivity_policy: ConnectivityPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transmission_profile: Option<TransmissionProfile>,
}

impl From<PeerIdentity> for MeshPeer {
    fn from(peer: PeerIdentity) -> Self {
        Self {
            id: peer.id,
            role: peer.role,
            display_name: peer.display_name,
            url: peer.url,
            status: peer.status,
            capabilities: peer.capabilities,
            transports: peer.transports,
            last_seen: peer.last_seen,
            latency_ms: peer.latency_ms,
            last_transport: peer.last_transport,
            escalation_state: peer.escalation_state,
            telemetry: peer.telemetry,
            connectivity_policy: peer.connectivity_policy,
            transmission_profile: peer.transmission_profile,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterPeerRequest {
    pub id: String,
    #[serde(default)]
    pub role: Option<NodeRole>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub transport: Option<String>,
    #[serde(default)]
    pub transports: Vec<TransportEndpoint>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub transmission_profile: Option<TransmissionProfile>,
    #[serde(default)]
    pub connectivity_policy: Option<ConnectivityPolicy>,
    #[serde(default)]
    pub metadata: Value,
}
