use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use time::OffsetDateTime;
use uuid::Uuid;

pub const PROTOCOL_VERSION: &str = "mesh.v0";
pub const MAX_ENVELOPE_BYTES: usize = 64 * 1024;
pub const CONTROL_TARGET_BYTES: usize = 2 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PayloadType {
    Json,
    Text,
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DestinationKind {
    Direct,
    Broadcast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolEnvelope {
    pub protocol_version: String,
    pub message_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<Uuid>,
    pub source_node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    pub destination: DestinationKind,
    pub timestamp: String,
    pub ttl: u8,
    pub hop_count: u8,
    pub message_type: String,
    pub payload_type: PayloadType,
    #[serde(default)]
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

impl ProtocolEnvelope {
    pub fn new(
        source_node_id: impl Into<String>,
        target_node_id: Option<String>,
        message_type: impl Into<String>,
        payload_type: PayloadType,
        payload: Value,
    ) -> Self {
        let target_node_id = target_node_id.filter(|value| !value.trim().is_empty());
        let destination = if target_node_id.is_some() {
            DestinationKind::Direct
        } else {
            DestinationKind::Broadcast
        };
        let mut envelope = Self {
            protocol_version: PROTOCOL_VERSION.to_string(),
            message_id: Uuid::new_v4(),
            correlation_id: None,
            source_node_id: source_node_id.into(),
            target_node_id,
            destination,
            timestamp: now_rfc3339(),
            ttl: 8,
            hop_count: 0,
            message_type: message_type.into(),
            payload_type,
            payload,
            checksum: None,
            metadata: Value::Null,
        };
        envelope.checksum = Some(envelope.payload_checksum());
        envelope
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err("unsupported_protocol_version".to_string());
        }
        if self.source_node_id.trim().is_empty() {
            return Err("source_node_id_required".to_string());
        }
        if self.message_type.trim().is_empty() {
            return Err("message_type_required".to_string());
        }
        if self.hop_count > self.ttl {
            return Err("ttl_exhausted".to_string());
        }
        let serialized =
            serde_json::to_vec(self).map_err(|error| format!("invalid_envelope: {error}"))?;
        if serialized.len() > MAX_ENVELOPE_BYTES {
            return Err("payload_too_large".to_string());
        }
        Ok(())
    }

    pub fn forwarded(&self) -> Result<Self, String> {
        if self.hop_count >= self.ttl {
            return Err("ttl_exhausted".to_string());
        }
        let mut next = self.clone();
        next.hop_count += 1;
        Ok(next)
    }

    pub fn serialized_len(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX)
    }

    fn payload_checksum(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.protocol_version.hash(&mut hasher);
        self.message_id.hash(&mut hasher);
        self.source_node_id.hash(&mut hasher);
        self.target_node_id.hash(&mut hasher);
        self.message_type.hash(&mut hasher);
        serde_json::to_string(&self.payload)
            .unwrap_or_default()
            .hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_valid_envelope_with_defaults() {
        let env = ProtocolEnvelope::new(
            "node-a",
            Some("node-b".to_string()),
            "core.ping",
            PayloadType::Json,
            serde_json::json!({ "ok": true }),
        );
        assert_eq!(env.protocol_version, PROTOCOL_VERSION);
        assert_eq!(env.destination, DestinationKind::Direct);
        assert!(env.checksum.is_some());
        assert!(env.validate().is_ok());
    }

    #[test]
    fn rejects_ttl_exhausted() {
        let mut env = ProtocolEnvelope::new(
            "node-a",
            None,
            "mesh.broadcast",
            PayloadType::Empty,
            Value::Null,
        );
        env.ttl = 1;
        env.hop_count = 2;
        assert_eq!(env.validate().unwrap_err(), "ttl_exhausted");
    }
}
