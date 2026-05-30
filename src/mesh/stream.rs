use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const STREAM_OPEN: &str = "stream.open";
pub const STREAM_CHUNK: &str = "stream.chunk";
pub const STREAM_ACK: &str = "stream.ack";
pub const STREAM_NACK: &str = "stream.nack";
pub const STREAM_END: &str = "stream.end";
pub const STREAM_CANCEL: &str = "stream.cancel";
pub const STREAM_ERROR: &str = "stream.error";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamChunk {
    pub session_id: Uuid,
    pub chunk_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_chunks: Option<u64>,
    pub byte_length: usize,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamState {
    Open,
    Ended,
    Cancelled,
    Error,
}

#[derive(Debug)]
pub struct StreamSession {
    pub session_id: Uuid,
    pub state: StreamState,
    chunks: BTreeMap<u64, String>,
    seen_chunks: HashSet<u64>,
    total_chunks: Option<u64>,
    opened_at: Instant,
    last_activity: Instant,
    pub retransmit_count: u64,
}

impl StreamSession {
    pub fn new(total_chunks: Option<u64>) -> Self {
        Self {
            session_id: Uuid::new_v4(),
            state: StreamState::Open,
            chunks: BTreeMap::new(),
            seen_chunks: HashSet::new(),
            total_chunks,
            opened_at: Instant::now(),
            last_activity: Instant::now(),
            retransmit_count: 0,
        }
    }

    pub fn accept_chunk(&mut self, chunk: StreamChunk) -> Result<(), String> {
        if self.state != StreamState::Open {
            return Err("stream_not_open".to_string());
        }
        if chunk.session_id != self.session_id {
            return Err("stream_session_mismatch".to_string());
        }
        if self.seen_chunks.contains(&chunk.chunk_index) {
            return Ok(());
        }
        if chunk.byte_length != chunk.payload.len() {
            return Err("stream_chunk_length_mismatch".to_string());
        }
        if let Some(total_chunks) = chunk.total_chunks {
            self.total_chunks = Some(total_chunks);
        }
        self.seen_chunks.insert(chunk.chunk_index);
        self.chunks.insert(chunk.chunk_index, chunk.payload);
        self.last_activity = Instant::now();
        Ok(())
    }

    pub fn missing_chunks(&self) -> Vec<u64> {
        let Some(total) = self.total_chunks else {
            return vec![];
        };
        (0..total)
            .filter(|index| !self.seen_chunks.contains(index))
            .collect()
    }

    pub fn reassemble_text(&self) -> Result<String, String> {
        let Some(total) = self.total_chunks else {
            return Err("stream_unknown_length".to_string());
        };
        let missing = self.missing_chunks();
        if !missing.is_empty() {
            return Err("stream_missing_chunk".to_string());
        }
        let mut output = String::new();
        for index in 0..total {
            if let Some(chunk) = self.chunks.get(&index) {
                output.push_str(chunk);
            }
        }
        Ok(output)
    }

    pub fn cancel(&mut self) {
        self.state = StreamState::Cancelled;
    }

    pub fn mark_error(&mut self) {
        self.state = StreamState::Error;
    }

    pub fn end(&mut self) {
        self.state = StreamState::Ended;
    }

    pub fn timed_out(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    pub fn reassembly_delay_ms(&self) -> u128 {
        self.opened_at.elapsed().as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reassembles_out_of_order_text_chunks() {
        let mut session = StreamSession::new(Some(3));
        let id = session.session_id;
        session
            .accept_chunk(StreamChunk {
                session_id: id,
                chunk_index: 2,
                total_chunks: Some(3),
                byte_length: 1,
                payload: "!".to_string(),
            })
            .unwrap();
        session
            .accept_chunk(StreamChunk {
                session_id: id,
                chunk_index: 0,
                total_chunks: Some(3),
                byte_length: 5,
                payload: "hello".to_string(),
            })
            .unwrap();
        session
            .accept_chunk(StreamChunk {
                session_id: id,
                chunk_index: 1,
                total_chunks: Some(3),
                byte_length: 1,
                payload: " ".to_string(),
            })
            .unwrap();
        assert_eq!(session.reassemble_text().unwrap(), "hello !");
    }

    #[test]
    fn reports_missing_chunks() {
        let mut session = StreamSession::new(Some(2));
        let id = session.session_id;
        session
            .accept_chunk(StreamChunk {
                session_id: id,
                chunk_index: 0,
                total_chunks: Some(2),
                byte_length: 2,
                payload: "ok".to_string(),
            })
            .unwrap();
        assert_eq!(session.missing_chunks(), vec![1]);
    }
}
