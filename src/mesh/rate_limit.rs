use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLane {
    Control,
    Stream,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_per_second: f64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_per_second,
            last_refill: Instant::now(),
        }
    }

    fn allow(&mut self, cost: f64) -> bool {
        self.refill();
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_second).min(self.capacity);
        self.last_refill = Instant::now();
    }
}

#[derive(Debug)]
pub struct RateLimiter {
    control: HashMap<String, TokenBucket>,
    stream: HashMap<String, TokenBucket>,
    global_in: TokenBucket,
    global_out: TokenBucket,
}

impl Default for RateLimiter {
    fn default() -> Self {
        let bytes_per_second = 2.0 * 1024.0 * 1024.0;
        Self {
            control: HashMap::new(),
            stream: HashMap::new(),
            global_in: TokenBucket::new(bytes_per_second, bytes_per_second),
            global_out: TokenBucket::new(bytes_per_second, bytes_per_second),
        }
    }
}

impl RateLimiter {
    pub fn allow_outbound(&mut self, peer_id: &str, lane: RateLane, bytes: usize) -> bool {
        if !self.global_out.allow(bytes.max(1) as f64) {
            return false;
        }
        self.allow_peer(peer_id, lane, bytes)
    }

    pub fn allow_inbound(&mut self, peer_id: &str, lane: RateLane, bytes: usize) -> bool {
        if !self.global_in.allow(bytes.max(1) as f64) {
            return false;
        }
        self.allow_peer(peer_id, lane, bytes)
    }

    fn allow_peer(&mut self, peer_id: &str, lane: RateLane, bytes: usize) -> bool {
        match lane {
            RateLane::Control => self
                .control
                .entry(peer_id.to_string())
                .or_insert_with(|| TokenBucket::new(40.0, 20.0))
                .allow(1.0),
            RateLane::Stream => {
                let per_second = 200.0 * 1024.0;
                let burst = 400.0 * 1024.0;
                self.stream
                    .entry(peer_id.to_string())
                    .or_insert_with(|| TokenBucket::new(burst, per_second))
                    .allow(bytes.max(1) as f64)
            }
        }
    }

    pub fn retry_after() -> Duration {
        Duration::from_millis(250)
    }
}
