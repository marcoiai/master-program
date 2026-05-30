use crate::mesh::identity::PeerIdentity;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct PeerRegistry {
    peers: HashMap<String, PeerIdentity>,
}

impl PeerRegistry {
    pub fn register(&mut self, peer: PeerIdentity) -> PeerIdentity {
        self.peers.insert(peer.id.clone(), peer.clone());
        peer
    }

    pub fn get(&self, id: &str) -> Option<PeerIdentity> {
        self.peers.get(id).cloned()
    }

    pub fn list(&self) -> Vec<PeerIdentity> {
        let mut peers = self.peers.values().cloned().collect::<Vec<_>>();
        peers.sort_by(|a, b| a.id.cmp(&b.id));
        peers
    }

    pub fn update(&mut self, peer: PeerIdentity) -> PeerIdentity {
        self.peers.insert(peer.id.clone(), peer.clone());
        peer
    }
}
