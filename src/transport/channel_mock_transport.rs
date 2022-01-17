use std::{collections::HashMap, sync::mpsc::{SyncSender, Receiver}, time::Duration, thread};

use rand::Rng;

use crate::raft::{NodeID, Message};

use super::Transport;

pub struct NodeChannel {
    pub sender: SyncSender<Message>,
    pub receiver: Receiver<Message>
}

/// Implements [`Transport`] using channels for local testing
pub struct ChannelMockTransport {
    nodes: HashMap<NodeID, NodeChannel>,
    network_delay_range: std::ops::RangeInclusive<u64>,
}

impl ChannelMockTransport {
    /// Create a new [`ChannelMockTransport`] with a randomized delay in ms.
    pub fn new(network_delay_range: std::ops::RangeInclusive<u64>, nodes: HashMap<NodeID, NodeChannel>) -> Self {
        ChannelMockTransport { nodes, network_delay_range }
    }

    pub fn start(&self) {
        loop {
            for node in self.nodes.values() {
                if let Some(outgoing) = node.receiver.try_recv().ok() {
                    self.send(outgoing);
                }
            }
        }
    }
}

impl Transport for ChannelMockTransport {
    fn send(&self, message: Message) {
        self.nodes[&message.destination].sender.send(message).unwrap();
    }
}