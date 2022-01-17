use std::{collections::HashMap, sync::mpsc::{Sender, Receiver}};

use crate::raft::{NodeID, Message};

use super::Transport;

pub struct NodeChannel {
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>
}

/// Implements [`Transport`] using channels for local testing
pub struct ChannelMockTransport {
    nodes: HashMap<NodeID, NodeChannel>,
    _network_delay_range: std::ops::RangeInclusive<u64>,
}

impl ChannelMockTransport {
    /// Create a new [`ChannelMockTransport`] with a randomized delay in ms.
    pub fn new(_network_delay_range: std::ops::RangeInclusive<u64>, nodes: HashMap<NodeID, NodeChannel>) -> Self {
        ChannelMockTransport { nodes, _network_delay_range }
    }

    pub fn start(&self) -> ! {
        loop {
            for node in self.nodes.values() {
                if let Ok(outgoing) = node.receiver.try_recv() {
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