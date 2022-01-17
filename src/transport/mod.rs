use crate::raft::{CommandRequest, MessageData, NodeID, Message};

pub mod channel_mock_transport;

/// Represents a nodes communication method with other nodes.
pub trait Transport {
    /// Send a message to the target node
    fn send(&self, message: Message);
}


