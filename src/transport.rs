use parking_lot::Mutex;
use rand::Rng;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, mpsc::{Sender, SyncSender}}, thread, time::Duration,
};

use crate::raft::{CommandRequest, NetworkMessage, NodeID};

pub trait Transport {
    fn send(&self, target: NodeID, message: NetworkMessage);
    fn send_fifo(&self, sender: NodeID, target: NodeID, message: CommandRequest);
}

pub struct ChannelMockTransport {
    senders: HashMap<NodeID, SyncSender<NetworkMessage>>,
    network_delay_range: std::ops::RangeInclusive<u64>,
}

impl ChannelMockTransport {
    pub fn new(network_delay_range: std::ops::RangeInclusive<u64>) -> Self {
        ChannelMockTransport {
            senders: HashMap::new(),
            network_delay_range
        }
    }

    pub fn setup_senders(&mut self, senders: HashMap<NodeID, SyncSender<NetworkMessage>>) {
        self.senders = senders;
    }
}

impl Transport for ChannelMockTransport {
    

    fn send(&self, target: NodeID, message: NetworkMessage) {
        let sender = self.senders[&target].clone();
        let delay = Duration::from_millis(rand::thread_rng().gen_range(self.network_delay_range.clone()));
        thread::spawn(move || {
            thread::sleep(delay);
            sender.send(message).unwrap();
        });
    }

    fn send_fifo(&self, _sender: NodeID, target: NodeID, message: CommandRequest) {
        let inbox = self.senders[&target].clone();
        let delay = Duration::from_millis(rand::thread_rng().gen_range(self.network_delay_range.clone()));
        thread::spawn(move || {
            thread::sleep(delay);
            inbox.send(NetworkMessage::CommandRequest(message)).unwrap();
        });
    }
}
