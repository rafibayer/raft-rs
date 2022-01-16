use parking_lot::Mutex;
use rand::Rng;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc, thread, time::Duration,
};

use crate::raft::{CommandRequest, NetworkMessage, NodeID};

type Inbox = Arc<Mutex<VecDeque<NetworkMessage>>>;

pub trait Transport {
    fn send(&self, target: NodeID, message: NetworkMessage);
    fn send_fifo(&self, sender: NodeID, target: NodeID, message: CommandRequest);
}

/// (tx.send, rx.recv)
/// (Sender<T>, Receiver<T>)
///
///   

pub struct MockTransport {
    pub inboxes: HashMap<NodeID, Inbox>,
    network_delay_range: std::ops::RangeInclusive<u64>,
}

impl MockTransport {
    pub fn new(network_delay_range: std::ops::RangeInclusive<u64>) -> Self {
        MockTransport {
            inboxes: HashMap::new(),
            network_delay_range
        }
    }

    pub fn setup_inboxes(&mut self, refs: HashMap<NodeID, Inbox>) {
        self.inboxes = refs;
    }
}

impl Transport for MockTransport {
    fn send(&self, target: NodeID, message: NetworkMessage) {
        let inbox = self.inboxes[&target].clone();
        let delay = Duration::from_millis(rand::thread_rng().gen_range(self.network_delay_range.clone()));
        thread::spawn(move || {
            thread::sleep(delay);
            inbox.lock().push_back(message);
        });
    }

    fn send_fifo(&self, _sender: NodeID, _target: NodeID, _message: CommandRequest) {
        todo!()
    }
}
