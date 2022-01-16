use std::{collections::{HashMap, VecDeque}, sync::{Arc, Mutex, mpsc, MutexGuard}, thread};

use crate::{raft::{NodeID, NetworkMessage, CommandRequest}, node::Node};

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
}

impl MockTransport {
    pub fn new() -> Self {
        MockTransport {
            inboxes: HashMap::new(),
        }
    }

    pub fn setup_inboxes(&mut self, refs: HashMap<NodeID, Inbox>) {
        self.inboxes = refs;
    }
}

impl Transport for MockTransport {

    fn send(&self, target: NodeID, message: NetworkMessage) {
        self.inboxes[&target].lock().unwrap().push_back(message);
    }

    fn send_fifo(&self, sender: NodeID, target: NodeID, message: CommandRequest) {
        todo!()
    }
}
