use std::sync::mpsc::{Receiver, Sender};

use crate::{node::Node, state::Storage, raft::MessageData};

pub struct RaftServer<S: Storage> {
    node: Node<S>,
    into_node: Sender<MessageData>,
    from_node: Receiver<MessageData>
}