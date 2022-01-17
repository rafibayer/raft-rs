use std::sync::mpsc::{Receiver, SyncSender};

use crate::{node::Node, state::Storage, raft::MessageData};

pub struct RaftServer<S: Storage> {
    node: Node<S>,
    into_node: SyncSender<MessageData>,
    from_node: Receiver<MessageData>
}