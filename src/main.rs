mod raft;
mod state;
mod node;
mod utils;
mod transport;

#[cfg(test)]
mod test;

use std::{thread, time::Duration, sync::{Arc, Mutex}, collections::{HashSet, HashMap}};

use log;
use simple_logger::SimpleLogger;

use node::Node;
use raft::{Role, NetworkMessage, CommandRequest};
use transport::{MockTransport, Transport};

/// Implementation notes:
///     may need better mutex, mutexs don't get locked in same
///     order that threads start waiting. 
///     e.g. 
///     state = Mutex()
///     *thread 1*: state.lock(); 
/// 


fn main() {
    SimpleLogger::new().init().unwrap();
    let n = 3;

    let transport = Arc::new(Mutex::new(MockTransport::new()));

    let mut nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        nodes.push(Arc::new(Mutex::new(Node::new(i, node_set.clone(), transport.clone(), HashMap::new()))));
    }

    let mut inboxes = HashMap::new();
    for i in 0..n {
        inboxes.insert(i, nodes[i].lock().unwrap().get_inbox());
        nodes[i].lock().unwrap().start(nodes[i].clone());
    }

    {
        transport.lock().unwrap().setup_inboxes(inboxes);
    }

    thread::sleep(Duration::from_secs(10));

}