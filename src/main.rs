mod raft;
mod state;
mod node;
mod utils;
mod transport;

// thoughts: probably need to reset/stop election timer in more places.
// I think we are assuming that only follower receives heartbeats
// but candidate does to?? prob rewatch video/other impl

#[cfg(test)]
mod test;

use std::{thread, time::Duration, sync::{Arc}, collections::{HashSet, HashMap}};
use parking_lot::Mutex;

use simple_logger::SimpleLogger;

use node::Node;
use raft::{NetworkMessage, CommandRequest};
use transport::{MockTransport, Transport};


fn main() {

    SimpleLogger::new().init().unwrap();
    let n = 3;

    let mut transport = MockTransport::new(0..=0);

    let mut nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        nodes.push(Arc::new(Mutex::new(Node::new(i, node_set.clone(), HashMap::new()))));
    }

    let mut inboxes = HashMap::new();
    for (i, node) in nodes.iter().enumerate() {
        inboxes.insert(i, node.lock().get_inbox());
    }

    transport.setup_inboxes(inboxes);
    let transport_arc = Arc::new(transport);

    for node in nodes {
        let transport_clone = transport_arc.clone();
        thread::spawn(move || node.lock().start(transport_clone));
    }
   
    thread::sleep(Duration::from_secs(5));

    transport_arc.send(2, NetworkMessage::CommandRequest(CommandRequest{command: "SET X 5".to_string()}));


    thread::sleep(Duration::from_secs(1));

}