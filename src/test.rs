use std::{sync::{Arc}, collections::{HashMap, HashSet}, thread};
use parking_lot::Mutex;

use simple_logger::SimpleLogger;

use crate::{transport::{ChannelMockTransport}, node::Node};




#[test]
fn test_startup() {
    SimpleLogger::new().init().unwrap();
    let n = 3;

    let mut transport = ChannelMockTransport::new(0..=0);

    let mut nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        nodes.push(Node::new(i, node_set.clone(), HashMap::new()));
    }

    let mut senders = HashMap::new();
    for (i, node) in nodes.iter().enumerate() {
        senders.insert(i, node.get_sender());
    }

    transport.setup_senders(senders);
    let arc_transport = Arc::new(transport);

    for mut node in nodes {
        let arc_transport_clone = arc_transport.clone();
        thread::spawn(move || node.start(arc_transport_clone));
    }
}