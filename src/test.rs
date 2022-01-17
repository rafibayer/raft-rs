use std::{sync::{Arc}, collections::{HashMap, HashSet}, thread, time::Duration};
use parking_lot::Mutex;

use rand::Rng;
use simple_logger::SimpleLogger;

use crate::{transport::{ChannelMockTransport, Transport}, node::Node, raft::{NetworkMessage, CommandRequest}};




#[test]
fn test_startup() {
    SimpleLogger::new().without_timestamps().init().unwrap();
    let n = 15;

    let mut transport = ChannelMockTransport::new(10..=50);

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
   
    thread::sleep(Duration::from_secs(10));

    for i in 0..25 {
        let tgt = rand::thread_rng().gen_range(0..n);
        arc_transport.send(tgt, NetworkMessage::CommandRequest(
            CommandRequest{command: format!("SET X {}", i)}));
    }

    thread::sleep(Duration::from_secs(25));
}