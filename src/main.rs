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

use rand::Rng;
use simple_logger::SimpleLogger;

use node::Node;
use raft::{NetworkMessage, CommandRequest};
use transport::{Transport, ChannelMockTransport};

fn main() {

    SimpleLogger::new().with_level(log::LevelFilter::Trace).without_timestamps().init().unwrap();
    let n = 5;

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
   
    thread::sleep(Duration::from_secs(10));

    for i in 0..25 {
        let tgt = rand::thread_rng().gen_range(0..n);
        arc_transport.send(tgt, NetworkMessage::CommandRequest(
            CommandRequest{command: format!("SET X {}", i)}));
    }

    thread::sleep(Duration::from_secs(25));
}
