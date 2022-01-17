mod raft;
mod state;
mod node;
mod utils;
mod transport;
mod server;

// thoughts: probably need to reset/stop election timer in more places.
// I think we are assuming that only follower receives heartbeats
// but candidate does to?? prob rewatch video/other impl

#[cfg(test)]
mod test;

use std::{thread, time::Duration, sync::{Arc}, collections::{HashSet, HashMap}};

use rand::Rng;
use simple_logger::SimpleLogger;

use node::Node;
use raft::{Message, CommandRequest};
use transport::{Transport, channel_mock_transport::{ChannelMockTransport, NodeChannel}};

fn main() {

    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();
    let n = 5;


    let mut nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        nodes.push(Node::new(i, node_set.clone(), HashMap::new()));
    }

    let mut node_channels = HashMap::new();
    for mut node in nodes {
        
        node_channels.insert(node.node.id, NodeChannel {
            sender: node.sender,
            receiver: node.receiver,
        });

        thread::spawn(move || node.node.start());
    }

    let transport = ChannelMockTransport::new(0..=0, node_channels);
    thread::spawn(move|| {
        transport.start();
    });

    thread::sleep(Duration::from_secs(10));
}
