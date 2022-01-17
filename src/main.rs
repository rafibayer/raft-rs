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

use std::{thread, time::Duration, collections::{HashSet, HashMap}};

use simple_logger::SimpleLogger;

use node::Node;
use transport::{channel_mock_transport::{ChannelMockTransport, NodeChannel}};

fn main() {

    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();
    let n = 11;

    // collection of nodes and corresponding channels
    let mut new_nodes = Vec::new();

    let node_set = HashSet::from_iter(0..n);

    for i in 0..n {
        new_nodes.push(Node::new_node(i, node_set.clone(), HashMap::new()));
    }

    let mut nodes = Vec::new();
    let mut node_channels = HashMap::new();
    for new_node in new_nodes {
        node_channels.insert(new_node.node.id, NodeChannel {
            sender: new_node.sender,
            receiver: new_node.receiver,
        });

        nodes.push(new_node.node);
    }

    let transport = ChannelMockTransport::new(0..=0, node_channels);
    thread::spawn(move || {
        transport.start();
    });

    nodes.into_iter().for_each(|mut node| {
        thread::spawn(move || node.start());
    });

    thread::sleep(Duration::from_secs(10));
}
