use std::{collections::{HashMap, HashSet}, thread, time::Duration};

use simple_logger::SimpleLogger;

use crate::{transport::{channel_mock_transport::{ChannelMockTransport, NodeChannel}}, node::Node};


#[test]
fn test_startup() {
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