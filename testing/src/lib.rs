use std::thread;
use std::{collections::HashMap, thread::JoinHandle};

use core::node::config::{Config, LogLevel};
use core::node::Node;
use core::raft::CommandRequest;
use core::raft::{AdminRequest, AdminResponse};

use std::sync;

use client::Client;

mod admin;
mod consensus;

pub(crate) fn create_local_cluster(
    nodes: usize,
    start_port: usize,
) -> (Client, Vec<JoinHandle<()>>) {
    let mut cluster = HashMap::new();

    for i in 0..nodes {
        cluster.insert(i, format!("127.0.0.1:{}", start_port + i).parse().unwrap());
    }

    let client = Client::new(cluster.clone());

    let config = Config {
        cluster,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 350,
        heartbeat_interval_ms: 50,
        log_level: LogLevel::Off,
    };

    let mut handles = Vec::new();
    for i in 0..nodes {
        let config = config.clone();
        handles.push(thread::spawn(move || {
            let node = Node::new(i, config, HashMap::new());
            node.start();
        }));
    }

    (client, handles)
}
