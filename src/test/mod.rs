use std::thread;
use std::{thread::JoinHandle, collections::HashMap};

use crate::node::Node;
use crate::node::config::{Config, LogLevel};

use crate::networking::client::Client;

use std::{time::Duration, sync::{Arc, Mutex}};

use crate::raft::{AdminRequest, AdminResponse};
use crate::raft::CommandRequest;

mod consensus;
mod admin;


pub(crate) fn create_local_cluster(nodes: usize, start_port: usize) -> (Client, Vec<JoinHandle<()>>) {
    let mut cluster = HashMap::new();
    
    for i in 0..nodes {
        cluster.insert(i, format!("127.0.0.1:{}", start_port + i).parse().unwrap());
    };

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