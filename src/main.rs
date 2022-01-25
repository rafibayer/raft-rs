mod networking;
mod node;
mod raft;
mod state;
mod utils;

#[cfg(test)]
mod test;

use std::{collections::HashMap, thread, time::Duration};

use raft::CommandRequest;

use simple_logger::SimpleLogger;

use node::Node;

use crate::{networking::client::Client, raft::AdminRequest, node::config::Config};

fn main() {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let port = 7878;
    let n = 5;

    let mut cluster = HashMap::new();

    for i in 0..n {
        cluster.insert(i, format!("127.0.0.1:{}", port + i).parse().unwrap());
    }

    let mut client = Client::new(cluster.clone());

    let config = Config {
        cluster,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 350,
        heartbeat_interval_ms: 50,
    };

    for i in 0..n {
        let config = config.clone();
        thread::spawn(move || {
            let node = Node::new(i, config, HashMap::new());
            node.start();
        });
    }

    thread::sleep(Duration::from_secs(2));

    log::warn!("***************** BECOME LEADER: 0 *****************");
    client.admin(0, AdminRequest::BecomeLeader).unwrap();

    thread::sleep(Duration::from_secs(2));

    log::warn!("***************** SHUTDOWN: 0 *****************");
    client.admin(0, AdminRequest::Shutdown).unwrap();

    thread::sleep(Duration::from_secs(2));

    log::warn!("***************** SENDING *****************");
    for i in 1..=100 {
        let x_val = i;
        client.apply_command(CommandRequest { command: format!("SET X {x_val}") }).unwrap();
    }

    log::warn!("***************** VERIFYING *****************");
    let result = client.apply_command(CommandRequest { command: "GET X".to_string() }).unwrap();

    println!("======= result: {result:?} =======");

    log::warn!("***************** TERMINATING *****************");
    thread::sleep(Duration::from_secs(3));
}
