mod async_tcp;
mod node;
mod raft;
mod state;
mod utils;

use std::{collections::HashMap, thread, time::Duration};

use simple_logger::SimpleLogger;

use node::Node;

fn main() {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let port = 7878;
    let n = 43;

    let mut cluster = HashMap::new();

    for i in 0..n {
        cluster.insert(i, format!("127.0.0.1:{}", port + i).parse().unwrap());
    }

    for i in 0..n {
        let cluster = cluster.clone();
        thread::spawn(move || {
            let mut node = Node::new(i, cluster.clone(), HashMap::new());
            node.start();
        });
    }
    thread::sleep(Duration::from_secs(900000));
}
