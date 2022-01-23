mod async_tcp;
mod node;
mod raft;
mod state;
mod utils;

use std::{collections::HashMap, net::TcpStream, thread, time::Duration};

use raft::{CommandRequest, RaftRequest};
use rand::Rng;
use simple_logger::SimpleLogger;

use node::Node;

fn main() {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let port = 7878;
    let n = 3;

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
    thread::sleep(Duration::from_secs(5));

    log::warn!("***************** SENDING *****************");
    for i in 0..10_000 {
        async_tcp::apply_command(
            CommandRequest { command: format!("SET X {i}") },
            rand::thread_rng().gen_range(0..n),
            &cluster,
        )
        .unwrap();
    }

    log::warn!("***************** SENDING *****************");
    let result =
        async_tcp::apply_command(CommandRequest { command: "GET X".to_string() }, 0, &cluster)
            .unwrap();

    println!("======= result: {result} =======");

    log::warn!("***************** TERMINATING *****************");
    thread::sleep(Duration::from_secs(5));
}
