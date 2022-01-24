mod async_tcp;
mod node;
mod raft;
mod state;
mod utils;

use std::{collections::HashMap, thread, time::Duration};

use raft::{CommandRequest};
use rand::Rng;
use simple_logger::SimpleLogger;

use node::Node;

use crate::{async_tcp::admin, raft::AdminRequest};

#[cfg(test)]
mod test;

fn main() {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let port = 7878;
    let n = 5;

    let mut cluster = HashMap::new();

    for i in 0..n {
        cluster.insert(i, format!("127.0.0.1:{}", port + i).parse().unwrap());
    }

    for i in 0..n {
        let cluster = cluster.clone();
        thread::spawn(move || {
            let node = Node::new(i, cluster, HashMap::new());
            node.start();
        });
    }

    thread::sleep(Duration::from_secs(2));

    log::warn!("***************** BECOME LEADER: 0 *****************");
    admin(AdminRequest::BecomeLeader, cluster[&0]);

    thread::sleep(Duration::from_secs(2));


    log::warn!("***************** SHUTDOWN: 0 *****************");
    admin(AdminRequest::Shutdown, cluster[&0]);

    
    thread::sleep(Duration::from_secs(25));

    log::warn!("***************** SENDING *****************");
    let mut handles = Vec::new();
    for thread in 0..1 {
        let cluster_clone = cluster.clone();
        handles.push(thread::spawn(move || {
            for i in 1..=1000 {
                let x_val = i * (thread + 1);
                async_tcp::apply_command(
                    CommandRequest { command: format!("SET X {x_val}") },
                    rand::thread_rng().gen_range(0..n),
                    &cluster_clone,
                )
                .unwrap();
            }
        }));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }

    log::warn!("***************** VERIFYING *****************");
    let result =
        async_tcp::apply_command(CommandRequest { command: "GET X".to_string() }, 0, &cluster)
            .unwrap();

    println!("======= result: {result} =======");

    log::warn!("***************** TERMINATING *****************");
    thread::sleep(Duration::from_secs(5));
}
