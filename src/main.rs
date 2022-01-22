mod raft;
mod state;
mod node;
mod utils;
mod async_tcp;

// thoughts: probably need to reset/stop election timer in more places.
// I think we are assuming that only follower receives heartbeats
// but candidate does to?? prob rewatch video/other impl

#[cfg(test)]
mod test;

use std::{thread, time::{Duration, Instant}, collections::{HashSet, HashMap}, net::{SocketAddr, SocketAddrV4, TcpListener, TcpStream}, fmt::Display, io::{Read, Write}};

use raft::{VoteRequest, VoteResponse};
use simple_logger::SimpleLogger;

use node::Node;

use crate::raft::RaftRequest;

fn main() {

    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let port = 7878;
    let n = 43;

    let mut cluster = HashMap::new();

    for i in 0..n {
        cluster.insert(i,  format!("127.0.0.1:{}", port + i).parse().unwrap());
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