mod networking;
mod node;
mod raft;
mod state;
mod utils;

#[cfg(test)]
mod test;

use std::{collections::HashMap, thread, time::Duration, fs};

use raft::{CommandRequest, NodeID};

use simple_logger::SimpleLogger;

use node::Node;

use crate::{networking::client::Client, raft::AdminRequest, node::config::Config};

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    node: NodeID,

    #[clap(short, long)]
    config: String,
}

fn main() {
    let args = Args::parse();
    let config = serde_json::from_str::<Config>(&fs::read_to_string(args.config).unwrap()).unwrap();
    SimpleLogger::new().with_level(config.log_level.into()).init().unwrap();

    let node = Node::new(args.node, config, HashMap::new());

    node.start();
}
