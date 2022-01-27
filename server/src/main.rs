use std::{collections::HashMap, fs};

use core::raft::NodeID;

use simple_logger::SimpleLogger;

use core::node::Node;

use core::node::config::Config;

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
