use core::raft::{AdminRequest, CommandRequest, NodeID};
use std::error::Error;

use clap::{Parser, Subcommand};

use crate::lib::Client;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct ClientArgs {
    #[clap(short, long)]
    pub config: Option<String>,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    Execute { command: String },
    Shutdown { node: NodeID },
    BecomeLeader { node: NodeID },
    BecomeFollower { node: NodeID },
    GetLeader { node: NodeID },
}

impl Command {
    pub fn execute(self, client: &mut Client) -> Result<String, Box<dyn Error>> {
        match self {
            Command::Execute { command } => {
                Ok(format!("{:?}", client.apply_command(CommandRequest { command: command.as_bytes().to_vec() })?))
            }
            Command::Shutdown { node } => {
                Ok(format!("{:?}", client.admin(node, AdminRequest::Shutdown)?))
            }
            Command::BecomeLeader { node } => {
                Ok(format!("{:?}", client.admin(node, AdminRequest::BecomeLeader)?))
            }
            Command::BecomeFollower { node } => {
                Ok(format!("{:?}", client.admin(node, AdminRequest::BecomeFollower)?))
            }
            Command::GetLeader { node } => {
                Ok(format!("{:?}", client.admin(node, AdminRequest::GetLeader)?))
            }
        }
    }
}
