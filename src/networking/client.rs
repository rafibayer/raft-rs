use std::{
    collections::HashMap,
    error,
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use crate::raft::{AdminRequest, CommandRequest, CommandResponse, NodeID, RaftRequest};

use super::async_tcp;

pub struct Client {
    cached_leader: Option<NodeID>,
    cluster: HashMap<NodeID, SocketAddr>,
}

impl Client {
    pub fn new(cluster: HashMap<NodeID, SocketAddr>) -> Self {
        Client { cached_leader: None, cluster }
    }

    pub fn apply_command(
        &mut self,
        command: CommandRequest,
    ) -> Result<CommandResponse, Box<dyn error::Error>> {
        let mut stream = match self.cached_leader {
            Some(leader) => {
                log::info!("Client trying to connect to cached leader: {leader}");
                let stream = async_tcp::connect_with_retries(
                    self.cluster[&leader],
                    Duration::from_millis(100),
                    3,
                );
                match stream {
                    Ok(stream) => stream,
                    Err(_) => self.connect_to_any_node()?,
                }
            }
            None => self.connect_to_any_node()?,
        };

        async_tcp::send(&RaftRequest::CommandRequest(command.clone()), &mut stream)?;
        let response = async_tcp::read(&mut stream)?;

        if let RaftRequest::CommandResponse(response) = response {
            return match response {
                CommandResponse::Result(_) => Ok(response),
                crate::raft::CommandResponse::NotLeader(leader) => {
                    log::info!("Client received NotLeader, trying leader: {leader}");
                    self.cached_leader = Some(leader);
                    return self.apply_command(command);
                }
                CommandResponse::Unavailable => todo!(),
                CommandResponse::Failed => todo!(),
            };
        }
        log::error!("Client receive an unexpected message type: {response:?}");
        Err("Client receive an unexpected message type".into())
    }

    pub fn admin(&self, node: NodeID, request: AdminRequest) -> Result<(), Box<dyn error::Error>> {
        let mut stream =
            async_tcp::connect_with_retries(self.cluster[&node], Duration::from_millis(100), 10)?;
        async_tcp::send(&RaftRequest::AdminRequest(request), &mut stream)?;
        Ok(())
    }

    fn connect_to_any_node(&self) -> Result<TcpStream, Box<dyn error::Error>> {
        for (node, address) in &self.cluster {
            let conn = async_tcp::connect_with_retries(*address, Duration::from_millis(50), 2);
            if let Ok(conn) = conn {
                return Ok(conn);
            }

            log::warn!("Client failed to connect to {node} after 2 attempts!");
        }

        Err("Could not connect to any node".to_string().into())
    }
}
