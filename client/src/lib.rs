use std::{
    collections::HashMap,
    error,
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use core::{
    raft::{AdminRequest, AdminResponse, CommandRequest, CommandResponse, NodeID, RaftRequest},
    utils::RetryOptions,
};

use core::networking::tcp;

const CLIENT_RETRY: &'static RetryOptions =
    &RetryOptions { attempts: 3, delay: Duration::from_millis(500) };

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
                log::trace!("Client trying to connect to cached leader: {leader}");
                let stream = tcp::connect_with_retries(self.cluster[&leader], CLIENT_RETRY);

                match stream {
                    Ok(stream) => stream,
                    Err(_) => {
                        self.cached_leader = None;
                        self.connect_to_any_node()?
                    }
                }
            }
            None => self.connect_to_any_node()?,
        };

        tcp::send(&RaftRequest::CommandRequest(command.clone()), &mut stream)?;
        let response = tcp::read(&mut stream)?;

        if let RaftRequest::CommandResponse(response) = response {
            return match response {
                CommandResponse::Result(_) => Ok(response),
                CommandResponse::NotLeader(leader) => {
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

    pub fn admin(
        &self,
        node: NodeID,
        request: AdminRequest,
    ) -> Result<AdminResponse, Box<dyn error::Error>> {
        let mut stream = tcp::connect_with_retries(self.cluster[&node], CLIENT_RETRY)?;

        tcp::send(&RaftRequest::AdminRequest(request), &mut stream)?;
        let response = tcp::read(&mut stream)?;
        match response {
            RaftRequest::AdminResponse(response) => Ok(response),
            other => {
                Err(format!("Received unexpected response to admin request: {other:?}").into())
            }
        }
    }

    fn connect_to_any_node(&self) -> Result<TcpStream, Box<dyn error::Error>> {
        for (node, address) in &self.cluster {
            let conn = tcp::connect_with_retries(*address, CLIENT_RETRY);
            if let Ok(conn) = conn {
                return Ok(conn);
            }

            log::warn!("Client failed to connect to {node} after 3 attempts!");
        }

        Err("Could not connect to any node".to_string().into())
    }
}
