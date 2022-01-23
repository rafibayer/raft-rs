use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender, self},
    thread,
};

use crate::{raft::{NodeID, RaftRequest, CommandRequest}, node::ClientConnection};

pub fn apply_command(command: CommandRequest, node: NodeID, cluster: &HashMap<NodeID, SocketAddr>) -> Result<String, String> {
    let mut stream = TcpStream::connect(cluster[&node]).unwrap();

    send(&RaftRequest::CommandRequest(command.clone()), &mut stream);

    let response = read(&mut stream);

    drop(stream);

    if let RaftRequest::CommandResponse(response) = response {
        match response {
            crate::raft::CommandResponse::Result(result) => Ok(result),
            crate::raft::CommandResponse::NotLeader(leader) => {
                log::info!("{node} forwarding command to {leader}");
                apply_command(command, leader, cluster)
            },
            crate::raft::CommandResponse::Unavailable => todo!(),
            crate::raft::CommandResponse::Failed => todo!(),
        }
    } else {
        log::error!("Client receive an unexpected message type: {response:?}");
        return Err(String::from("Client receive an unexpected message type"))
    }
}

fn send(request: &RaftRequest, stream: &mut TcpStream) {
    let serialized = bincode::serialize(&request).unwrap();
    let size = serialized.len();

    stream.write_all(&size.to_ne_bytes()).unwrap();
    stream.flush().unwrap();

    stream.write_all(&serialized).unwrap();
    stream.flush().unwrap();
}

fn read(stream: &mut TcpStream) -> RaftRequest {
    let mut size_buf = [0; 8];
    stream.read_exact(&mut size_buf).unwrap();

    let content_length: usize = usize::from_ne_bytes(size_buf);
    let mut buffer = vec![0; content_length];

    stream.read_exact(&mut buffer).unwrap();
    bincode::deserialize(&buffer).unwrap()
}

pub fn incoming_listener(address: SocketAddr, incoming: Sender<(RaftRequest, Option<ClientConnection>)>) {
    let listener = TcpListener::bind(address).unwrap();
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let incoming_clone = incoming.clone();
        thread::spawn(move || {
            handle_connection(stream, incoming_clone);
        });
    }

    log::error!("listener thread terminating!!!");
}

fn handle_connection(mut stream: TcpStream, incoming: Sender<(RaftRequest, Option<ClientConnection>)>) {
    loop {
        let msg = read(&mut stream);
        match msg {
            // client commands must be answered sync 
            RaftRequest::CommandRequest(_) => {
                let (tx, rx) = mpsc::channel();
                incoming.send((msg, Some(tx))).unwrap();
                let response = rx.recv().unwrap();
                send(&RaftRequest::CommandResponse(response), &mut stream);

                // we don't keep client connections open, we can therefore return this thread.
                return;
            },
            _ => incoming.send((msg, None)).unwrap()
        };
    }
}

pub fn outgoing_pusher(
    id: NodeID,
    outgoing: Receiver<(RaftRequest, NodeID)>,
    nodes: HashMap<NodeID, SocketAddr>,
) {
    let mut connections = nodes
        .into_iter()
        .map(|(node, addr)| (node, TcpStream::connect(addr).unwrap()))
        .collect::<HashMap<NodeID, TcpStream>>();

    for (req, node) in outgoing.into_iter() {
        assert_ne!(node, id);
        send(&req, connections.get_mut(&node).unwrap());
    }

    log::error!("pusher thread terminating!!!");
}
