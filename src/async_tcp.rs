use std::{
    collections::HashMap,
    error::Error,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::Duration,
};

use crate::{
    node::ClientConnection,
    raft::{AdminRequest, CommandRequest, NodeID, RaftRequest},
};

// todo:
/// client should initially pick a random node,
/// and if connection fails or we get NotLeader,
/// we should then cache the leader and re-use.
/// on failure, we repeat
pub fn apply_command(
    command: CommandRequest,
    node: NodeID,
    cluster: &HashMap<NodeID, SocketAddr>,
) -> Result<String, Box<dyn Error>> {
    let address = cluster[&node];
    let mut stream = connect_with_retries(address, Duration::from_millis(500), 10).unwrap();

    send(&RaftRequest::CommandRequest(command.clone()), &mut stream)?;

    let response = read(&mut stream)?;

    stream.shutdown(std::net::Shutdown::Both)?;
    drop(stream);

    if let RaftRequest::CommandResponse(response) = response {
        match response {
            crate::raft::CommandResponse::Result(result) => Ok(result),
            crate::raft::CommandResponse::NotLeader(leader) => {
                log::info!("{node} forwarding command to {leader}");
                apply_command(command, leader, cluster)
            }
            crate::raft::CommandResponse::Unavailable => todo!(),
            crate::raft::CommandResponse::Failed => todo!(),
        }
    } else {
        log::error!("Client receive an unexpected message type: {response:?}");
        Err("Client receive an unexpected message type".into())
    }
}

pub fn admin(request: AdminRequest, address: SocketAddr) {
    let mut stream = connect_with_retries(address, Duration::from_millis(500), 10).unwrap();
    send(&RaftRequest::AdminRequest(request), &mut stream).unwrap();
}

fn send(request: &RaftRequest, stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let serialized = bincode::serialize(&request)?;
    let size = serialized.len();

    stream.write_all(&size.to_ne_bytes())?;
    stream.flush()?;

    stream.write_all(&serialized)?;
    stream.flush()?;

    Ok(())
}

fn read(stream: &mut TcpStream) -> Result<RaftRequest, Box<dyn Error>> {
    let mut size_buf = [0; 8];
    stream.read_exact(&mut size_buf)?;

    let content_length: usize = usize::from_ne_bytes(size_buf);
    let mut buffer = vec![0; content_length];

    stream.read_exact(&mut buffer)?;
    Ok(bincode::deserialize(&buffer)?)
}

pub fn inbox_thread(
    node: NodeID,
    address: SocketAddr,
    incoming: Sender<(RaftRequest, Option<ClientConnection>)>,
    shutdown: Receiver<()>,
) {
    let listener = TcpListener::bind(address).unwrap();
    listener.set_nonblocking(true).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let incoming_clone = incoming.clone();
                thread::spawn(move || {
                    stream.set_nonblocking(false).unwrap();
                    handle_connection(stream, incoming_clone).unwrap();
                });
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // pass?
            }
            Err(e) => panic!("encountered IO error: {}", e),
        }
        if shutdown.try_recv().is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    log::error!("inbox thread terminating!!!");
}

fn handle_connection(
    mut stream: TcpStream,
    incoming: Sender<(RaftRequest, Option<ClientConnection>)>,
) -> Result<(), Box<dyn Error>> {
    loop {
        let msg = read(&mut stream)?;
        match msg {
            // client commands must be answered sync
            RaftRequest::CommandRequest(_) => {
                let (tx, rx) = mpsc::channel();
                incoming.send((msg, Some(tx)))?;
                let response = rx.recv()?;
                send(&RaftRequest::CommandResponse(response), &mut stream)?;
                drop(stream);

                // we don't keep client connections open, we can therefore return this thread.
                return Ok(());
            }
            RaftRequest::AdminRequest(_) => {
                incoming.send((msg, None))?;
                drop(stream);

                // we don't keep admin connections open, we can therefore return this thread.
                return Ok(());
            }
            _ => incoming.send((msg, None))?,
        };
    }
}

pub fn outbox_thread(
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

        // todo: if this fails bc of connection we need to reconnect instead of hammering
        // this closed stream. need some logic to remove connection/reconnect
        let mut stream = connections.get_mut(&node).unwrap();
        if let Err(err) = send(&req, &mut stream) {
            log::trace!("Node {id} outbox: Error sending message to {node}: {err:?}");
        }
    }

    log::error!("outbox thread terminating!!!");
}

fn connect_with_retries(
    address: SocketAddr,
    delay: Duration,
    attempts: usize,
) -> Result<TcpStream, String> {
    for i in 0..attempts {
        if let Ok(stream) = TcpStream::connect(address) {
            return Ok(stream);
        }

        log::warn!(
            "failed to connect to {address:?} after {} attemps. Retrying in {delay:?}",
            i + 1
        );
        thread::sleep(delay);
    }

    Err(format!("Failed to connect to {address:?} after {attempts} attempts"))
}
