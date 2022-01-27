use std::{
    collections::HashMap,
    error::Error,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::Duration,
};

use crate::{
    networking::tcp,
    raft::{NodeID, RaftRequest}, utils::RetryOptions,
};

use super::SyncConnection;

pub fn inbox_thread(
    id: NodeID,
    address: SocketAddr,
    incoming: Sender<(RaftRequest, Option<SyncConnection>)>,
    shutdown_signal: Receiver<()>,
) {
    let listener = TcpListener::bind(address).unwrap();
    listener.set_nonblocking(true).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let incoming_clone = incoming.clone();
                thread::spawn(move || {
                    stream.set_nonblocking(false).unwrap();
                    if let Err(err) = handle_connection(stream, incoming_clone) {
                        log::error!("node {id} failed to handle a connection {err:?}");
                    }
                });
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // pass?
            }
            Err(e) => panic!("{id} encountered listener error: {}", e),
        }
        if shutdown_signal.try_recv().is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    log::error!("Node {id} inbox thread terminating!!!");
}

fn handle_connection(
    mut stream: TcpStream,
    incoming: Sender<(RaftRequest, Option<SyncConnection>)>,
) -> Result<(), Box<dyn Error>> {
    loop {
        let msg = tcp::read(&mut stream)?;
        match msg {
            // client commands must be answered sync
            RaftRequest::CommandRequest(_) | RaftRequest::AdminRequest(_) => {
                let (tx, rx) = mpsc::channel();
                incoming.send((msg, Some(tx)))?;
                let response = rx.recv()?;
                tcp::send(&response, &mut stream)?;
                drop(stream);

                // we don't keep client connections open, we can therefore return this thread.
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
    let mut connections = HashMap::new();

    for (req, node) in outgoing.into_iter() {
        assert_ne!(node, id);

        match get_conn(node, &nodes, &mut connections) {
            Ok(stream) => {
                if let Err(err) = tcp::send(&req, stream) {
                    log::trace!("Node {id} outbox: Error sending message to {node}: {err:?}");
                    connections.remove(&node); // will force us to reconnect next iteration
                }
            }
            Err(err) => log::warn!("{id} failed to connect to {node}: {err:?}"),
        }
    }

    log::error!("Node {id}: outbox thread terminating!!!");
}

fn get_conn<'a>(node: NodeID, addrs: &HashMap<NodeID, SocketAddr>, conns: &'a mut HashMap<NodeID, TcpStream>) -> Result<&'a mut TcpStream, Box<dyn Error>> {
    match conns.contains_key(&node) {
        true => conns.get_mut(&node).ok_or_else(|| panic!()),
        false => {
            conns.insert(node, tcp::connect_with_retries(addrs[&node], &RetryOptions {
                attempts: 3,
                delay: Duration::from_millis(50),
            })?);
            conns.get_mut(&node).ok_or_else(|| panic!())
        },
    }
}
