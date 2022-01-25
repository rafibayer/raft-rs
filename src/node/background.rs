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
    networking::async_tcp,
    raft::{NodeID, RaftRequest},
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
        let msg = async_tcp::read(&mut stream)?;
        match msg {
            // client commands must be answered sync
            RaftRequest::CommandRequest(_) => {
                let (tx, rx) = mpsc::channel();
                incoming.send((msg, Some(tx)))?;
                let response = rx.recv()?;
                if let RaftRequest::CommandResponse(response) = response {
                    async_tcp::send(&RaftRequest::CommandResponse(response), &mut stream)?;
                    drop(stream);

                    // we don't keep client connections open, we can therefore return this thread.
                    return Ok(());
                }

                log::error!("Command request received an unexpected response type: {response:?}");
            }
            // admin commands must be answered sync
            RaftRequest::AdminRequest(_) => {
                let (tx, rx) = mpsc::channel();
                incoming.send((msg, Some(tx)))?;
                let response = rx.recv()?;
                if let RaftRequest::AdminResponse(response) = response {
                    async_tcp::send(&RaftRequest::AdminResponse(response), &mut stream)?;
                    drop(stream);

                    // we don't keep client connections open, we can therefore return this thread.
                    return Ok(());
                }

                log::error!("Admin request received an unexpected response type: {response:?}");

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
        let stream = connections.get_mut(&node).unwrap();
        if let Err(err) = async_tcp::send(&req, stream) {
            log::trace!("Node {id} outbox: Error sending message to {node}: {err:?}");
        }
    }

    log::error!("Node {id}: outbox thread terminating!!!");
}
