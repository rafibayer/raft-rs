use std::{net::{TcpStream, TcpListener, SocketAddr}, thread, sync::mpsc::{Sender, Receiver}, io::{Read, Write}, collections::HashMap};

use log::info;

use crate::raft::{RaftRequest, NodeID};

pub fn incoming_listener(address: SocketAddr, incoming: Sender<RaftRequest>) {
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

fn handle_connection(mut stream: TcpStream,incoming: Sender<RaftRequest>) {
    loop {
        let mut size_buf = [0; 8];
        stream.read_exact(&mut size_buf).unwrap();

        let content_length: usize = usize::from_ne_bytes(size_buf);
        let mut buffer = vec![0; content_length];

        stream.read_exact(&mut buffer).unwrap();
        let message = bincode::deserialize(&buffer).unwrap();
        incoming.send(message).unwrap();
    }
}

pub fn outgoing_pusher(id: NodeID, outgoing: Receiver<(RaftRequest, NodeID)>, nodes: HashMap<NodeID, SocketAddr>) {
    let connections = nodes.into_iter().map(|(node, addr)| {
        (node, TcpStream::connect(addr).unwrap())
    }).collect::<HashMap<NodeID, TcpStream>>();
    
    for (req, node) in outgoing.into_iter() {
        assert_ne!(node, id);

        let serialized = bincode::serialize(&req).unwrap();
        let size = serialized.len();

        let mut stream = &connections[&node];
        stream.write_all(&size.to_ne_bytes()).unwrap();
        stream.flush().unwrap();

        stream.write_all(&serialized).unwrap();
        stream.flush().unwrap();
    }

    log::error!("pusher thread terminating!!!");
}