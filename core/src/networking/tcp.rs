use std::{
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use crate::{
    raft::RaftRequest,
    utils::{self, RetryOptions},
};

pub fn send(request: &RaftRequest, stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let serialized = bincode::serialize(&request)?;
    let size = serialized.len();

    stream.write_all(&size.to_ne_bytes())?;
    stream.flush()?;

    stream.write_all(&serialized)?;
    stream.flush()?;

    Ok(())
}

pub fn read(stream: &mut TcpStream) -> Result<RaftRequest, Box<dyn Error>> {
    let mut size_buf = [0; 8];
    stream.read_exact(&mut size_buf)?;

    let content_length: usize = usize::from_ne_bytes(size_buf);
    let mut buffer = vec![0; content_length];

    stream.read_exact(&mut buffer)?;
    Ok(bincode::deserialize(&buffer)?)
}

pub fn connect_with_retries(
    address: SocketAddr,
    options: &RetryOptions,
) -> Result<TcpStream, String> {
    utils::retry(|| TcpStream::connect(address), options)
        .or_else(|_| Err(format!("Failed to connect to {address:?}")))
}
