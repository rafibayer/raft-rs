use serde::{Serialize, Deserialize};

pub type NodeID = usize;

#[derive(PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub command: String,
    pub term: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRequest {
    CommandRequest(CommandRequest),
    CommandResponse(CommandResponse),
    LogRequest(LogRequest),
    LogResponse(LogResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
    pub sender: NodeID, // needed?
    pub command: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResponse {
    pub sender: Option<NodeID>, // needed?
    pub reply: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRequest {
    pub sender: NodeID,
    pub term: usize,
    pub prefix_lenth: usize,
    pub prefix_term: usize,
    pub leader_commit: usize,
    pub suffix: Vec<LogEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogResponse {
    pub sender: NodeID,
    pub term: usize,
    pub ack: usize,
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteRequest {
    pub sender: NodeID,
    pub term: usize,
    pub log_length: usize,
    pub last_log_term: usize
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteResponse {
    pub sender: NodeID,
    pub term: usize,
    pub granted: bool
}