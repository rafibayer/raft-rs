use serde::{Deserialize, Serialize};

pub type NodeID = usize;

#[derive(PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub command: String,
    pub term: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RaftRequest {
    // Client interaction
    CommandRequest(CommandRequest),
    CommandResponse(CommandResponse),

    // Internal
    LogRequest(LogRequest),
    LogResponse(LogResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),

    // Admin
    AdminRequest(AdminRequest),
    AdminResponse(AdminResponse),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandRequest {
    pub command: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CommandResponse {
    Result(String),
    NotLeader(NodeID),
    Unavailable,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogRequest {
    pub sender: NodeID,
    pub term: usize,
    pub prefix_lenth: usize,
    pub prefix_term: usize,
    pub leader_commit: usize,
    pub suffix: Vec<LogEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogResponse {
    pub sender: NodeID,
    pub term: usize,
    pub ack: usize,
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VoteRequest {
    pub sender: NodeID,
    pub term: usize,
    pub log_length: usize,
    pub last_log_term: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VoteResponse {
    pub sender: NodeID,
    pub term: usize,
    pub granted: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AdminRequest {
    Shutdown,
    BecomeLeader,
    BecomeFollower,
    BecomeCandidate,

    GetLeader,
    GetLogLength,
    GetTerm,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AdminResponse {
    /// generic "Done" response for admin action
    Done,
    
    // query admin request responses
    Leader(Option<NodeID>),
    LogLength(usize),
    Term(usize),
}


#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum Event {
    ShutdownCompleted,
    BecameLeader,
    BecameCandidate,
    BecameFollower,
    CommittedLog(usize),
}
