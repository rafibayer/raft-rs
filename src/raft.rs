pub type NodeID = usize;

#[derive(PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader
}

#[derive(PartialEq, Eq, Debug)]
pub enum RoleTransition {
    Follower{term: usize},
    Candidate,
    Leader
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub command: String,
    pub term: usize,
}

#[derive(Debug)]
pub struct CommandRequest {
    pub command: String,
}

#[derive(Debug)]
pub enum NetworkMessage {
    // client requests
    CommandRequest(CommandRequest),

    // internal
    LogRequest(LogRequest),
    LogResponse(LogResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
}

#[derive(Debug)]
pub struct LogRequest {
    pub leader_id: NodeID,
    pub term: usize,
    pub prefix_lenth: usize,
    pub prefix_term: usize,
    pub leader_commit: usize,
    pub suffix: Vec<LogEntry>,
}

#[derive(Debug)]
pub struct LogResponse {
    pub sender: NodeID,
    pub term: usize,
    pub ack: usize,
    pub success: bool,
}

#[derive(Debug)]
pub struct VoteRequest {
    pub sender: NodeID,
    pub term: usize,
    pub log_length: usize,
    pub last_log_term: usize
}

#[derive(Debug)]
pub struct VoteResponse {
    pub voter: NodeID,
    pub term: usize,
    pub granted: bool
}




