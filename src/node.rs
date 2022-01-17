use rand::prelude::*;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{mpsc, Arc};

use crate::raft::{
    CommandRequest, LogEntry, LogRequest, LogResponse, NetworkMessage, NodeID, Role, VoteRequest,
    VoteResponse, RoleTransition,
};
use crate::state::StateMachine;
use crate::transport::Transport;
use crate::utils::get_time_ms;

// should be long, allow time for elections to reach all nodes
const ELECTION_TIMEOUT_MS: std::ops::RangeInclusive<u128> = 1000..=5000;

// should be very low, need establish authority and stop other elections
const HEARTBEAT_INTERVAL_MS: u128 = 50;

pub struct Node<S: StateMachine + 'static, T: Transport + Send + Sync + 'static> {
    // stable
    pub id: NodeID,
    pub current_term: usize,
    pub voted_for: Option<NodeID>,
    pub log: Vec<LogEntry>,
    pub commit_length: usize,

    // memory
    pub current_role: Role,
    pub current_leader: Option<NodeID>,
    pub votes_received: HashSet<NodeID>,
    pub sent_length: HashMap<NodeID, usize>,
    pub acked_length: HashMap<NodeID, usize>,

    // implementation details
    t_heartbeat_received: u128,
    t_heartbeat_sent: u128,
    election_timeout: u128,
    t_election_start: u128,
    to_node_sender: SyncSender<NetworkMessage>,
    to_node_receiver: Receiver<NetworkMessage>,
    nodes: HashSet<NodeID>,
    transport: Option<Arc<T>>,
    pub state: S,
}

impl<S: StateMachine, T: Transport + Send + Sync + 'static> Node<S, T> {
    pub fn new(id: usize, nodes: HashSet<NodeID>, state: S) -> Self {
        let (rx, tx) = mpsc::sync_channel(100_000);
        log::info!("[Node {}]Initializing node", id);
        Node {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_length: 0,

            current_role: Role::Follower,
            current_leader: None,
            votes_received: HashSet::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),

            // implementation details
            t_heartbeat_received: get_time_ms(),
            t_heartbeat_sent: 0, // 0?
            election_timeout: rand::thread_rng().gen_range(ELECTION_TIMEOUT_MS), // 0? since we set on become follower
            t_election_start: 0,                                                 // 0?
            to_node_sender: rx,
            to_node_receiver: tx,
            nodes,
            transport: None,
            state,
        }
    }

    pub fn start(&mut self, transport: Arc<T>) {
        log::info!("{} starting...", self.dbg());
        self.transport = Some(transport);
        self.node_loop();
    }

    fn node_loop(&mut self) -> ! {
        loop {


            // process messages and check for transitions
            let next_role = if let Some(transition) = self.process_next_message() {
                Some(transition)
            // perform role if we haven't transitioned due to a message
            } else {
                match self.current_role {
                    Role::Follower => self.follower(),
                    Role::Candidate => self.candidate(),
                    Role::Leader => self.leader(),
                }
            };

            // transition if needed
            if let Some(transition) = next_role {
                match transition {
                    RoleTransition::Follower { term } => self.become_follower(term),
                    RoleTransition::Candidate => self.become_candidate(),
                    RoleTransition::Leader => self.become_leader(),
                };
            }
        }
    }

    fn follower(&mut self) -> Option<RoleTransition> {
        // check for heartbeat timeout
        if get_time_ms() > self.t_heartbeat_received + self.election_timeout {
            log::warn!("{} has not received heartbeat, becoming candidate", self.dbg());
            return Some(RoleTransition::Candidate);
        }

        None
    }

    fn candidate(&mut self) -> Option<RoleTransition> {
        // check for election timeout
        if get_time_ms() > self.t_election_start + self.election_timeout {
            log::warn!("{} election timeout reached, restarting election", self.dbg());
            return Some(RoleTransition::Candidate);
        }

        None
    }

    fn leader(&mut self) -> Option<RoleTransition> {
        if get_time_ms() > self.t_heartbeat_sent + HEARTBEAT_INTERVAL_MS {
            log::info!("{} heartbeat timeout, sending heartbeat", self.dbg());
            self.send_heartbeat();
        }

        None
    }

    fn become_follower(&mut self, term: usize) {
        self.current_role = Role::Follower;
        self.voted_for = None;
        self.current_term = term;
        self.election_timeout = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MS);
    }

    fn become_candidate(&mut self) {
        self.current_role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.current_term += 1;
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.t_election_start = get_time_ms();

        self.request_votes();
    }

    fn become_leader(&mut self) {
        self.current_role = Role::Leader;
        self.current_leader = Some(self.id);

        // replicate logs to other nodes
        for follower in &self.nodes {
            if *follower != self.id {
                self.sent_length.insert(*follower, self.log.len());
                self.acked_length.insert(*follower, 0);
                self.replicate_log(follower);
            }
        }
    }

    fn request_votes(&mut self) {
        let mut last_term = 0;
        if !self.log.is_empty() {
            last_term = self.log[self.log.len() - 1].term;
        }

        for node in &self.nodes {
            if *node != self.id {
                let request = VoteRequest {
                    sender: self.id,
                    term: self.current_term,
                    log_length: self.log.len(),
                    last_log_term: last_term,
                };
                self.send_message(*node, NetworkMessage::VoteRequest(request));
            }
        }
    }

    pub fn receive_vote_request(&mut self, request: VoteRequest) -> Option<RoleTransition> {
        log::info!("{} received vote request from Node {}", self.dbg(), request.sender);

        let mut should_become_follower = false;

        // if we see a higher term, step down
        if request.term > self.current_term {
            log::error!(
                "{} Found higher term: current_term = {} but node {} had term {}",
                self.dbg(),
                self.current_term,
                request.sender,
                request.term
            );
            // have to set term here since it is used in request validation
            self.current_term = request.term;
            should_become_follower = true;
        }

        let mut last_term = 0;
        if !self.log.is_empty() {
            last_term = self.log[self.log.len() - 1].term;
        }

        // determine if requestors log is healthy
        let log_ok = (request.last_log_term > last_term)
            || (request.last_log_term == last_term && request.log_length >= self.log.len());

        // if the requestors term is current, log is healthy, and we haven't voted yet, vote yes
        if request.term == self.current_term && log_ok && self.voted_for == None {
            log::info!("{} voting for Node {}", self.dbg(), request.sender);

            self.voted_for = Some(request.sender);
            self.send_message(
                request.sender,
                NetworkMessage::VoteResponse(VoteResponse {
                    voter: self.id,
                    term: self.current_term,
                    granted: true,
                }),
            );
        } else {
            log::warn!(
                "{} will not vote for Node {}
                request.term = {}, current_term = {}, log_ok = {}, vote_for = {:?}",
                self.dbg(),
                request.sender,
                request.term,
                self.current_term,
                log_ok,
                self.voted_for
            );

            self.send_message(
                request.sender,
                NetworkMessage::VoteResponse(VoteResponse {
                    voter: self.id,
                    term: self.current_term,
                    granted: false,
                }),
            );
        }

        if should_become_follower {
            // we already fixed term above
            return Some(RoleTransition::Follower{term: self.current_term});
        }

        None
    }

    pub fn receive_vote_response(&mut self, response: VoteResponse) -> Option<RoleTransition> {
        log::info!("{} received vote response from Node {}", self.dbg(), response.voter);

        if self.current_role == Role::Candidate
            && response.term == self.current_term
            && response.granted
        {
            log::info!("{} received granted vote from {}", self.dbg(), response.voter);

            self.votes_received.insert(response.voter);

            // check for quorum
            if self.votes_received.len() >= (self.nodes.len() + 1) / 2 {
                log::info!(
                    "{} ****** received a quorum with {} votes ******",
                    self.dbg(),
                    self.votes_received.len()
                );

                // replicate log, assume leader role
                for follower in &self.nodes {
                    if *follower != self.id {
                        self.sent_length.insert(*follower, self.log.len());
                        self.acked_length.insert(*follower, 0);
                        self.replicate_log(follower);
                    }
                }

                return Some(RoleTransition::Leader);
            }
        } else if response.term > self.current_term {
            log::error!(
                "{} Stepping down. received a vote response with a higher term {} vs {}",
                self.dbg(),
                response.term,
                self.current_term
            );

            return Some(RoleTransition::Follower{term: response.term});
        }

        None
    }

    pub fn broadcast_request(&mut self, message: CommandRequest) -> Option<RoleTransition> {
        if self.current_role == Role::Leader {
            log::info!("{} received broadcast request as leader", self.dbg());
            self.log.push(LogEntry { command: message.command, term: self.current_term });
            self.acked_length.insert(self.id, self.log.len());

            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(follower);
                }
            }
        } else {
            log::info!("{} received broadcast request as follower, forwarding.", self.dbg());
            self.forward(self.current_leader, message);
        }

        None
    }

    fn send_heartbeat(&mut self) {
        self.t_heartbeat_sent = get_time_ms();
        for follower in &self.nodes {
            if *follower != self.id {
                self.replicate_log(follower);
            }
        }
    }

    fn replicate_log(&self, follower: &NodeID) {
        // prefix: all log entries we think we have sent to follower
        let prefix_len = self.sent_length[follower];

        // suffix: all log entries we have not yet sent to the follower
        let suffix = &self.log[prefix_len..];

        // term of last entry in prefix
        let mut prefix_term = 0;
        if prefix_len > 0 {
            prefix_term = self.log[prefix_len - 1].term;
        }

        self.send_message(
            *follower,
            NetworkMessage::LogRequest(LogRequest {
                leader_id: self.id,
                term: self.current_term,
                prefix_lenth: prefix_len,
                prefix_term,
                leader_commit: self.commit_length,
                suffix: suffix.to_vec(), // todo: expensive clone
            }),
        )
    }

    pub fn receive_log_request(&mut self, request: LogRequest) -> Option<RoleTransition> {
        let mut should_become_follower = false;

        self.t_heartbeat_received = get_time_ms();

        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None;
        }

        // above, we accepted current_term = term, so we always
        // fall through to this if-statement as well if
        // the first executed
        if request.term == self.current_term {
            // instead of transitioning immediately, finish processing log first
            should_become_follower = true;
            self.current_leader = Some(request.leader_id);
        }

        // check that we have the prefix that the sender is assuming we have
        // and last log term in prefix on follower = last log term on leader
        let log_ok = (self.log.len() >= request.prefix_lenth)
            && (request.prefix_lenth == 0
                || self.log[request.prefix_lenth - 1].term == request.prefix_term);

        if !log_ok {
            // if we short circuit on the && or ||, the call to self.log[request.prefix_lenth - 1] will panic
            log::warn!(
                "{} rejecting log, log.len={}, prefix_len={}, log{:?}, prefix_term={}",
                self.dbg(),
                self.log.len(),
                request.prefix_lenth,
                &self.log,
                request.prefix_term
            );
        }

        if request.term == self.current_term && log_ok {
            // if terms match and log is ok, append and ack success
            let ack = request.prefix_lenth + request.suffix.len();
            self.append_entries(request.prefix_lenth, request.leader_commit, request.suffix);
            self.send_message(
                request.leader_id,
                NetworkMessage::LogResponse(LogResponse {
                    sender: self.id,
                    term: self.current_term,
                    ack,
                    success: true,
                }),
            )
        } else {
            // otherwise ack error
            self.send_message(
                request.leader_id,
                NetworkMessage::LogResponse(LogResponse {
                    sender: self.id,
                    term: self.current_term,
                    ack: 0,
                    success: false,
                }),
            )
        }

        if should_become_follower {
            // self.current_term already updated above
            return Some(RoleTransition::Follower{term: self.current_term});
        }

        None
    }

    fn append_entries(&mut self, prefix_len: usize, leader_commit: usize, suffix: Vec<LogEntry>) {
        // check if we have anything to append
        if !suffix.is_empty() && self.log.len() > prefix_len {
            // last log entry we can compare between follower state and leader state.
            let index = min(self.log.len(), prefix_len + suffix.len()) - 1;

            // compare term numbers, if not same, we have an inconsistency.
            // must truncate the log where they diverge.
            // this is fine because they are not committed.
            if self.log[index].term != suffix[index - prefix_len].term {
                self.log = self.log[..prefix_len - 1].to_vec();
            }
        }

        // append new log entries
        if prefix_len + suffix.len() > self.log.len() {
            let take_start = self.log.len() - prefix_len;
            let take_n = (suffix.len()) - take_start; // take inclusive, needed to remove -1
            let entries = suffix.into_iter().skip(take_start).take(take_n);

            for entry in entries {
                self.log.push(entry);
            }
        }

        if leader_commit > self.commit_length {
            for i in self.commit_length..leader_commit - 1 {
                self.state.apply_command(&self.log[i].command).expect("err applying command");
            }

            self.commit_length = leader_commit;
        }
    }

    pub fn receive_log_response(&mut self, response: LogResponse) -> Option<RoleTransition> {
        if response.term == self.current_term && self.current_role == Role::Leader {
            // ensures ack > last ack, incase response re-ordered
            if response.success && response.ack >= self.acked_length[&response.sender] {
                self.sent_length.insert(response.sender, response.ack);
                self.acked_length.insert(response.sender, response.ack);
                self.commit_log_entries();
            } else if self.sent_length[&response.sender] > 0 {
                log::error!(
                    "{} follower failed to log: success={}, ack={} vs last ack={}",
                    self.dbg(),
                    response.success,
                    response.ack,
                    self.acked_length[&response.sender]
                );

                // if send fails, maybe gap in follower log.
                // decrement to try and shrink prefix, sending one more log
                // on next attempt.
                // if gap is large, this could take many iterations. (can be optimized)
                *self.sent_length.get_mut(&response.sender).unwrap() -= 1;
                self.replicate_log(&response.sender);
            }
        } else if response.term > self.current_term {
            // as usual, step down if we see higher term
            self.t_heartbeat_received = get_time_ms();
            return Some(RoleTransition::Follower{term: response.term});
        }

        None
    }

    fn commit_log_entries(&mut self) {
        while self.commit_length < self.log.len() {
            // count acks
            let mut acks = 0;
            for node in &self.nodes {
                if self.acked_length[node] > self.commit_length {
                    acks += 1;
                }
            }

            // check for quorum
            if acks >= (self.nodes.len() + 1) / 2 {
                self.state
                    .apply_command(&self.log[self.commit_length].command)
                    .expect("err applying command");

                self.commit_length += 1;
            } else {
                break;
            }
        }
    }

    fn send_message(&self, node: NodeID, message: NetworkMessage) {
        log::info!("{} sending message {:?} to Node {}", self.dbg(), message, node);
        self.transport.as_ref().unwrap().send(node, message);
    }

    pub fn get_sender(&self) -> SyncSender<NetworkMessage> {
        self.to_node_sender.clone()
    }

    fn get_next_message(&self) -> Option<NetworkMessage> {
        self.to_node_receiver.try_recv().ok()
    }

    // todo: this must be FIFO to preserve total ordering
    fn forward(&self, leader: Option<NodeID>, message: CommandRequest) {
        // todo: need to hold messages in queue until leader elected
        if leader.is_none() {
            log::warn!("No leader, discarding message {}", message.command);
            return;
        }

        log::info!("{} forwarding {:?} to leader {}", self.dbg(), message, leader.unwrap());

        // will fail if no leader, need queue to hold until leader elected and send in order
        self.transport
            .as_ref()
            .unwrap()
            .send(leader.unwrap(), NetworkMessage::CommandRequest(message));
    }


    fn process_next_message(&mut self) -> Option<RoleTransition> {
        
        match self.get_next_message() {
            Some(NetworkMessage::CommandRequest(command)) => self.broadcast_request(command),
            Some(NetworkMessage::VoteRequest(vote_req)) => self.receive_vote_request(vote_req),
            Some(NetworkMessage::VoteResponse(vote_resp)) => self.receive_vote_response(vote_resp),
            Some(NetworkMessage::LogRequest(log_req)) => self.receive_log_request(log_req),
            Some(NetworkMessage::LogResponse(log_resp)) => self.receive_log_response(log_resp),
            _ => None,
        } 
    }

    fn dbg(&self) -> String {
        format!("[Node {} | Term {} | {:?}]", self.id, self.current_term, self.current_role)
    }
}
