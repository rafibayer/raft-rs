use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc};
use std::thread;
use std::time::Duration;
use rand::prelude::*;

use crate::raft::{
    CommandRequest, LogEntry, LogRequest, LogResponse, NetworkMessage, NodeID, Role, VoteRequest,
    VoteResponse,
};
use crate::state::StateMachine;
use crate::transport::Transport;
use crate::utils;

use log;
use parking_lot::Mutex;

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
    election_running: bool,
    inbox: Arc<Mutex<VecDeque<NetworkMessage>>>,
    self_mutex: Option<Arc<Mutex<Node<S, T>>>>,
    nodes: HashSet<NodeID>,
    transport: Option<Arc<T>>,
    pub state: S,
    pub last_heartbeat_unix: u128,
}

impl<S: StateMachine, T: Transport + Send + Sync + 'static> Node<S, T> {
    pub fn new(id: usize, nodes: HashSet<NodeID>, state: S) -> Self {
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
            election_running: false,
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            self_mutex: None,
            nodes,
            transport: None,
            state,
            last_heartbeat_unix: utils::get_time_ms()
        }
    }

    pub fn start(&mut self, mutex: Arc<Mutex<Node<S, T>>>, transport: Arc<T>) {
        log::info!("[Node {}] starting...", self.id);

        self.transport = Some(transport);
        self.self_mutex = Some(mutex);
        self.start_election();
        self.start_inbox();
        self.start_heartbeat();
    }

    fn request_votes(&mut self) {
        log::info!("[Node {}] requesting votes", self.id);
        // become candidate, vote for self
        self.current_term += 1;
        self.current_role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        let mut last_term = 0;
        if self.log.len() > 0 {
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
                log::info!("[Node {}] requesting vote from Node {}", self.id, node);
                self.send_message(*node, NetworkMessage::VoteRequest(request));
            }
        }

        self.election_running = true;
    }

    pub fn receive_vote_request(&mut self, request: VoteRequest) {
        log::info!(
            "[Node {}] received vote request from Node {}",
            self.id,
            request.sender
        );

        // if we see a higher term, step down
        if request.term > self.current_term {
            log::error!(
                "[Node {}] Found higher term: current_term = {} but node {} had term {}",
                self.id,
                self.current_term,
                request.sender,
                request.term
            );
            self.current_term = request.term;
            self.current_role = Role::Follower;
            self.voted_for = None;
        }

        let mut last_term = 0;
        if self.log.len() > 0 {
            last_term = self.log[self.log.len() - 1].term;
        }

        // determine if requestors log is healthy
        let log_ok = (request.last_log_term > last_term)
            || (request.last_log_term == last_term && request.log_length >= self.log.len());

        // if the requestors term is current, log is healthy, and we haven't voted yet, vote yes
        if request.term == self.current_term && log_ok && self.voted_for == None {
            log::info!("[Node {}] voting for Node {}", self.id, request.sender);

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
                "[Node {}] will not vote for Node {}
                request.term = {}, current_term = {}, log_ok = {}, vote_for = {:?}",
                self.id,
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
    }

    pub fn receive_vote_response(&mut self, response: VoteResponse) {
        log::info!(
            "[Node {}] received vote response from Node {}",
            self.id,
            response.voter
        );

        if self.current_role == Role::Candidate
            && response.term == self.current_term
            && response.granted
        {
            log::info!(
                "[Node {}] received granted vote from {}",
                self.id,
                response.voter
            );

            self.votes_received.insert(response.voter);

            // check for quorum
            if self.votes_received.len() >= (self.nodes.len() + 1) / 2 {
                log::info!(
                    "[Node {}] ****** received a quorum with {} votes ******",
                    self.id,
                    self.votes_received.len()
                );

                self.current_role = Role::Leader;
                self.current_leader = Some(self.id);

                self.election_running = false;

                // replicate logs to other nodes
                for follower in &self.nodes {
                    if *follower != self.id {
                        self.sent_length.insert(*follower, self.log.len());
                        self.acked_length.insert(*follower, 0);

                        self.replicate_log(follower);
                    }
                }
            }
        } else if response.term > self.current_term {
            log::error!(
                "[Node {}] Stepping down. received a vote response with a higher term {} vs {}",
                self.id,
                response.term,
                self.current_term
            );

            // step down if we see a higher term
            self.current_term = response.term;
            self.current_role = Role::Follower;
            self.voted_for = None;

            self.election_running = false;
        }
    }

    pub fn broadcast_request(&mut self, message: CommandRequest) {
        if self.current_role == Role::Leader {
            log::info!("[Node {}] received broadcast request as leader", self.id);
            self.log.push(LogEntry {
                command: message.command,
                term: self.current_term,
            });
            self.acked_length.insert(self.id, self.log.len());

            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(follower);
                }
            }
        } else {
            log::info!("[Node {}] received broadcast request as follower, forwarding.", self.id);
            self.forward(self.current_leader, message);
        }
    }

    fn heartbeat(&mut self) {
        if self.current_role == Role::Leader {
            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(follower);
                }
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

    pub fn receive_log_request(&mut self, request: LogRequest) {
        self.last_heartbeat_unix = utils::get_time_ms();
        
        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None;

            self.election_running = false;
        }

        // above, we accepted current_term = term, so we always
        // fall through to this if-statement as well if
        // the first executed
        if request.term == self.current_term {
            self.current_role = Role::Follower;
            self.current_leader = Some(request.leader_id);
        }

        // check that we have the prefix that the sender is assuming we have
        // (or longer).
        // AND
        // prefix length is 0
        //      OR last log term in prefix on follower = last log term on leader

        let log_ok = (self.log.len() >= request.prefix_lenth)
            && (request.prefix_lenth == 0
                || self.log[request.prefix_lenth - 1].term == request.prefix_term);

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
    }

    fn append_entries(&mut self, prefix_len: usize, leader_commit: usize, suffix: Vec<LogEntry>) {
        // check if we have anything to append
        if suffix.len() > 0 && self.log.len() > prefix_len {
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
            // todo: sketchy
            let take_start = self.log.len() - prefix_len;
            let take_n = (suffix.len() - 1) - take_start;
            let entries = suffix.into_iter().skip(take_start).take(take_n);

            for entry in entries {
                self.log.push(entry);
            }
        }

        if leader_commit > self.commit_length {
            for i in self.commit_length..leader_commit - 1 {
                self.state
                    .apply_command(&self.log[i].command)
                    .expect("err applying command");
            }

            self.commit_length = leader_commit;
        }
    }

    pub fn recieve_log_response(&mut self, response: LogResponse) {
        if response.term == self.current_term && self.current_role == Role::Leader {
            // ensures ack > last ack, incase response re-ordered
            if response.success && response.ack >= self.acked_length[&response.sender] {
                self.sent_length.insert(response.sender, response.ack);
                self.acked_length.insert(response.sender, response.ack);

                self.commit_log_entries();
            } else if self.sent_length[&response.sender] > 0 {
                // if send fails, maybe gap in follower log.
                // decrement to try and shrink prefix, sending one more log
                // on next attempt.
                // if gap is large, this could take many iterations. (can be optimized)
                *self.sent_length.get_mut(&response.sender).unwrap() -= 1;
                self.replicate_log(&response.sender);
            }
        } else if response.term > self.current_term {
            // as usual, step down if we see higher term
            self.current_term = response.term;
            self.current_role = Role::Follower;
            self.voted_for = None;

            self.election_running = false;
        }
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
        log::info!("[Node {}] sending message {:?} to Node {}", self.id, message, node);
            self.transport.as_ref().unwrap().send(node, message);
    }

    pub fn get_inbox(&self) -> Arc<Mutex<VecDeque<NetworkMessage>>> {
        self.inbox.clone()
    }

    // todo: this must be FIFO to preserve total ordering
    fn forward(&self, leader: Option<NodeID>, message: CommandRequest) {
        if leader.is_none() {
            log::warn!("No leader, discarding message {}", message.command);
            return;
        }

        // will fail if no leader, need queue to hold until leader elected and send in order
        self.transport
            .as_ref()
            .unwrap()
            .send(leader.unwrap(), NetworkMessage::CommandRequest(message));
    }

    fn process_message(&mut self, message: NetworkMessage) {
        match message {
            NetworkMessage::CommandRequest(command) => self.broadcast_request(command),
            NetworkMessage::LogRequest(log_req) => self.receive_log_request(log_req),
            NetworkMessage::LogResponse(log_resp) => self.recieve_log_response(log_resp),
            NetworkMessage::VoteRequest(vote_req) => self.receive_vote_request(vote_req),
            NetworkMessage::VoteResponse(vote_resp) => self.receive_vote_response(vote_resp),
        }
    }

    fn start_election(&mut self) {
        Node::run_election_loop_thread(self.id, self.self_mutex.clone().unwrap());
    }

    fn start_inbox(&mut self) {
        Node::run_process_message_loop_thread(self.id, self.inbox.clone(), self.self_mutex.clone().unwrap())
    }

    fn start_heartbeat(&mut self) {
        Node::run_heartbeat_loop_thread(self.id, self.self_mutex.clone().unwrap());
    }

    fn run_election_loop_thread(id: NodeID, node: Arc<Mutex<Node<S, T>>>){
        
        let election_timeout_ms = rand::thread_rng().gen_range(500..1000u128); 

        thread::spawn(move || loop {

            if let Some(mut node_lock) = node.try_lock() {
                if node_lock.election_running {
                    log::info!("[Node {}] acquired lock in election loop", id);

                    if utils::get_time_ms() > node_lock.last_heartbeat_unix + election_timeout_ms {
                        node_lock.request_votes();
                    }
                    
                    log::info!("[Node {}] released lock in election loop", id);
                }
            }

            thread::sleep(Duration::from_millis(100));
        });
    }

    fn run_process_message_loop_thread(id: NodeID, inbox: Arc<Mutex<VecDeque<NetworkMessage>>>, node: Arc<Mutex<Node<S, T>>>) {
        // yield after at most this many messages
        let process_limit = 10; 
        
        thread::spawn(move || loop {
            let mut processed = 0;

            if let Some(mut node_lock) = node.try_lock() {
                log::info!("[Node {}] acquired lock in inbox loop", id);

                let mut inbox_lock = inbox.lock();
                log::info!("[Node {}] inbox length: {}", id, inbox_lock.len());
                
                while !inbox_lock.is_empty() && processed < process_limit {
                    node_lock.process_message(inbox_lock.pop_front().unwrap());
                    processed += 1;
                }

                log::info!("[Node {}] released lock in inbox loop after {} messages", id, processed);
            }

            thread::sleep(Duration::from_millis(100));
        });
    }

    fn run_heartbeat_loop_thread(id: NodeID, node: Arc<Mutex<Node<S, T>>>) {
        thread::spawn(move || loop {

            if let Some(mut node_lock) = node.try_lock_for(Duration::from_millis(500)) {
                log::info!("[Node {}] acquired lock in heartbeat loop", id);
                node_lock.heartbeat();
            }

            thread::sleep(Duration::from_millis(1000));
        });
    }

}
