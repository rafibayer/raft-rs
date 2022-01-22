use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, self, Write, Error};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::{thread, error};
use std::time::{Duration, Instant};

use crate::async_tcp::{incoming_listener, outgoing_pusher};
use crate::raft::{
    CommandRequest, LogEntry, LogRequest, LogResponse, NodeID, RaftRequest, Role, VoteRequest,
    VoteResponse, CommandResponse,
};
use crate::state::Storage;
use crate::utils;

use bincode;
use log::info;

// should be long, allow time for elections to reach all nodes
const ELECTION_TIMEOUT: std::ops::RangeInclusive<Duration> =
    Duration::from_millis(1000)..=Duration::from_millis(2500);

// should be very low, need establish authority and stop other elections
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1);

// ms to wait for an incoming message before assuming none.
// helps to reduce CPU usage by slowing down event loop.
const GET_MESSAGE_WAIT: u64 = 1;

pub struct Node<S: Storage> {
    // stable
    pub id: NodeID,
    current_term: usize,
    voted_for: Option<NodeID>,
    log: Vec<LogEntry>,
    commit_length: usize,

    // memory
    current_role: Role,
    current_leader: Option<NodeID>,
    votes_received: HashSet<NodeID>,
    sent_length: HashMap<NodeID, usize>,
    acked_length: HashMap<NodeID, usize>,

    // implementation
    t_heartbeat_received: Instant,
    t_heartbeat_sent: Instant,
    election_timeout: Duration,
    t_election_start: Instant,
    nodes: HashMap<NodeID, SocketAddr>,

    // outgoing requests and responses
    outgoing: Sender<(RaftRequest, NodeID)>,

    // incoming requests and responses
    incoming: Receiver<RaftRequest>,

    state: S,
}

impl<S: Storage> Node<S> {
    pub fn new(id: usize, nodes: HashMap<NodeID, SocketAddr>, state: S) -> Self {
        let (tx_outgoing, rx_outgoing) = mpsc::channel();
        let (tx_incoming, rx_incoming) = mpsc::channel();

        let address = nodes[&id];
        thread::spawn(move || {
            // will send incoming requests to tx_incoming.
            // node will get them from incoming: rx_incoming
            incoming_listener(address, tx_incoming);
        });

        let node_clone = nodes.clone();
        thread::spawn(move || {
            // will receive outgoing requests from rx_outgoing.
            // node will send them from tx_outgoing
            outgoing_pusher(id, rx_outgoing, node_clone);
        });

        log::info!("[Node {}] Initializing...", id);
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
            t_heartbeat_received: Instant::now(),
            t_heartbeat_sent: Instant::now(), // 0?
            election_timeout: utils::rand_duration(ELECTION_TIMEOUT), // 0? since we set on become follower
            t_election_start: Instant::now(),
            nodes,
            outgoing: tx_outgoing,
            incoming: rx_incoming,

            state,
        }
    }

    pub fn start(&mut self) {
        log::info!("{} starting", self.stamp());
        self.event_loop();
    }

    fn event_loop(&mut self) -> ! {
        loop {
            self.process_message().unwrap();

            match self.current_role {
                Role::Follower => self.follower(),
                Role::Candidate => self.candidate(),
                Role::Leader => self.leader(),
            };
        }
    }

    fn follower(&mut self) {
        // check for heartbeat timeout
        if Instant::now() > self.t_heartbeat_received + self.election_timeout {
            log::warn!("{} has not received heartbeat since \n\t(last: {:?}, now: {:?}), \n\tbecoming candidate", self.stamp(), self.t_heartbeat_received, Instant::now());
            self.become_candidate();
        }
    }

    fn candidate(&mut self) {
        // check for election timeout
        if Instant::now() > self.t_election_start + self.election_timeout {
            log::warn!("{} election timeout reached \n\t(start: {:?}, now: {:?}), \n\trestarting election",
                self.stamp(),
                self.t_election_start,
                Instant::now(),
            );
            self.become_candidate();
        }
    }

    fn leader(&mut self) {
        if Instant::now() > self.t_heartbeat_sent + HEARTBEAT_INTERVAL {
            self.send_heartbeat();
        }
    }

    fn become_follower(&mut self, term: usize) {
        self.current_role = Role::Follower;
        self.current_term = term;
        self.election_timeout = utils::rand_duration(ELECTION_TIMEOUT);
    }

    fn become_candidate(&mut self) {
        self.current_role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.current_term += 1;
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.t_election_start = Instant::now();

        self.request_votes();
    }

    fn become_leader(&mut self) {
        self.current_role = Role::Leader;
        self.current_leader = Some(self.id);

        // replicate logs to other nodes
        for follower in self.followers() {
            self.sent_length.insert(follower, self.log.len());
            self.acked_length.insert(follower, 0);
            self.replicate_log(follower);
        }
    }

    fn request_votes(&mut self) {
        let mut last_term = 0;
        if !self.log.is_empty() {
            last_term = self.log[self.log.len() - 1].term;
        }

        for follower in self.followers() {
            let request = VoteRequest {
                sender: self.id,
                term: self.current_term,
                log_length: self.log.len(),
                last_log_term: last_term,
            };

            log::trace!("{} sending vote request to {}", self.stamp(), follower);
            self.outgoing.send((RaftRequest::VoteRequest(request), follower)).unwrap();
        }
    }

    fn receive_vote_request(&mut self, request: VoteRequest) -> VoteResponse {
        log::trace!("{} received vote request from Node {}", self.stamp(), request.sender);

        // if we see a higher term, step down
        if request.term > self.current_term {
            log::warn!(
                "{} Found higher term: current_term = {} but node {} had term {}",
                self.stamp(),
                self.current_term,
                request.sender,
                request.term
            );
            self.voted_for = None; // must reset our vote, since it was for an old term
            self.become_follower(request.term);
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
            log::info!("{} voting for Node {}", self.stamp(), request.sender);
            self.voted_for = Some(request.sender);

            return VoteResponse { sender: self.id, term: self.current_term, granted: true };
        }

        VoteResponse { sender: self.id, term: self.current_term, granted: false }
    }

    fn receive_vote_response(&mut self, response: VoteResponse) {
        log::trace!("{} received vote response from Node {}", self.stamp(), response.sender);

        // check for higher term on vote response;
        if response.term > self.current_term {
            log::warn!(
                "{} Stepping down. received a vote response with a higher term {} vs {}",
                self.stamp(),
                response.term,
                self.current_term
            );
            self.voted_for = None; // must reset our vote, since it was for an old term
            self.become_follower(response.term);
        }

        if self.current_role == Role::Candidate
            && response.term == self.current_term
            && response.granted
        {
            log::info!("{} received granted vote from {}", self.stamp(), response.sender);

            self.votes_received.insert(response.sender);

            // check for quorum
            if self.votes_received.len() >= (self.nodes.len() / 2) + 1 {
                log::info!(
                    "{} ****** received a quorum with {} votes ******",
                    self.stamp(),
                    self.votes_received.len()
                );

                self.become_leader()
            }
        }
    }

    // todo: uh-oh, how is this gonna work?
    fn broadcast_request(&mut self, message: CommandRequest) -> CommandResponse {
        if self.current_role == Role::Leader {
            log::trace!("{} received broadcast request as leader", self.stamp());
            self.log.push(LogEntry { command: message.command, term: self.current_term });
            self.acked_length.insert(self.id, self.log.len());

            for follower in self.followers() {
                self.replicate_log(follower);
            }

            todo!()
            // return ...
            
        } 

        self.outgoing.send((RaftRequest::CommandRequest(message), self.current_leader.unwrap())).unwrap();
        todo!() // todo
    }

    fn send_heartbeat(&mut self) {
        self.t_heartbeat_sent = Instant::now();

        for follower in self.followers() {
            self.replicate_log(follower);
        }
    }

    fn replicate_log(&self, follower: NodeID) {
        // prefix: all log entries we think we have sent to follower
        let prefix_len = self.sent_length[&follower];

        // suffix: all log entries we have not yet sent to the follower
        let suffix = &self.log[prefix_len..];

        // term of last entry in prefix
        let mut prefix_term = 0;
        if prefix_len > 0 {
            prefix_term = self.log[prefix_len - 1].term;
        }

        let request = LogRequest {
            sender: self.id,
            term: self.current_term,
            prefix_lenth: prefix_len,
            prefix_term,
            leader_commit: self.commit_length,
            suffix: suffix.to_vec(), // todo: expensive clone
        };

        self.outgoing.send((RaftRequest::LogRequest(request), follower)).unwrap();
    }

    fn receive_log_request(&mut self, request: LogRequest) -> LogResponse {
        self.t_heartbeat_received = Instant::now();

        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None; // must reset vote, as it is for an older term
        }

        // above, we accepted current_term = term, so we always
        // fall through to this if-statement as well if
        // the first executed
        if request.term == self.current_term {
            self.become_follower(self.current_term);
            self.current_leader = Some(request.sender);
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
                self.stamp(),
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

            return LogResponse { sender: self.id, term: self.current_term, ack, success: true };
        }

        LogResponse { sender: self.id, term: self.current_term, ack: 0, success: false }
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

    fn receive_log_response(&mut self, response: LogResponse) {
        if response.term == self.current_term && self.current_role == Role::Leader {
            // ensures ack > last ack, incase response re-ordered
            if response.success && response.ack >= self.acked_length[&response.sender] {
                self.sent_length.insert(response.sender, response.ack);
                self.acked_length.insert(response.sender, response.ack);
                self.commit_log_entries();
            } else if self.sent_length[&response.sender] > 0 {
                log::warn!(
                    "{} follower failed to log: success={}, ack={} vs last ack={}",
                    self.stamp(),
                    response.success,
                    response.ack,
                    self.acked_length[&response.sender]
                );

                // if send fails, maybe gap in follower log.
                // decrement to try and shrink prefix, sending one more log
                // on next attempt.
                // if gap is large, this could take many iterations. (can be optimized)
                *self.sent_length.get_mut(&response.sender).unwrap() -= 1;
                self.replicate_log(response.sender);
            }
        } else if response.term > self.current_term {
            // as usual, step down if we see higher term
            self.t_heartbeat_received = Instant::now();
            self.voted_for = None;
            self.become_follower(response.term);
        }
    }

    fn commit_log_entries(&mut self) {
        while self.commit_length < self.log.len() {
            // count acks
            let mut acks = 0;
            for node in self.nodes.keys() {
                if self.acked_length[node] > self.commit_length {
                    acks += 1;
                }
            }

            // check for quorum
            if acks >= (self.nodes.len() / 2) + 1 {
                log::info!("{} leader committing log {}", self.stamp(), self.commit_length);
                self.state
                    .apply_command(&self.log[self.commit_length].command)
                    .expect("err applying command");

                self.commit_length += 1;
            } else {
                break;
            }
        }
    }

    fn get_next_request(&self) -> Option<RaftRequest> {
       self.incoming.try_recv().ok()
    }

    fn process_message(&mut self) -> Result<(), Box<dyn error::Error>> {
        if let Some(message) = self.get_next_request() {
            match message {
                RaftRequest::CommandRequest(request) => {
                    let from = request.sender;
                    let response = RaftRequest::CommandResponse(self.broadcast_request(request));
                    self.outgoing.send((response, from))?;
                },
                RaftRequest::CommandResponse(_response) => {
                    todo!()
                },
                RaftRequest::LogRequest(request) => {
                    let from = request.sender;
                    let response = RaftRequest::LogResponse(self.receive_log_request(request));
                    self.outgoing.send((response, from))?;
                },
                RaftRequest::LogResponse(response) => {
                    self.receive_log_response(response);
                },
                RaftRequest::VoteRequest(request) => {
                    let from = request.sender;

                    let response = RaftRequest::VoteResponse(self.receive_vote_request(request));
                    self.outgoing.send((response, from))?;
                },
                RaftRequest::VoteResponse(response) => {
                    self.receive_vote_response(response);
                },
            }
        } 

        Ok(())
    }
   
    #[inline]
    fn followers(&self) -> Vec<NodeID> {
        self.nodes
            .keys()
            .map(|k| *k)
            .filter(|k| *k != self.id)
            .collect()
    }

    #[inline]
    fn stamp(&self) -> String {
        format!("[Node {} | Term {} | {:?}]", self.id, self.current_term, self.current_role)
    }
}