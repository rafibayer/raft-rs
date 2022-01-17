use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

use crate::raft::{
    CommandRequest, LogEntry, LogRequest, LogResponse, Message, MessageData, NodeID, Role,
    RoleTransition, VoteRequest, VoteResponse,
};
use crate::state::Storage;
use crate::utils;

// should be long, allow time for elections to reach all nodes
const ELECTION_TIMEOUT: std::ops::RangeInclusive<Duration> = Duration::from_millis(1000)..=Duration::from_millis(2500);

// should be very low, need establish authority and stop other elections
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

// ms to wait for an incoming message before assuming none.
// helps to reduce CPU usage by slowing down event loop.
const GET_MESSAGE_WAIT: u64 = 1;

pub struct NewNode<S: Storage> {
    pub node: Node<S>,
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>,
}

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
    forwarding_queue: VecDeque<CommandRequest>,
    t_heartbeat_received: Instant,
    t_heartbeat_sent: Instant,
    election_timeout: Duration,
    t_election_start: Instant,
    receiver: Receiver<Message>,
    transport: Sender<Message>,
    nodes: HashSet<NodeID>,
    state: S,
}

impl<S: Storage> Node<S> {
    pub fn new_node(id: usize, nodes: HashSet<NodeID>, state: S) -> NewNode<S> {
        // sender is used to send messages to the node from the transport
        let (sender, rx) = mpsc::channel();

        // receiver is used to receive messages outside the node in the transport
        let (tx, receiver) = mpsc::channel();
        log::info!("[Node {}] Initializing...", id);
        let node = Node {
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
            forwarding_queue: VecDeque::new(),
            t_heartbeat_received: Instant::now(),
            t_heartbeat_sent: Instant::now(), // 0?
            election_timeout: utils::rand_duration(ELECTION_TIMEOUT), // 0? since we set on become follower
            t_election_start: Instant::now(),                                                 // 0?
            // incoming messages
            receiver: rx,
            // outgoing messages
            transport: tx,
            nodes,
            state,
        };

        NewNode { node, sender, receiver }
    }

    pub fn start(&mut self) {
        log::info!("{} starting", self.dbg());
        self.event_loop();
    }

    fn event_loop(&mut self) -> ! {
        loop {
            // process messages and check for transitions
            // todo: optimistic message processing? if the queue is long, can we process a few messages before moving to role specific stuff?
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

            // transition if needed.
            // we process transitions even to the same role, because this could represent
            // incrementing follwer term when stepping down, or restarting an election.
            if let Some(transition) = next_role {
                // log::warn!("{} transitioning to {:?}", self.dbg(), transition);
                match transition {
                    RoleTransition::Follower { term } => self.become_follower(term),
                    RoleTransition::Candidate => self.become_candidate(),
                    RoleTransition::Leader => self.become_leader(),
                };
            }
        }
    }

    fn follower(&mut self) -> Option<RoleTransition> {
        // should this go here or event_loop?
        self.try_send_fifo();

        // check for heartbeat timeout
        if Instant::now() > self.t_heartbeat_received + self.election_timeout {
            log::warn!("{} has not received heartbeat, becoming candidate", self.dbg());
            return Some(RoleTransition::Candidate);
        }

        None
    }

    fn candidate(&mut self) -> Option<RoleTransition> {
        // check for election timeout
        if Instant::now() > self.t_election_start + self.election_timeout {
            log::warn!("{} election timeout reached, restarting election", self.dbg());
            return Some(RoleTransition::Candidate);
        }

        None
    }

    fn leader(&mut self) -> Option<RoleTransition> {
        if Instant::now() > self.t_heartbeat_sent + HEARTBEAT_INTERVAL {
            log::trace!("{} heartbeat timeout, sending heartbeat", self.dbg());
            self.send_heartbeat();
        }

        None
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
                self.send_message(*node, MessageData::VoteRequest(request));
            }
        }
    }

    fn receive_vote_request(&mut self, request: VoteRequest) -> Option<RoleTransition> {
        log::trace!("{} received vote request from Node {}", self.dbg(), request.sender);

        let mut should_become_follower = false;

        // if we see a higher term, step down
        if request.term > self.current_term {
            log::warn!(
                "{} Found higher term: current_term = {} but node {} had term {}",
                self.dbg(),
                self.current_term,
                request.sender,
                request.term
            );
            // have to set term here since it is used in request validation
            self.current_term = request.term;
            self.voted_for = None; // must reset our vote, since it was for an old term
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
            log::trace!("{} voting for Node {}", self.dbg(), request.sender);

            self.voted_for = Some(request.sender);
            self.send_message(
                request.sender,
                MessageData::VoteResponse(VoteResponse {
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
                MessageData::VoteResponse(VoteResponse {
                    voter: self.id,
                    term: self.current_term,
                    granted: false,
                }),
            );
        }

        // if we voted yes, we should NOT be resetting vote here but we do
        if should_become_follower {
            // we already fixed term above
            return Some(RoleTransition::Follower { term: self.current_term });
        }

        None
    }

    fn receive_vote_response(&mut self, response: VoteResponse) -> Option<RoleTransition> {
        log::trace!("{} received vote response from Node {}", self.dbg(), response.voter);

        // check for higher term on vote response;
        if response.term > self.current_term {
            log::warn!(
                "{} Stepping down. received a vote response with a higher term {} vs {}",
                self.dbg(),
                response.term,
                self.current_term
            );
            self.voted_for = None; // must reset our vote, since it was for an old term
            return Some(RoleTransition::Follower { term: response.term });
        }

        if self.current_role == Role::Candidate
            && response.term == self.current_term
            && response.granted
        {
            log::trace!("{} received granted vote from {}", self.dbg(), response.voter);

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
        }

        None
    }

    fn broadcast_request(&mut self, message: CommandRequest) -> Option<RoleTransition> {
        if self.current_role == Role::Leader {
            log::trace!("{} received broadcast request as leader", self.dbg());
            self.log.push(LogEntry { command: message.command, term: self.current_term });
            self.acked_length.insert(self.id, self.log.len());

            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(follower);
                }
            }
        } else {
            log::trace!("{} received broadcast request as follower, forwarding.", self.dbg());
            self.forward(message);
        }

        None
    }

    fn send_heartbeat(&mut self) {
        self.t_heartbeat_sent = Instant::now();
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
            MessageData::LogRequest(LogRequest {
                leader_id: self.id,
                term: self.current_term,
                prefix_lenth: prefix_len,
                prefix_term,
                leader_commit: self.commit_length,
                suffix: suffix.to_vec(), // todo: expensive clone
            }),
        )
    }

    fn receive_log_request(&mut self, request: LogRequest) -> Option<RoleTransition> {
        let mut should_become_follower = false;

        self.t_heartbeat_received = Instant::now();

        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None; // must reset vote, as it is for an older term
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
                MessageData::LogResponse(LogResponse {
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
                MessageData::LogResponse(LogResponse {
                    sender: self.id,
                    term: self.current_term,
                    ack: 0,
                    success: false,
                }),
            )
        }

        if should_become_follower {
            // self.current_term already updated above
            return Some(RoleTransition::Follower { term: self.current_term });
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

    fn receive_log_response(&mut self, response: LogResponse) -> Option<RoleTransition> {
        if response.term > self.current_term {
            // as usual, step down if we see higher term
            self.t_heartbeat_received = Instant::now();
            self.voted_for = None;
            return Some(RoleTransition::Follower { term: response.term });
        }

        if response.term == self.current_term && self.current_role == Role::Leader {
            // ensures ack > last ack, incase response re-ordered
            if response.success && response.ack >= self.acked_length[&response.sender] {
                self.sent_length.insert(response.sender, response.ack);
                self.acked_length.insert(response.sender, response.ack);
                self.commit_log_entries();
            } else if self.sent_length[&response.sender] > 0 {
                log::warn!(
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

    fn send_message(&self, node: NodeID, message: MessageData) {
        log::trace!("{} sending message {:?} to Node {}", self.dbg(), message, node);
        self.transport.send(Message { destination: node, message }).unwrap();
    }

    fn get_next_message(&self) -> Option<MessageData> {
        if let Ok(Message { destination: _, message }) =
            self.receiver.recv_timeout(Duration::from_millis(GET_MESSAGE_WAIT))
        {
            return Some(message);
        }

        None
    }

    fn forward(&mut self, message: CommandRequest) {
        self.forwarding_queue.push_back(message);
        self.try_send_fifo();
    }

    // forwards every message in the forwarding_queue if there is a current_leader
    fn try_send_fifo(&mut self) {
        if let Some(leader) = self.current_leader {
            for command in self.forwarding_queue.drain(..) {
                self.transport
                    .send(Message {
                        destination: leader,
                        message: MessageData::CommandRequest(command),
                    })
                    .unwrap();
            }
        }
    }

    fn process_next_message(&mut self) -> Option<RoleTransition> {
        match self.get_next_message() {
            Some(MessageData::CommandRequest(command)) => self.broadcast_request(command),
            Some(MessageData::VoteRequest(vote_req)) => self.receive_vote_request(vote_req),
            Some(MessageData::VoteResponse(vote_resp)) => self.receive_vote_response(vote_resp),
            Some(MessageData::LogRequest(log_req)) => self.receive_log_request(log_req),
            Some(MessageData::LogResponse(log_resp)) => self.receive_log_response(log_resp),
            None => None,
        }
    }

    #[inline]
    fn dbg(&self) -> String {
        format!("[Node {} | Term {} | {:?}]", self.id, self.current_term, self.current_role)
    }
}
