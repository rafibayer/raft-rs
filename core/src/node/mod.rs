mod background;
pub mod config;

use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};
use std::{error, thread};

use log::info;

use background::{inbox_thread, outbox_thread};

use crate::raft::{
    AdminRequest, AdminResponse, CommandRequest, CommandResponse, Event, LogEntry, LogRequest,
    LogResponse, NodeID, RaftRequest, Role, VoteRequest, VoteResponse,
};
use crate::state::Storage;
use crate::utils;

use self::config::{Config, InternalConfig};

type SyncConnection = Sender<RaftRequest>;

pub struct Node<S: Storage> {
    // must be stable storage
    pub id: NodeID,
    current_term: usize,
    voted_for: Option<NodeID>,
    log: Vec<LogEntry>,
    commit_length: usize,

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
    config: InternalConfig,

    // clients waiting for event notifications
    waiting_events: HashMap<Event, Vec<SyncConnection>>,

    // outgoing requests and responses
    outgoing: Sender<(RaftRequest, NodeID)>,

    // incoming requests and responses
    incoming: Receiver<(RaftRequest, Option<SyncConnection>)>,

    state: S,

    shutdown: bool,
    shutdown_signal: Sender<()>,
}

impl<S: Storage> Node<S> {
    pub fn new(id: usize, config: Config, state: S) -> Self {
        let (tx_shutdown, rx_shutdown) = mpsc::channel();

        let (tx_outgoing, rx_outgoing) = mpsc::channel();
        let (tx_incoming, rx_incoming) = mpsc::channel();

        let address = config.cluster[&id];
        thread::spawn(move || {
            // will send incoming requests to tx_incoming.
            // node will get them from incoming: rx_incoming
            inbox_thread(id, address, tx_incoming, rx_shutdown);
        });

        let cluster_clone = config.cluster.clone();
        thread::spawn(move || {
            // will receive outgoing requests from rx_outgoing.
            // node will send them from tx_outgoing
            outbox_thread(id, rx_outgoing, cluster_clone);
        });

        let config = InternalConfig::from(config);

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
            t_heartbeat_sent: Instant::now(),
            election_timeout: utils::rand_duration(config.election_timeout_range.clone()), // 0? since we set on become follower
            t_election_start: Instant::now(),
            config,

            waiting_events: HashMap::new(),

            outgoing: tx_outgoing,
            incoming: rx_incoming,

            state,

            shutdown: false,
            shutdown_signal: tx_shutdown,
        }
    }

    pub fn start(self) {
        log::info!("{} starting", self.info());
        self.event_loop();
    }

    fn event_loop(mut self) {
        loop {
            self.process_message().unwrap();

            match self.current_role {
                Role::Follower => self.follower(),
                Role::Candidate => self.candidate(),
                Role::Leader => self.leader(),
            };

            if self.shutdown {
                self.shutdown_signal.send(()).unwrap();

                // if shutdown takes longer, we may notify early
                thread::sleep(Duration::from_millis(1000));

                self.notify(
                    Event::ShutdownCompleted,
                    RaftRequest::AdminResponse(AdminResponse::Done),
                );

                // here we close outgoing, which does stop outbox.
                // note this happens after we notify, because we can't notify after dropping self.
                drop(self.outgoing);

                return;
            }
        }
    }

    fn follower(&mut self) {
        // check for heartbeat timeout
        if self.t_heartbeat_received.elapsed() > self.election_timeout {
            log::warn!(
                "{} has not received heartbeat in {:?}, becoming candidate",
                self.info(),
                self.t_heartbeat_received.elapsed()
            );
            self.become_candidate();
        }
    }

    fn candidate(&mut self) {
        // check for election timeout
        if self.t_election_start.elapsed() > self.election_timeout {
            log::warn!(
                "{} election timed out after {:?}, restarting election",
                self.info(),
                self.t_election_start.elapsed()
            );
            self.become_candidate();
        }
    }

    fn leader(&mut self) {
        if self.t_heartbeat_sent.elapsed() > self.config.heartbeat_interval {
            self.send_heartbeat();
        }
    }

    fn become_follower(&mut self, term: usize) {
        self.current_role = Role::Follower;
        self.current_term = term;
        self.election_timeout = utils::rand_duration(self.config.election_timeout_range.clone());
    }

    fn become_candidate(&mut self) {
        self.current_role = Role::Candidate;
        self.current_term += 1;

        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        self.t_election_start = Instant::now();
        self.election_timeout = utils::rand_duration(self.config.election_timeout_range.clone());

        self.request_votes();
    }

    fn become_leader(&mut self) {
        info!("{} becoming leader", self.info());
        self.current_role = Role::Leader;
        self.current_leader = Some(self.id);

        // replicate logs to other nodes
        for follower in self.peers() {
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

        for follower in self.peers() {
            let request = VoteRequest {
                sender: self.id,
                term: self.current_term,
                log_length: self.log.len(),
                last_log_term: last_term,
            };

            log::trace!("{} sending vote request to {}", self.info(), follower);
            self.outgoing.send((RaftRequest::VoteRequest(request), follower)).unwrap();
        }
    }

    fn receive_vote_request(&mut self, request: VoteRequest) -> VoteResponse {
        log::trace!("{} received vote request from Node {}", self.info(), request.sender);

        // if we see a higher term, step down
        if request.term > self.current_term {
            log::warn!(
                "{} Found higher term: current_term = {} but node {} had term {}",
                self.info(),
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
            log::info!("{} voting for Node {}", self.info(), request.sender);
            self.voted_for = Some(request.sender);

            return VoteResponse { sender: self.id, term: self.current_term, granted: true };
        }

        log::warn!("{} not voting for Node {}", self.info(), request.sender);
        VoteResponse { sender: self.id, term: self.current_term, granted: false }
    }

    fn receive_vote_response(&mut self, response: VoteResponse) {
        log::trace!("{} received vote response from Node {}", self.info(), response.sender);

        // check for higher term on vote response;
        if response.term > self.current_term {
            log::warn!(
                "{} Stepping down. received a vote response with a higher term {} vs {}",
                self.info(),
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
            log::info!("{} received granted vote from {}", self.info(), response.sender);

            self.votes_received.insert(response.sender);

            // check for quorum
            if self.votes_received.len() > self.config.cluster.len() / 2 {
                log::info!(
                    "{} ****** received a quorum with {} votes ******",
                    self.info(),
                    self.votes_received.len()
                );

                self.become_leader()
            }
        }
    }

    fn broadcast_request(&mut self, message: CommandRequest, client: SyncConnection) {
        if self.current_role == Role::Leader {
            log::trace!("{} received broadcast request as leader", self.info());
            self.log.push(LogEntry { command: message.command, term: self.current_term });
            self.acked_length.insert(self.id, self.log.len());

            for follower in self.peers() {
                self.replicate_log(follower);
            }

            self.subscribe(Event::CommittedLog(self.log.len() - 1), client);
        } else {
            // reply with NotLeader or Unavailable
            let response = match self.current_leader {
                Some(leader) => CommandResponse::NotLeader(leader),
                None => CommandResponse::Unavailable,
            };
            client.send(RaftRequest::CommandResponse(response)).unwrap();
        }
    }

    fn send_heartbeat(&mut self) {
        self.t_heartbeat_sent = Instant::now();

        for follower in self.peers() {
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
                self.info(),
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
                if self.waiting_events.contains_key(&Event::CommittedLog(prefix_len - 1)) {
                    log::error!(
                        "{} truncated log {:?} with a waiting client!",
                        self.info(),
                        prefix_len - 1
                    );
                }
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
                self.state
                    .apply_command(&self.log[i].command)
                    .expect("err applying command");
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
                    self.info(),
                    response.success,
                    response.ack,
                    self.acked_length[&response.sender]
                );

                // if send fails, maybe gap in follower log.
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
            for node in self.config.cluster.keys() {
                if self.acked_length[node] > self.commit_length {
                    acks += 1;
                }
            }

            // check for quorum
            if acks > self.config.cluster.len() / 2 {
                log::info!("{} leader committing log {}", self.info(), self.commit_length);
                let result = self
                    .state
                    .apply_command(&self.log[self.commit_length].command)
                    .expect("err applying command");

                self.notify(
                    Event::CommittedLog(self.commit_length),
                    RaftRequest::CommandResponse(CommandResponse::Result(result)),
                );

                self.commit_length += 1;
            } else {
                break;
            }
        }
    }

    /// process the next incoming message.
    /// Messages may come from clients, or other nodes in the cluster.
    fn process_message(&mut self) -> Result<(), Box<dyn error::Error>> {
        if let Ok((message, client)) = self.incoming.try_recv() {
            match message {
                RaftRequest::CommandRequest(request) => {
                    self.broadcast_request(request, client.unwrap());
                }
                RaftRequest::CommandResponse(response) => {
                    log::error!("{} received a command response! {response:?}", self.info());
                }
                RaftRequest::LogRequest(request) => {
                    let from = request.sender;
                    let response = RaftRequest::LogResponse(self.receive_log_request(request));
                    self.outgoing.send((response, from))?;
                }
                RaftRequest::LogResponse(response) => {
                    self.receive_log_response(response);
                }
                RaftRequest::VoteRequest(request) => {
                    let from = request.sender;
                    let response = RaftRequest::VoteResponse(self.receive_vote_request(request));
                    self.outgoing.send((response, from))?;
                }
                RaftRequest::VoteResponse(response) => {
                    self.receive_vote_response(response);
                }
                RaftRequest::AdminRequest(request) => {
                    self.receive_admin_request(request, client.unwrap());
                }
                RaftRequest::AdminResponse(response) => {
                    log::error!(
                        "{} recieved an admin response as a node: {response:?}",
                        self.info()
                    );
                }
            }
        }

        Ok(())
    }

    /// Processes an admin request from a given client
    fn receive_admin_request(&mut self, request: AdminRequest, client: SyncConnection) {
        log::warn!("{} received admin request: {request:?}", self.info());
        match request {
            AdminRequest::Shutdown => {
                // shutdown must happen in the event_loop, so we subscribe the client here.
                self.subscribe(Event::ShutdownCompleted, client);

                // shutdown will begin next iteration of the event loop
                self.shutdown = true
            }
            AdminRequest::BecomeLeader => {
                self.current_term += 1;
                self.become_leader();
                client.send(RaftRequest::AdminResponse(AdminResponse::Done)).unwrap();
            }
            AdminRequest::BecomeFollower => {
                self.become_follower(self.current_term);
                client.send(RaftRequest::AdminResponse(AdminResponse::Done)).unwrap();
            }
            AdminRequest::BecomeCandidate => {
                self.become_candidate();
                client.send(RaftRequest::AdminResponse(AdminResponse::Done)).unwrap();
            }
            AdminRequest::GetLeader => {
                client
                    .send(RaftRequest::AdminResponse(AdminResponse::Leader(self.current_leader)))
                    .unwrap();
            }
            AdminRequest::GetLogLength => {
                client
                    .send(RaftRequest::AdminResponse(AdminResponse::LogLength(self.log.len())))
                    .unwrap();
            }
            AdminRequest::GetTerm => {
                client
                    .send(RaftRequest::AdminResponse(AdminResponse::Term(self.current_term)))
                    .unwrap();
            }
        }
    }

    /// subscribes a connection to an event
    fn subscribe(&mut self, event: Event, conn: SyncConnection) {
        self.waiting_events.entry(event).or_insert_with(Vec::new).push(conn);
    }

    /// sends a notification to all subscribes for an event,
    /// unsubscribing them.
    fn notify(&mut self, event: Event, notification: RaftRequest) {
        if let Some(conns) = self.waiting_events.remove(&event) {
            for conn in conns {
                conn.send(notification.clone()).unwrap();
            }
        }
    }

    /// Returns a Vec<NodeID> containing the NodeID for every node in the cluster
    /// except self.
    #[inline]
    fn peers(&self) -> Vec<NodeID> {
        self.config.cluster.keys().cloned().filter(|k| *k != self.id).collect()
    }

    /// Returns a nicely-formatted info string about this node
    #[inline]
    fn info(&self) -> String {
        format!("[Node {} | Term {} | {:?}]", self.id, self.current_term, self.current_role)
    }
}
