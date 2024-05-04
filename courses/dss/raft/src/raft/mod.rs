use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::Add;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures_timer::Delay;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::Role::{Candidate, Follower, Leader};

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

const BASIC_TIMEOUT_MS: u64 = 150;
const RPC_RETRY: usize = 20;

type AsyncOneshotSender<T> = oneshot::Sender<T>;

#[derive(Serialize, Deserialize)]
struct RaftState {
    term: u64,
    voted_terms: HashMap<u64, u64>,
    #[serde(serialize_with = "raft_serde::serialize_vec_entry")]
    #[serde(deserialize_with = "raft_serde::deserialize_vec_entry")]
    logs: Vec<AppendEntry>,
    logs_vote: Vec<HashSet<u64>>,
}

mod raft_serde {
    use crate::proto::raftpb::AppendEntry;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize_vec_entry<S: Serializer>(
        vec: &[AppendEntry],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let vec: Vec<AppendEntrySerde> = vec.iter().map(|entry| entry.clone().into()).collect();
        vec.serialize(serializer)
    }

    pub fn deserialize_vec_entry<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<AppendEntry>, D::Error> {
        let vec: Vec<AppendEntrySerde> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into_iter().map(|entry| entry.into()).collect())
    }

    #[derive(Serialize, Deserialize)]
    struct AppendEntrySerde {
        pub term: u64,
        pub index: u64,
        pub command: std::vec::Vec<u8>,
    }

    impl From<AppendEntry> for AppendEntrySerde {
        fn from(value: AppendEntry) -> Self {
            AppendEntrySerde {
                term: value.term,
                index: value.index,
                command: value.command,
            }
        }
    }

    impl Into<AppendEntry> for AppendEntrySerde {
        fn into(self) -> AppendEntry {
            AppendEntry {
                term: self.term,
                index: self.index,
                command: self.command,
            }
        }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    term: u64,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    n: usize,
    role: Role,
    voted_terms: HashMap<u64, u64>,
    get_voted: HashMap<u64, HashSet<usize>>,
    last_receive_heartbeat: Instant,
    last_send_heartbeat: Vec<Instant>,
    remote_servers: Vec<usize>,
    worker: ThreadPool,
    tx: Sender<RaftMessage>,
    rx: Receiver<RaftMessage>,
    logs: Vec<AppendEntry>,
    logs_vote: Vec<HashSet<u64>>,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    commit_index: u64,
    last_applied: u64,
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let n = peers.len();
        let remote_servers = (0..n).filter(|&i| i != me).collect();
        // Your initialization code here (2A, 2B, 2C).
        let (tx, rx) = std::sync::mpsc::channel();
        let first_vote_set = (0..(n as u64)).collect::<HashSet<_>>();
        let mut rf = Raft {
            peers,
            persister,
            me,
            term: 0,
            voted_terms: HashMap::new(),
            get_voted: HashMap::new(),
            role: Follower,
            last_receive_heartbeat: Instant::now(),
            last_send_heartbeat: vec![Instant::now(); n],
            n,
            remote_servers,
            worker: ThreadPool::new().unwrap(),
            tx,
            rx,
            logs: vec![AppendEntry::dummy_entry()],
            next_index: vec![1; n],
            match_index: vec![0; n],
            logs_vote: vec![first_vote_set],
            commit_index: 0,
            last_applied: 0,
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
        //crate::your_code_here((rf, apply_ch))
    }

    fn run(&mut self) {
        while let Ok(msg) = self.rx.recv() {
            match msg {
                RaftMessage::FollowerTimeout(duration) => {
                    self.handle_follower_timeout(duration);
                }
                RaftMessage::LeaderTimeout(server, duration) => {
                    self.handle_heartbeat_timeout(server, duration);
                }
                RaftMessage::VoteReply(vote_reply) => {
                    self.handle_vote_reply(vote_reply);
                }
                RaftMessage::AppendReply(append_reply) => {
                    self.handle_append_reply(append_reply);
                }
                RaftMessage::VoteRequest(request, tx) => {
                    self.handle_vote_request(request, tx);
                }
                RaftMessage::AppendRequest(request, tx) => {
                    self.handle_append_request(request, tx);
                }
                RaftMessage::QueryTerm(tx) => {
                    let _ = tx.send(self.term);
                }
                RaftMessage::QueryLeader(tx) => {
                    let _ = tx.send(self.is_leader());
                }
                RaftMessage::StartCommand(command, tx) => {
                    self.start(command, tx);
                }
                RaftMessage::SyncLog(server) => {
                    self.send_append(server, false);
                }
                RaftMessage::Stop => {
                    break;
                }
            }
        }
    }

    fn get_state(&self) -> RaftState {
        RaftState {
            term: self.term,
            voted_terms: self.voted_terms.clone(),
            logs: self.logs.clone(),
            logs_vote: self.logs_vote.clone(),
        }
    }

    fn new_tx(&self) -> Sender<RaftMessage> {
        self.tx.clone()
    }

    fn handle_vote_request(
        &mut self,
        vote_request: RequestVoteArgs,
        tx: AsyncOneshotSender<RequestVoteReply>,
    ) {
        if vote_request.term < self.term {
            let _ = tx.send(RequestVoteReply {
                id: self.me as u64,
                term: self.term,
                vote: false,
            });
            return;
        }
        if vote_request.term > self.term {
            self.catch_up_term(vote_request.term);
        }
        let vote = if self.should_vote(
            self.term,
            vote_request.last_log_index,
            vote_request.last_log_term,
            vote_request.id,
        ) {
            self.vote_for(vote_request.id as usize);
            self.refresh_follower_timeout();
            true
        } else {
            false
        };
        let _ = tx.send(RequestVoteReply {
            id: self.me as u64,
            term: self.term,
            vote,
        });
    }

    fn should_vote(&self, term: u64, last_index: u64, last_term: u64, id: u64) -> bool {
        if !self.is_follower() {
            return false;
        }
        if let Some(&vote_for) = self.voted_terms.get(&term) {
            if vote_for != id {
                return false;
            }
        }
        let my_last_entry = self.logs.last().unwrap();
        if my_last_entry.term == last_term {
            my_last_entry.index <= last_index
        } else {
            my_last_entry.term < last_term
        }
    }

    fn handle_append_request(
        &mut self,
        request: AppendEntriesArgs,
        tx: AsyncOneshotSender<AppendEntriesReply>,
    ) {
        if request.term < self.term {
            let _ = tx.send(AppendEntriesReply {
                id: self.me as u64,
                term: self.term,
                accept: false,
                pre_log_index: request.pre_log_index,
                matched_index: 0,
            });
            return;
        }
        if request.term > self.term {
            self.catch_up_term(request.term);
        }
        if self.is_leader() {
            let _ = tx.send(AppendEntriesReply {
                id: 0,
                term: 0,
                accept: false,
                pre_log_index: 0,
                matched_index: 0,
            });
            return;
        }
        self.refresh_follower_timeout();
        let accept = self.is_matched(request.pre_log_index, request.pre_log_term);
        if accept {
            let mut matched_index = request.pre_log_index as usize;
            for (i, entry) in request.entries.iter().enumerate() {
                let i = i + request.pre_log_index as usize + 1;
                if i > self.logs.len() {
                    break;
                } else if i == self.logs.len() {
                    self.logs.push(entry.clone());
                    self.logs_vote.push(HashSet::new());
                } else if i < self.logs.len() {
                    self.logs[i] = entry.clone();
                    self.logs_vote[i] = HashSet::new();
                }
                matched_index = i;
            }

            let _ = tx.send(AppendEntriesReply {
                id: self.me as u64,
                term: self.term,
                accept: true,
                matched_index: matched_index as u64,
                pre_log_index: request.pre_log_index,
            });

            if matched_index as u64 >= request.leader_commit
                && request.leader_commit > self.commit_index
            {
                self.commit_index = request.leader_commit.min(self.logs.len() as u64 - 1);
                self.commit();
            }
        } else {
            let len = self.logs.len() as u64;
            for _ in request.pre_log_index..len {
                self.logs.pop();
                self.logs_vote.pop();
            }
            assert!(!self.logs.is_empty(), "empty log");
            let _ = tx.send(AppendEntriesReply {
                id: self.me as u64,
                term: self.term,
                accept: false,
                pre_log_index: request.pre_log_index,
                matched_index: 0,
            });
        }
        self.persist();
    }

    fn is_matched(&self, index: u64, term: u64) -> bool {
        self.logs
            .get(index as usize)
            .map_or(false, |entry| entry.term == term)
    }

    fn refresh_follower_timeout(&mut self) {
        self.last_receive_heartbeat = Instant::now();
    }

    fn handle_follower_timeout(&mut self, duration: Duration) {
        if self.is_leader() {
            return;
        }
        let next_fire = self
            .last_receive_heartbeat
            .add(duration)
            .checked_duration_since(Instant::now());
        if let Some(next_fire) = next_fire {
            let tx = self.new_tx();
            self.worker.spawn_ok(async move {
                Delay::new(next_fire).await;
                let _ = tx.send(RaftMessage::FollowerTimeout(duration));
            });
        } else {
            self.start_election();
        }
    }

    fn handle_heartbeat_timeout(&mut self, server: usize, _d: Duration) {
        if !self.is_leader() {
            return;
        }
        self.send_append(server, true);
    }

    fn start_election(&mut self) {
        assert!(!self.is_leader(), "leader start election");
        self.increase_term();
        self.role = Candidate;
        self.vote_for(self.me);

        let vote_args = RequestVoteArgs {
            id: self.me as u64,
            term: self.term,
            last_log_index: self.logs.len() as u64 - 1,
            last_log_term: self.logs.last().unwrap().term,
        };
        self.remote_servers
            .iter()
            .map(|&i| self.send_request_vote(i, vote_args.clone()))
            .for_each(|rx| {
                let tx = self.new_tx();
                self.worker.spawn_ok(async move {
                    let reply = rx.await;
                    if let Ok(reply) = reply {
                        let _ = tx.send(RaftMessage::VoteReply(reply));
                    }
                })
            });
    }

    fn handle_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term < self.term {
            return;
        }
        if reply.term > self.term {
            self.catch_up_term(reply.term);
            return;
        }
        if !self.is_candidate() {
            return;
        }
        if reply.vote {
            self.get_voted(reply.term, reply.id as usize);
        }
        if self.has_gotten_majority_votes() {
            self.switch_to_leader();
        }
    }

    fn handle_append_reply(&mut self, reply: AppendEntriesReply) {
        if reply.term < self.term {
            return;
        }
        if reply.term > self.term {
            self.catch_up_term(reply.term);
            return;
        }
        if !self.is_leader() {
            return;
        }
        let server = reply.id as usize;
        if reply.accept {
            self.match_index[server] = self.match_index[server].max(reply.matched_index);
            self.next_index[server] = (self.match_index[server] + 2).min(self.highest_index());
            let idx = reply.matched_index as usize;
            for i in (0..=idx).rev() {
                if self.logs[i].term == self.term {
                    self.logs_vote[i].insert(reply.id);
                    self.logs_vote[i].insert(self.me as u64);
                    if (self.logs_vote[i].len() * 2 > self.n) && self.commit_index < i as u64 {
                        self.commit_index = i as u64;
                        self.commit();
                        break;
                    }
                } else {
                    break;
                }
            }
        } else {
            self.next_index[server] = self.next_index[server]
                .checked_sub(20)
                .unwrap_or(1)
                .max(self.match_index[server] + 1);
        }

        if self.next_index[server] < self.highest_index() {
            let _ = self.tx.send(RaftMessage::SyncLog(server));
        }
    }

    fn commit(&mut self) {
        let start = self.last_applied + 1;
        for i in start..=self.commit_index {
            self.apply_ch
                .unbounded_send(ApplyMsg::Command {
                    data: self.logs[i as usize].command.clone(),
                    index: i,
                })
                .expect("commit call error");
            self.last_applied = i;
        }
    }

    fn highest_index(&self) -> u64 {
        self.logs.len() as u64
    }

    fn vote_for(&mut self, id: usize) {
        self.voted_terms.insert(self.term, id as u64);
        if id == self.me {
            self.get_voted(self.term, id);
        }
        self.persist();
    }

    fn get_voted(&mut self, term: u64, id: usize) {
        self.get_voted.entry(term).or_default().insert(id);
    }

    fn has_gotten_majority_votes(&self) -> bool {
        let votes = self.get_voted.get(&self.term).map_or(0, |s| s.len());
        votes * 2 > self.n
    }

    fn switch_to_leader(&mut self) {
        assert!(!self.is_leader(), "error: leader switch to leader");
        self.role = Leader;
        for s in &mut self.logs_vote {
            s.clear();
        }
        for i in self.remote_servers.clone() {
            self.last_send_heartbeat[i] = Instant::now();
            self.match_index[i] = 0;
            self.next_index[i] = self.highest_index();
            self.send_append(i, true);
        }
    }

    fn increase_term(&mut self) {
        self.term += 1;
    }

    fn catch_up_term(&mut self, new_term: u64) {
        if new_term <= self.term {
            error!(
                "new term {} is not greater than current term {}",
                new_term, self.term
            );
            return;
        }
        self.term = new_term;
        self.role = Follower;
        self.last_receive_heartbeat = Instant::now();
        self.persist();
    }

    fn is_leader(&self) -> bool {
        self.role == Leader
    }

    fn is_candidate(&self) -> bool {
        self.role == Candidate
    }

    fn is_follower(&self) -> bool {
        self.role == Follower
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let state = self.get_state();
        let s = serde_json::to_string(&state).unwrap();
        self.persister.save_raft_state(s.into_bytes());
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let state: RaftState = serde_json::from_slice(data).unwrap();
        self.term = state.term;
        self.logs = state.logs;
        self.voted_terms = state.voted_terms;
        self.logs_vote = state.logs_vote;
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> oneshot::Receiver<RequestVoteReply> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = oneshot::channel::<RequestVoteReply>();
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            for _ in 0..RPC_RETRY {
                let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    let _ = tx.send(res);
                    return;
                }
            }
        });
        rx
    }

    fn send_append(&mut self, server: usize, heartbeat: bool) {
        let next_index = self.next_index[server] as usize;
        let pre_log_index = self.logs[next_index - 1].index;
        let pre_log_term = self.logs[next_index - 1].term;
        let entries: Vec<_> = (next_index..self.logs.len())
            .take(20)
            .map(|i| self.logs[i].clone())
            .collect();
        if entries.len() < 4 && !heartbeat {
            return;
        }
        let args = AppendEntriesArgs {
            id: self.me as u64,
            term: self.term,
            pre_log_index,
            pre_log_term,
            entries,
            leader_commit: self.commit_index,
        };

        let (tx, rx) = oneshot::channel::<AppendEntriesReply>();
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            for _ in 0.. {
                let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    let _ = tx.send(res);
                    break;
                }
            }
        });
        self.last_send_heartbeat[server] = Instant::now();

        let tx = self.tx.clone();
        self.worker.spawn_ok(async move {
            let reply = rx.await;
            if let Ok(reply) = reply {
                let _ = tx.send(RaftMessage::AppendReply(reply));
            }
        });
    }

    fn start(&mut self, command: Vec<u8>, tx: Sender<Result<(u64, u64)>>) {
        if !self.is_leader() {
            let _ = tx.send(Err(Error::NotLeader));
            return;
        }
        let term = self.term;
        let index = self.highest_index();
        if let Err(err) = tx.send(Ok((index, term))) {
            error!("raft start end result failed {}", err);
            return;
        }
        let entry = AppendEntry {
            term,
            index,
            command,
        };
        self.logs.push(entry);
        self.logs_vote.push(HashSet::new());
        self.logs_vote.last_mut().unwrap().insert(self.me as u64);
        self.persist();
        for i in self.remote_servers.clone() {
            self.send_append(i, false);
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
    }
}

enum RaftMessage {
    FollowerTimeout(Duration),
    LeaderTimeout(usize, Duration),

    VoteRequest(RequestVoteArgs, AsyncOneshotSender<RequestVoteReply>),
    VoteReply(RequestVoteReply),

    AppendRequest(AppendEntriesArgs, AsyncOneshotSender<AppendEntriesReply>),
    AppendReply(AppendEntriesReply),

    QueryTerm(Sender<u64>),
    QueryLeader(Sender<bool>),

    StartCommand(Vec<u8>, Sender<Result<(u64, u64)>>),

    SyncLog(usize),

    Stop,
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    worker: ThreadPool,
    sender: Sender<RaftMessage>,
    raft_daemon: Arc<JoinHandle<()>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let tx = raft.tx.clone();
        let worker = raft.worker.clone();
        let n = raft.n;
        let me = raft.me;

        let follower_timeout_tx = tx.clone();
        worker.spawn_ok(async move {
            loop {
                let duration = random_election_timeout();
                Delay::new(duration).await;
                let _ = follower_timeout_tx.send(RaftMessage::FollowerTimeout(duration));
            }
        });
        let leader_timeout_tx = tx.clone();
        worker.spawn_ok(async move {
            loop {
                let duration = Duration::from_millis(BASIC_TIMEOUT_MS - 10);
                Delay::new(duration).await;
                for i in 0..n {
                    if i != me {
                        let _ = leader_timeout_tx.send(RaftMessage::LeaderTimeout(i, duration));
                    }
                }
            }
        });

        let t = std::thread::spawn(move || raft.run());
        Node {
            worker,
            sender: tx,
            raft_daemon: Arc::new(t),
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        let (tx, rx) = channel();
        let _ = self.sender.send(RaftMessage::StartCommand(buf, tx));
        match rx.recv() {
            Ok(reply) => reply,
            Err(_) => Err(Error::Rpc(labrpc::Error::Stopped)),
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        let (tx, rx) = channel();
        self.sender.send(RaftMessage::QueryTerm(tx)).unwrap();
        rx.recv().unwrap()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        let (tx, rx) = channel();
        self.sender.send(RaftMessage::QueryLeader(tx)).unwrap();
        rx.recv().unwrap()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        let _ = self.sender.send(RaftMessage::Stop);
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(RaftMessage::VoteRequest(args, tx))
            .is_err()
        {
            return Err(labrpc::Error::Other(
                "send vote request to raft failed".into(),
            ));
        };
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(RaftMessage::AppendRequest(args, tx))
            .is_err()
        {
            return Err(labrpc::Error::Other(
                "send append request to raft failed".into(),
            ));
        };
        let reply = rx.await;
        reply.map_err(labrpc::Error::Recv)
    }
}

fn random_election_timeout() -> Duration {
    let mut random = rand::thread_rng();
    let timeout_ms: u64 = random.gen::<u64>() % 200 + BASIC_TIMEOUT_MS + 20;
    Duration::from_millis(timeout_ms)
}

impl From<i32> for RequestType {
    fn from(value: i32) -> Self {
        match value {
            0 => RequestType::Heartbeat,
            _ => unreachable!(),
        }
    }
}

impl AppendEntry {
    fn dummy_entry() -> AppendEntry {
        AppendEntry {
            term: 0,
            index: 0,
            command: vec![],
        }
    }
}
