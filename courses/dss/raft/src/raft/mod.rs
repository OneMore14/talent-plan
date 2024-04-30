use std::collections::{HashMap, HashSet};
use std::ops::Add;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;
use futures::executor::ThreadPool;

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

const BASIC_TIMEOUT_MS: u64 = 90;
const RPC_RETRY: usize = 10;

type AsyncOneshotSender<T> = oneshot::Sender<T>;

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
    voted_terms: HashSet<u64>,
    get_voted: HashMap<u64, HashSet<usize>>,
    last_receive_heartbeat: Instant,
    last_send_heartbeat: Instant,
    remote_servers: Vec<usize>,
    worker: ThreadPool,
    tx: Sender<RaftMessage>,
    rx: Receiver<RaftMessage>,
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
        let mut rf = Raft {
            peers,
            persister,
            me,
            term: 0,
            voted_terms: HashSet::new(),
            get_voted: HashMap::new(),
            role: Follower,
            last_receive_heartbeat: Instant::now(),
            last_send_heartbeat: Instant::now(),
            n,
            remote_servers,
            worker: ThreadPool::new().unwrap(),
            tx,
            rx,
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
                RaftMessage::LeaderTimeout(duration) => {
                    self.handle_heartbeat_timeout(duration);
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
            }
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
        let vote = if self.has_voted(self.term) {
            false
        } else {
            self.vote_for(vote_request.id as usize);
            true
        };
        let _ = tx.send(RequestVoteReply {
            id: self.me as u64,
            term: self.term,
            vote,
        });
    }

    fn has_voted(&self, term: u64) -> bool {
        self.voted_terms.contains(&term)
    }

    fn handle_append_request(
        &mut self,
        append_request: AppendEntriesArgs,
        tx: AsyncOneshotSender<AppendEntriesReply>,
    ) {
        if append_request.term < self.term {
            let _ = tx.send(AppendEntriesReply {
                id: self.me as u64,
                term: self.term,
                r#type: append_request.r#type,
            });
            return;
        }
        if append_request.term > self.term {
            self.catch_up_term(append_request.term);
        }
        self.refresh_follower_timeout();
        match append_request.r#type.into() {
            RequestType::Heartbeat => {
                let _ = tx.send(AppendEntriesReply {
                    id: self.me as u64,
                    term: self.term,
                    r#type: append_request.r#type,
                });
            }
        }
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
                let _ = tx.send(RaftMessage::FollowerTimeout(next_fire));
            });
        } else {
            self.start_election();
        }
    }

    fn handle_heartbeat_timeout(&mut self, _d: Duration) {
        if !self.is_leader() {
            return;
        }
        self.send_heartbeat();
    }

    fn start_election(&mut self) {
        self.increase_term();
        self.role = Candidate;
        self.vote_for(self.me);

        let vote_args = RequestVoteArgs {
            id: self.me as u64,
            term: self.term,
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
        match reply.r#type.into() {
            RequestType::Heartbeat => {}
        }
    }

    fn vote_for(&mut self, id: usize) {
        self.voted_terms.insert(self.term);
        if id == self.me {
            self.get_voted(self.term, id);
        }
    }

    fn get_voted(&mut self, term: u64, id: usize) {
        self.get_voted
            .entry(term)
            .or_insert(HashSet::new())
            .insert(id);
    }

    fn has_gotten_majority_votes(&self) -> bool {
        let votes = self.get_voted.get(&self.term).map_or(0, |s| s.len());
        votes * 2 > self.n
    }

    fn switch_to_leader(&mut self) {
        self.role = Leader;
        self.send_heartbeat();
    }

    fn send_heartbeat(&mut self) {
        let heartbeat_args = AppendEntriesArgs {
            id: self.me as u64,
            term: self.term,
            r#type: RequestType::Heartbeat as i32,
        };

        for i in self.remote_servers.clone() {
            let rx = self.send_append(i, heartbeat_args.clone());
            let tx = self.tx.clone();
            self.worker.spawn_ok(async move {
                let reply = rx.await;
                if let Ok(reply) = reply {
                    let _ = tx.send(RaftMessage::AppendReply(reply));
                }
            });
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
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
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

    fn send_append(
        &mut self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> oneshot::Receiver<AppendEntriesReply> {
        let (tx, rx) = oneshot::channel::<AppendEntriesReply>();
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            for _ in 0..RPC_RETRY {
                let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    let _ = tx.send(res);
                    return;
                }
            }
        });
        self.last_send_heartbeat = Instant::now();
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
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
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

enum RaftMessage {
    FollowerTimeout(Duration),
    LeaderTimeout(Duration),

    VoteRequest(RequestVoteArgs, AsyncOneshotSender<RequestVoteReply>),
    VoteReply(RequestVoteReply),

    AppendRequest(AppendEntriesArgs, AsyncOneshotSender<AppendEntriesReply>),
    AppendReply(AppendEntriesReply),

    QueryTerm(Sender<u64>),
    QueryLeader(Sender<bool>),
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
                let duration = Duration::from_millis(BASIC_TIMEOUT_MS);
                Delay::new(duration).await;
                let _ = leader_timeout_tx.send(RaftMessage::LeaderTimeout(duration));
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
        crate::your_code_here(command)
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
        reply.map_err(|err| labrpc::Error::Recv(err))
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
        reply.map_err(|err| labrpc::Error::Recv(err))
    }
}

fn random_election_timeout() -> Duration {
    let mut random = rand::thread_rng();
    let timeout_ms: u64 = random.gen::<u64>() % 100 + 10 + BASIC_TIMEOUT_MS;
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
