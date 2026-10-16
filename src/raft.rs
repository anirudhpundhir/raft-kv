//! The Raft core as a deterministic, single-threaded state machine.
//!
//! It performs no I/O of its own beyond calling [`Storage`] synchronously.
//! Time advances only through [`Node::tick`]; messages arrive only through
//! [`Node::step`]; outputs are drained with [`Node::take_messages`] and
//! [`Node::take_applied`]. The same code therefore runs inside the
//! deterministic simulator and inside the TCP server.

use crate::kv::KvStore;
use crate::rng::Rng;
use crate::storage::Storage;
use crate::types::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    /// All cluster members, including `id`.
    pub members: Vec<NodeId>,
    /// Election timeout is drawn uniformly from `[election_ticks, 2*election_ticks)`.
    pub election_ticks: u64,
    pub heartbeat_ticks: u64,
    /// Take a snapshot once this many applied entries are in the log (0 = never).
    pub snapshot_threshold: u64,
    pub max_entries_per_append: usize,
    pub seed: u64,
}

impl Config {
    pub fn new(id: NodeId, members: Vec<NodeId>) -> Self {
        Self {
            id,
            members,
            election_ticks: 10,
            heartbeat_ticks: 2,
            snapshot_threshold: 1000,
            max_entries_per_append: 64,
            seed: id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A committed entry that was applied to the state machine on this node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Applied {
    pub index: Index,
    pub term: Term,
    pub command: Command,
    pub response: Option<Response>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader { leader: Option<NodeId> },
}

pub struct Node<S: Storage> {
    cfg: Config,
    storage: S,
    rng: Rng,

    // persistent (mirrored in storage)
    term: Term,
    voted_for: Option<NodeId>,
    snap_index: Index,
    snap_term: Term,
    snapshot: Option<Snapshot>,
    /// entries[i].index == snap_index + 1 + i
    log: Vec<Entry>,

    // volatile
    role: Role,
    leader: Option<NodeId>,
    commit: Index,
    applied: Index,
    sm: KvStore,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    votes: BTreeSet<NodeId>,
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,
    /// Peers with an unacknowledged AppendEntries carrying entries. New
    /// proposals are batched behind it instead of resending the same suffix
    /// once per proposal; heartbeats still retransmit in case it was lost.
    inflight: BTreeSet<NodeId>,
    /// Peers heard from since the last check-quorum round (leader only).
    recent_active: BTreeSet<NodeId>,

    outbox: Vec<Envelope>,
    applied_out: Vec<Applied>,
}

impl<S: Storage> Node<S> {
    /// Creates a node, recovering any persisted state from `storage`.
    pub fn new(cfg: Config, mut storage: S) -> Self {
        assert!(cfg.members.contains(&cfg.id), "members must include self");
        let rec = storage.load().expect("storage load failed");
        let mut rng = Rng::new(cfg.seed);
        let election_timeout = cfg.election_ticks + rng.below(cfg.election_ticks);
        let (snap_index, snap_term, sm) = match &rec.snapshot {
            Some(s) => (s.index, s.term, KvStore::from_bytes(&s.data)),
            None => (0, 0, KvStore::default()),
        };
        Node {
            storage,
            rng,
            term: rec.hard_state.term,
            voted_for: rec.hard_state.voted_for,
            snap_index,
            snap_term,
            snapshot: rec.snapshot,
            log: rec.entries,
            role: Role::Follower,
            leader: None,
            commit: snap_index,
            applied: snap_index,
            sm,
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            recent_active: BTreeSet::new(),
            inflight: BTreeSet::new(),
            outbox: Vec::new(),
            applied_out: Vec::new(),
            cfg,
        }
    }

    // ---------- accessors ----------

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> Index {
        self.commit
    }
    pub fn applied_index(&self) -> Index {
        self.applied
    }
    pub fn snapshot_index(&self) -> Index {
        self.snap_index
    }
    pub fn state_machine(&self) -> &KvStore {
        &self.sm
    }
    pub fn last_index(&self) -> Index {
        self.snap_index + self.log.len() as Index
    }
    pub fn take_messages(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }
    pub fn take_applied(&mut self) -> Vec<Applied> {
        std::mem::take(&mut self.applied_out)
    }

    fn last_term(&self) -> Term {
        self.log.last().map_or(self.snap_term, |e| e.term)
    }
    fn term_at(&self, i: Index) -> Option<Term> {
        if i == self.snap_index {
            return Some(self.snap_term);
        }
        if i < self.snap_index || i > self.last_index() {
            return None;
        }
        Some(self.log[(i - self.snap_index - 1) as usize].term)
    }
    fn entry(&self, i: Index) -> &Entry {
        &self.log[(i - self.snap_index - 1) as usize]
    }
    fn quorum(&self) -> usize {
        self.cfg.members.len() / 2 + 1
    }
    fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        let me = self.cfg.id;
        self.cfg.members.iter().copied().filter(move |p| *p != me)
    }
    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Envelope {
            from: self.cfg.id,
            to,
            msg,
        });
    }

    // ---------- persistence ----------

    fn persist_hard_state(&mut self) {
        let hs = HardState {
            term: self.term,
            voted_for: self.voted_for,
        };
        self.storage.save_hard_state(&hs).expect("persist hard state");
    }
    fn append_entries_local(&mut self, entries: Vec<Entry>) {
        self.storage.append(&entries).expect("persist entries");
        self.log.extend(entries);
    }
    fn truncate_local(&mut self, from: Index) {
        assert!(
            from > self.commit,
            "attempt to truncate committed entry {from} (commit {})",
            self.commit
        );
        self.storage.truncate_from(from).expect("truncate log");
        self.log.truncate((from - self.snap_index - 1) as usize);
    }

    // ---------- role transitions ----------

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.cfg.election_ticks + self.rng.below(self.cfg.election_ticks);
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.persist_hard_state();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.reset_election_timer();
    }

    fn campaign(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.leader = None;
        self.voted_for = Some(self.cfg.id);
        self.persist_hard_state();
        self.votes = BTreeSet::from([self.cfg.id]);
        self.reset_election_timer();
        if self.votes.len() >= self.quorum() {
            self.become_leader();
            return;
        }
        let (term, lli, llt) = (self.term, self.last_index(), self.last_term());
        let peers: Vec<_> = self.peers().collect();
        for p in peers {
            self.send(
                p,
                Message::RequestVote {
                    term,
                    last_log_index: lli,
                    last_log_term: llt,
                },
            );
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.recent_active.clear();
        self.inflight.clear();
        let next = self.last_index() + 1;
        let peers: Vec<_> = self.peers().collect();
        self.next_index = peers.iter().map(|p| (*p, next)).collect();
        self.match_index = peers.iter().map(|p| (*p, 0)).collect();
        // Commit a no-op from the new term so earlier-term entries commit (§5.4.2).
        let e = Entry {
            term: self.term,
            index: next,
            command: Command::Noop,
        };
        self.append_entries_local(vec![e]);
        self.maybe_commit();
        self.broadcast_append();
    }

    // ---------- public driving API ----------

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
                // Check quorum: step down if a majority has been silent for a
                // full election timeout, so clients stop waiting on a deposed
                // (partitioned) leader.
                self.election_elapsed += 1;
                if self.election_elapsed >= self.cfg.election_ticks * 2 {
                    self.election_elapsed = 0;
                    let active = self.recent_active.len() + 1;
                    self.recent_active.clear();
                    if active < self.quorum() {
                        let t = self.term;
                        self.become_follower(t, None);
                    }
                }
            }
            _ => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.campaign();
                }
            }
        }
    }

    /// Proposes a client operation. Returns the log index it was appended at.
    /// The response is reported through [`Node::take_applied`] once committed.
    pub fn propose(&mut self, client: ClientId, seq: u64, op: Op) -> Result<Index, ProposeError> {
        self.propose_batch(vec![(client, seq, op)])
    }

    /// Proposes several operations with a single durable append (one fsync:
    /// group commit). Returns the index of the first one.
    pub fn propose_batch(&mut self, ops: Vec<(ClientId, u64, Op)>) -> Result<Index, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader { leader: self.leader });
        }
        let first = self.last_index() + 1;
        let term = self.term;
        let entries = ops
            .into_iter()
            .enumerate()
            .map(|(i, (client, seq, op))| Entry {
                term,
                index: first + i as Index,
                command: Command::Client { client, seq, op },
            })
            .collect();
        self.append_entries_local(entries);
        self.maybe_commit(); // single-node cluster
        let idle: Vec<_> = self.peers().filter(|p| !self.inflight.contains(p)).collect();
        for p in idle {
            self.send_append(p);
        }
        Ok(first)
    }

    /// Handles a message from a peer.
    #[allow(clippy::comparison_chain)]
    pub fn step(&mut self, env: Envelope) {
        let from = env.from;
        let mterm = env.msg.term();
        if mterm > self.term {
            let leader = match env.msg {
                Message::AppendEntries { .. } | Message::InstallSnapshot { .. } => Some(from),
                _ => None,
            };
            self.become_follower(mterm, leader);
        } else if mterm < self.term {
            // Stale sender: tell it our term so it steps down. Responses are dropped.
            match env.msg {
                Message::RequestVote { .. } => {
                    let term = self.term;
                    self.send(from, Message::RequestVoteResponse { term, granted: false });
                }
                Message::AppendEntries { .. } | Message::InstallSnapshot { .. } => {
                    let term = self.term;
                    self.send(
                        from,
                        Message::AppendEntriesResponse {
                            term,
                            success: false,
                            index: 0,
                        },
                    );
                }
                _ => {}
            }
            return;
        }

        match env.msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                let up_to_date = (last_log_term, last_log_index) >= (self.last_term(), self.last_index());
                let can_vote = self.voted_for.is_none_or(|v| v == from) && self.leader.is_none();
                let granted = can_vote && up_to_date;
                if granted {
                    self.voted_for = Some(from);
                    self.persist_hard_state();
                    self.reset_election_timer();
                }
                self.send(from, Message::RequestVoteResponse { term, granted });
            }
            Message::RequestVoteResponse { granted, .. } => {
                if self.role == Role::Candidate && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.quorum() {
                        self.become_leader();
                    }
                }
            }
            Message::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                ..
            } => {
                self.observe_leader(from);
                let resp = self.handle_append(prev_log_index, prev_log_term, entries, leader_commit);
                self.send(from, resp);
            }
            Message::InstallSnapshot { snapshot, .. } => {
                self.observe_leader(from);
                let resp = self.handle_snapshot(snapshot);
                self.send(from, resp);
            }
            Message::AppendEntriesResponse { success, index, .. } => {
                if self.role == Role::Leader {
                    self.handle_append_response(from, success, index);
                }
            }
        }
    }

    fn observe_leader(&mut self, from: NodeId) {
        // Same term (higher was handled in step). Two leaders in one term are
        // impossible, so a candidate simply yields.
        debug_assert!(self.role != Role::Leader, "two leaders in term {}", self.term);
        self.role = Role::Follower;
        self.leader = Some(from);
        self.reset_election_timer();
    }

    // ---------- replication: follower side ----------

    fn handle_append(&mut self, prev: Index, prev_term: Term, entries: Vec<Entry>, leader_commit: Index) -> Message {
        let term = self.term;
        if prev > self.last_index() {
            return Message::AppendEntriesResponse {
                term,
                success: false,
                index: self.last_index() + 1,
            };
        }
        if prev >= self.snap_index && self.term_at(prev) != Some(prev_term) {
            // Skip back over the whole conflicting term in one round trip.
            let bad = self.term_at(prev).expect("prev in range");
            let mut i = prev;
            while i > self.snap_index + 1 && i - 1 > self.commit && self.term_at(i - 1) == Some(bad) {
                i -= 1;
            }
            return Message::AppendEntriesResponse {
                term,
                success: false,
                index: i.max(self.commit + 1),
            };
        }
        // prev matches (or precedes our snapshot, which only holds committed entries).
        let last_new = prev + entries.len() as Index;
        let mut to_append = Vec::new();
        for e in entries {
            if e.index <= self.snap_index {
                continue;
            }
            if !to_append.is_empty() {
                to_append.push(e);
                continue;
            }
            match self.term_at(e.index) {
                Some(t) if t == e.term => {}
                Some(_) => {
                    self.truncate_local(e.index);
                    to_append.push(e);
                }
                None => to_append.push(e),
            }
        }
        if !to_append.is_empty() {
            self.append_entries_local(to_append);
        }
        let matched = last_new.max(self.snap_index);
        if leader_commit > self.commit {
            // Only entries verified by *this* message are known to match.
            self.commit = leader_commit.min(matched).max(self.commit);
            self.apply_committed();
        }
        Message::AppendEntriesResponse {
            term,
            success: true,
            index: matched,
        }
    }

    fn handle_snapshot(&mut self, snap: Snapshot) -> Message {
        let term = self.term;
        if snap.index <= self.commit {
            return Message::AppendEntriesResponse {
                term,
                success: true,
                index: self.commit,
            };
        }
        let index = snap.index;
        if self.term_at(snap.index) == Some(snap.term) {
            // Keep the suffix after the snapshot.
            let keep = (snap.index - self.snap_index) as usize;
            self.log.drain(..keep);
        } else {
            self.storage.truncate_from(self.snap_index + 1).expect("truncate log");
            self.log.clear();
        }
        self.storage.save_snapshot(&snap).expect("persist snapshot");
        self.sm = KvStore::from_bytes(&snap.data);
        self.snap_index = snap.index;
        self.snap_term = snap.term;
        self.snapshot = Some(snap);
        self.commit = index;
        self.applied = index;
        Message::AppendEntriesResponse {
            term,
            success: true,
            index,
        }
    }

    // ---------- replication: leader side ----------

    fn broadcast_append(&mut self) {
        let peers: Vec<_> = self.peers().collect();
        for p in peers {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let next = self.next_index[&peer];
        let prev = next - 1;
        let term = self.term;
        match self.term_at(prev) {
            None => {
                // The entries the peer needs were compacted away.
                let snapshot = self.snapshot.clone().expect("compacted log implies snapshot");
                self.send(peer, Message::InstallSnapshot { term, snapshot });
            }
            Some(prev_term) => {
                let end = self.last_index().min(prev + self.cfg.max_entries_per_append as Index);
                let entries = ((prev + 1)..=end).map(|i| self.entry(i).clone()).collect();
                let leader_commit = self.commit;
                self.send(
                    peer,
                    Message::AppendEntries {
                        term,
                        prev_log_index: prev,
                        prev_log_term: prev_term,
                        entries,
                        leader_commit,
                    },
                );
            }
        }
    }

    fn handle_append_response(&mut self, peer: NodeId, success: bool, index: Index) {
        self.recent_active.insert(peer);
        self.inflight.remove(&peer);
        if success {
            let m = self.match_index.get_mut(&peer).expect("known peer");
            if index > *m {
                *m = index;
            }
            let n = self.next_index.get_mut(&peer).expect("known peer");
            *n = (*n).max(index + 1);
            self.maybe_commit();
            if self.next_index[&peer] <= self.last_index() {
                self.send_append(peer);
            }
        } else if index > 0 {
            let floor = self.match_index[&peer] + 1;
            let n = index.max(floor).min(self.last_index() + 1);
            self.next_index.insert(peer, n);
            self.send_append(peer);
        }
    }

    fn maybe_commit(&mut self) {
        let mut n = self.last_index();
        while n > self.commit {
            // Only entries from the current term commit by counting (§5.4.2).
            if self.term_at(n) != Some(self.term) {
                break;
            }
            let acks = 1 + self.match_index.values().filter(|m| **m >= n).count();
            if acks >= self.quorum() {
                self.commit = n;
                self.apply_committed();
                return;
            }
            n -= 1;
        }
    }

    // ---------- state machine ----------

    fn apply_committed(&mut self) {
        while self.applied < self.commit {
            self.applied += 1;
            let e = self.entry(self.applied).clone();
            let response = self.sm.apply(&e.command);
            self.applied_out.push(Applied {
                index: e.index,
                term: e.term,
                command: e.command,
                response,
            });
        }
        self.maybe_snapshot();
    }

    fn maybe_snapshot(&mut self) {
        let th = self.cfg.snapshot_threshold;
        if th == 0 || self.applied - self.snap_index < th {
            return;
        }
        let index = self.applied;
        let term = self.term_at(index).expect("applied in log");
        let snap = Snapshot {
            index,
            term,
            data: self.sm.to_bytes(),
        };
        self.storage.save_snapshot(&snap).expect("persist snapshot");
        let drop = (index - self.snap_index) as usize;
        self.log.drain(..drop);
        self.snap_index = index;
        self.snap_term = term;
        self.snapshot = Some(snap);
    }
}
