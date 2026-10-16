//! Deterministic cluster simulator.
//!
//! Everything (the network, the scheduling of clients, fault injection and
//! every node's election timer) is driven by one seeded [`Rng`], so a seed
//! fully determines the run. The simulator checks Raft safety invariants on
//! every tick and records a client history for the linearizability checker.

use crate::linearizability::Operation;
use crate::raft::{Config, Node, ProposeError, Role};
use crate::rng::Rng;
use crate::storage::MemStorage;
use crate::types::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: u64,
    pub seed: u64,
    pub clients: u64,
    pub keys: u64,
    /// Probability a message is lost.
    pub drop_rate: f64,
    /// Message delay in ticks, uniform in [min_delay, max_delay].
    pub min_delay: u64,
    pub max_delay: u64,
    pub snapshot_threshold: u64,
    /// Client resends to another node after this many ticks without a response.
    pub client_timeout: u64,
    /// Fault-injection intensity multiplier for the nemesis (1 = default).
    pub chaos: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 5,
            seed: 1,
            clients: 4,
            keys: 3,
            drop_rate: 0.0,
            min_delay: 1,
            max_delay: 3,
            snapshot_threshold: 0,
            client_timeout: 40,
            chaos: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Addr {
    Node(NodeId),
    Client(ClientId),
}

#[derive(Clone, Debug)]
enum Payload {
    Raft(Message),
    Request {
        client: ClientId,
        seq: u64,
        op: Op,
    },
    Response {
        seq: u64,
        result: Result<Response, Option<NodeId>>,
    },
}

struct InFlight {
    at: u64,
    from: Addr,
    to: Addr,
    payload: Payload,
}

struct ClientState {
    seq: u64,
    /// Index into history of the outstanding op.
    pending: Option<usize>,
    target: NodeId,
    sent_at: u64,
}

pub struct Sim {
    pub cfg: SimConfig,
    rng: Rng,
    pub now: u64,
    events: u64,
    nodes: BTreeMap<NodeId, Option<Node<MemStorage>>>,
    storage: BTreeMap<NodeId, MemStorage>,
    /// Requests each live node will answer once applied.
    waiting: BTreeMap<NodeId, BTreeSet<(ClientId, u64)>>,
    group: BTreeMap<NodeId, u32>,
    net: Vec<InFlight>,
    clients: BTreeMap<ClientId, ClientState>,
    pub history: Vec<Operation>,
    // safety bookkeeping
    leaders: BTreeMap<Term, NodeId>,
    committed: BTreeMap<Index, (Term, Command)>,
    pub stats: Stats,
    issuing: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub messages: u64,
    pub dropped: u64,
    pub elections_won: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub completed_ops: u64,
    pub snapshots_installed: u64,
}

impl Sim {
    pub fn new(cfg: SimConfig) -> Self {
        let mut s = Sim {
            rng: Rng::new(cfg.seed),
            now: 0,
            events: 0,
            nodes: BTreeMap::new(),
            storage: BTreeMap::new(),
            waiting: BTreeMap::new(),
            group: BTreeMap::new(),
            net: Vec::new(),
            clients: BTreeMap::new(),
            history: Vec::new(),
            leaders: BTreeMap::new(),
            committed: BTreeMap::new(),
            stats: Stats::default(),
            issuing: true,
            cfg,
        };
        for id in 1..=s.cfg.nodes {
            s.storage.insert(id, MemStorage::new());
            s.group.insert(id, 0);
            s.start(id);
        }
        for c in 1..=s.cfg.clients {
            s.clients.insert(
                c,
                ClientState {
                    seq: 0,
                    pending: None,
                    target: 1,
                    sent_at: 0,
                },
            );
        }
        s
    }

    fn members(&self) -> Vec<NodeId> {
        (1..=self.cfg.nodes).collect()
    }

    fn start(&mut self, id: NodeId) {
        let mut c = Config::new(id, self.members());
        c.snapshot_threshold = self.cfg.snapshot_threshold;
        c.max_entries_per_append = 8;
        c.seed = self.rng.next_u64();
        let node = Node::new(c, self.storage[&id].clone());
        self.nodes.insert(id, Some(node));
        self.waiting.insert(id, BTreeSet::new());
    }

    // ---------- fault injection ----------

    pub fn crash(&mut self, id: NodeId) {
        if self.nodes[&id].is_some() {
            self.nodes.insert(id, None);
            self.waiting.insert(id, BTreeSet::new());
            self.stats.crashes += 1;
        }
    }
    pub fn restart(&mut self, id: NodeId) {
        if self.nodes[&id].is_none() {
            self.start(id);
        }
    }
    pub fn is_up(&self, id: NodeId) -> bool {
        self.nodes[&id].is_some()
    }
    /// Places each listed node in its own group; unlisted nodes stay in group 0.
    pub fn partition(&mut self, groups: &[&[NodeId]]) {
        for g in self.group.values_mut() {
            *g = 0;
        }
        for (i, ids) in groups.iter().enumerate() {
            for id in *ids {
                self.group.insert(*id, i as u32 + 1);
            }
        }
        self.stats.partitions += 1;
    }
    pub fn heal(&mut self) {
        for g in self.group.values_mut() {
            *g = 0;
        }
    }
    pub fn leader(&self) -> Option<NodeId> {
        // The live leader with the highest term (a deposed one may linger).
        self.nodes
            .iter()
            .filter_map(|(id, n)| n.as_ref().filter(|n| n.role() == Role::Leader).map(|n| (n.term(), *id)))
            .max()
            .map(|(_, id)| id)
    }
    pub fn node(&self, id: NodeId) -> Option<&Node<MemStorage>> {
        self.nodes[&id].as_ref()
    }

    /// Random nemesis: occasionally crashes/restarts nodes (never more than a
    /// minority down at once) and creates/heals random partitions.
    pub fn nemesis(&mut self) {
        let n = self.cfg.nodes;
        let r = self.rng.below(1000) / self.cfg.chaos.max(1);
        if r < 4 {
            let down = self.nodes.values().filter(|x| x.is_none()).count() as u64;
            if down < (n - 1) / 2 {
                // prefer killing the leader half the time
                let id = match self.leader() {
                    Some(l) if self.rng.chance(0.5) => l,
                    _ => 1 + self.rng.below(n),
                };
                self.crash(id);
            }
        } else if r < 12 {
            let down: Vec<_> = self
                .nodes
                .iter()
                .filter(|(_, x)| x.is_none())
                .map(|(i, _)| *i)
                .collect();
            if !down.is_empty() {
                let id = down[self.rng.below(down.len() as u64) as usize];
                self.restart(id);
            }
        } else if r < 16 {
            let mut ids = self.members();
            for i in (1..ids.len()).rev() {
                ids.swap(i, self.rng.below(i as u64 + 1) as usize);
            }
            let cut = 1 + self.rng.below(n - 1) as usize;
            let (a, b) = ids.split_at(cut);
            self.partition(&[a, b]);
        } else if r < 22 {
            self.heal();
        }
    }

    // ---------- network ----------

    fn connected(&self, a: Addr, b: Addr) -> bool {
        match (a, b) {
            (Addr::Node(x), Addr::Node(y)) => self.group[&x] == self.group[&y],
            _ => true, // clients reach every node
        }
    }

    fn send(&mut self, from: Addr, to: Addr, payload: Payload) {
        self.stats.messages += 1;
        if self.rng.chance(self.cfg.drop_rate) {
            self.stats.dropped += 1;
            return;
        }
        let delay = self.cfg.min_delay + self.rng.below(self.cfg.max_delay - self.cfg.min_delay + 1);
        self.net.push(InFlight {
            at: self.now + delay,
            from,
            to,
            payload,
        });
    }

    // ---------- main loop ----------

    /// Advances the simulation by one tick.
    pub fn step(&mut self) {
        self.now += 1;
        let ids = self.members();
        for id in &ids {
            if let Some(n) = self.nodes.get_mut(id).unwrap().as_mut() {
                n.tick();
            }
            self.flush(*id);
        }
        // Deliver due messages in a seeded random order (reordering).
        let mut due = Vec::new();
        let mut i = 0;
        while i < self.net.len() {
            if self.net[i].at <= self.now {
                due.push(self.net.swap_remove(i));
            } else {
                i += 1;
            }
        }
        due.sort_by_key(|m| (m.at, m.from, m.to));
        for i in (1..due.len()).rev() {
            due.swap(i, self.rng.below(i as u64 + 1) as usize);
        }
        for m in due {
            self.deliver(m);
        }
        self.drive_clients();
        self.check_invariants();
    }

    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    pub fn run_with_nemesis(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.nemesis();
            self.step();
        }
    }

    fn deliver(&mut self, m: InFlight) {
        if !self.connected(m.from, m.to) {
            self.stats.dropped += 1;
            return;
        }
        match (m.to, m.payload) {
            (Addr::Node(id), Payload::Raft(msg)) => {
                let Addr::Node(from) = m.from else { unreachable!() };
                if let Some(n) = self.nodes.get_mut(&id).unwrap().as_mut() {
                    let before = n.snapshot_index();
                    let install = matches!(msg, Message::InstallSnapshot { .. });
                    n.step(Envelope { from, to: id, msg });
                    if install && n.snapshot_index() > before {
                        self.stats.snapshots_installed += 1;
                    }
                }
                self.flush(id);
            }
            (Addr::Node(id), Payload::Request { client, seq, op }) => {
                let Some(n) = self.nodes.get_mut(&id).unwrap().as_mut() else {
                    return;
                };
                match n.propose(client, seq, op) {
                    Ok(_) => {
                        self.waiting.get_mut(&id).unwrap().insert((client, seq));
                    }
                    Err(ProposeError::NotLeader { leader }) => {
                        self.send(
                            m.to,
                            m.from,
                            Payload::Response {
                                seq,
                                result: Err(leader),
                            },
                        );
                    }
                }
                self.flush(id);
            }
            (Addr::Client(c), Payload::Response { seq, result }) => self.client_response(c, seq, result),
            _ => unreachable!("misrouted message"),
        }
    }

    /// Moves a node's outputs into the network and checks applied entries.
    fn flush(&mut self, id: NodeId) {
        let Some(n) = self.nodes.get_mut(&id).unwrap().as_mut() else {
            return;
        };
        let msgs = n.take_messages();
        let applied = n.take_applied();
        if n.role() == Role::Leader {
            let (t, me) = (n.term(), n.id());
            match self.leaders.get(&t) {
                Some(other) => assert_eq!(*other, me, "election safety: two leaders in term {t}"),
                None => {
                    self.leaders.insert(t, me);
                    self.stats.elections_won += 1;
                }
            }
        }
        for env in msgs {
            self.send(Addr::Node(env.from), Addr::Node(env.to), Payload::Raft(env.msg));
        }
        for a in applied {
            // State machine safety: every node applies the same entry at an index.
            match self.committed.get(&a.index) {
                Some((t, c)) => assert!(
                    *t == a.term && *c == a.command,
                    "state machine safety violated at index {}: {:?} vs {:?}",
                    a.index,
                    (t, c),
                    (a.term, &a.command)
                ),
                None => {
                    self.committed.insert(a.index, (a.term, a.command.clone()));
                }
            }
            let (Command::Client { client, seq, .. }, Some(resp)) = (&a.command, a.response) else {
                continue;
            };
            if self.waiting.get_mut(&id).unwrap().remove(&(*client, *seq)) {
                self.send(
                    Addr::Node(id),
                    Addr::Client(*client),
                    Payload::Response {
                        seq: *seq,
                        result: Ok(resp),
                    },
                );
            }
        }
    }

    // ---------- clients ----------

    fn drive_clients(&mut self) {
        let ids: Vec<_> = self.clients.keys().copied().collect();
        for c in ids {
            let st = &self.clients[&c];
            match st.pending {
                None => {
                    if self.issuing && self.rng.chance(0.3) {
                        self.issue(c);
                    }
                }
                Some(h) => {
                    if self.now - st.sent_at >= self.cfg.client_timeout {
                        let target = 1 + self.rng.below(self.cfg.nodes);
                        self.resend(c, h, target);
                    }
                }
            }
        }
    }

    fn issue(&mut self, c: ClientId) {
        let key = format!("k{}", self.rng.below(self.cfg.keys));
        let seq = self.clients[&c].seq + 1;
        let op = match self.rng.below(10) {
            0..=4 => Op::Get { key },
            5..=8 => Op::Put {
                key,
                value: format!("c{c}s{seq}"),
            },
            _ => Op::Delete { key },
        };
        self.events += 1;
        self.history.push(Operation {
            client: c,
            op,
            call: self.events,
            ret: None,
            result: None,
        });
        let h = self.history.len() - 1;
        let st = self.clients.get_mut(&c).unwrap();
        st.seq = seq;
        st.pending = Some(h);
        let target = st.target;
        self.resend(c, h, target);
    }

    fn resend(&mut self, c: ClientId, h: usize, target: NodeId) {
        let st = self.clients.get_mut(&c).unwrap();
        st.target = target;
        st.sent_at = self.now;
        let seq = st.seq;
        let op = self.history[h].op.clone();
        self.send(
            Addr::Client(c),
            Addr::Node(target),
            Payload::Request { client: c, seq, op },
        );
    }

    fn client_response(&mut self, c: ClientId, seq: u64, result: Result<Response, Option<NodeId>>) {
        let st = &self.clients[&c];
        let Some(h) = st.pending else { return };
        if seq != st.seq {
            return; // late duplicate
        }
        match result {
            Ok(resp) => {
                self.events += 1;
                self.history[h].ret = Some(self.events);
                self.history[h].result = Some(resp);
                self.clients.get_mut(&c).unwrap().pending = None;
                self.stats.completed_ops += 1;
            }
            Err(hint) => {
                let target = hint.unwrap_or_else(|| 1 + self.rng.below(self.cfg.nodes));
                self.resend(c, h, target);
            }
        }
    }

    pub fn clients_idle(&self) -> bool {
        self.clients.values().all(|c| c.pending.is_none())
    }

    /// Stops issuing new operations (permanently) and runs until every
    /// outstanding one has completed or `max_ticks` elapse. Returns true if
    /// all completed.
    pub fn drain_clients(&mut self, max_ticks: u64) -> bool {
        self.issuing = false;
        for _ in 0..max_ticks {
            if self.clients_idle() {
                return true;
            }
            self.step();
        }
        self.clients_idle()
    }

    // ---------- invariants ----------

    fn check_invariants(&self) {
        for n in self.nodes.values().flatten() {
            assert!(n.applied_index() <= n.commit_index());
            assert!(n.commit_index() <= n.last_index());
        }
    }

    fn converged(&self) -> bool {
        let live: Vec<_> = self.nodes.values().flatten().collect();
        live.windows(2)
            .all(|w| w[0].applied_index() == w[1].applied_index() && w[0].state_machine() == w[1].state_machine())
    }

    /// Runs until all live nodes have applied the same prefix (heartbeats
    /// propagate the commit index), or `max_ticks` elapse.
    pub fn run_until_converged(&mut self, max_ticks: u64) -> bool {
        for _ in 0..max_ticks {
            if self.converged() && self.leader().is_some() {
                return true;
            }
            self.step();
        }
        self.converged()
    }

    /// After quiescence: every live node has applied the same prefix and holds
    /// an identical state machine.
    pub fn assert_converged(&self) {
        let live: Vec<_> = self.nodes.values().flatten().collect();
        let first = live[0];
        for n in &live {
            assert_eq!(
                n.applied_index(),
                first.applied_index(),
                "node {} applied index differs",
                n.id()
            );
            assert_eq!(
                n.state_machine(),
                first.state_machine(),
                "node {} state differs",
                n.id()
            );
        }
    }
}
