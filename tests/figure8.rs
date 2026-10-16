//! Hand-scheduled reproduction of the Raft paper's Figure 8: a leader must not
//! commit an entry from a previous term just because it is stored on a
//! majority. Messages are routed manually so the exact interleaving happens.

use raft_kv::raft::{Applied, Config, Node, Role};
use raft_kv::storage::MemStorage;
use raft_kv::types::*;
use std::collections::BTreeMap;

struct Harness {
    nodes: BTreeMap<NodeId, Option<Node<MemStorage>>>,
    storage: BTreeMap<NodeId, MemStorage>,
    queue: Vec<Envelope>,
    applied: BTreeMap<NodeId, Vec<Applied>>,
}

impl Harness {
    fn new(n: u64) -> Self {
        let mut h = Harness {
            nodes: BTreeMap::new(),
            storage: BTreeMap::new(),
            queue: vec![],
            applied: BTreeMap::new(),
        };
        for id in 1..=n {
            h.storage.insert(id, MemStorage::new());
        }
        for id in 1..=n {
            h.start(id);
        }
        h
    }
    fn start(&mut self, id: NodeId) {
        let mut c = Config::new(id, (1..=self.storage.len() as u64).collect());
        c.max_entries_per_append = 1;
        c.snapshot_threshold = 0;
        self.nodes.insert(id, Some(Node::new(c, self.storage[&id].clone())));
    }
    fn node(&mut self, id: NodeId) -> &mut Node<MemStorage> {
        self.nodes.get_mut(&id).unwrap().as_mut().expect("node is up")
    }
    fn collect(&mut self, id: NodeId) {
        if let Some(n) = self.nodes.get_mut(&id).unwrap().as_mut() {
            self.queue.extend(n.take_messages());
            self.applied.entry(id).or_default().extend(n.take_applied());
        }
    }
    /// Ticks `id` until it starts an election.
    fn campaign(&mut self, id: NodeId) {
        let t = self.node(id).term();
        while self.node(id).term() == t {
            self.node(id).tick();
        }
        self.collect(id);
    }
    /// Delivers queued messages whose (from, to) are both in `allowed`, for
    /// `rounds` rounds; everything else is dropped.
    fn deliver(&mut self, allowed: &[NodeId], rounds: usize) {
        for _ in 0..rounds {
            for env in std::mem::take(&mut self.queue) {
                if allowed.contains(&env.from) && allowed.contains(&env.to) && self.nodes[&env.to].is_some() {
                    let to = env.to;
                    self.node(to).step(env);
                    self.collect(to);
                }
            }
        }
    }
    fn crash(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
    }
    fn assert_state_machine_safety(&self) {
        let mut seen: BTreeMap<Index, &Applied> = BTreeMap::new();
        for a in self.applied.values().flatten() {
            if let Some(prev) = seen.insert(a.index, a) {
                assert_eq!(
                    (prev.term, &prev.command),
                    (a.term, &a.command),
                    "two different entries applied at index {}",
                    a.index
                );
            }
        }
    }
}

fn put(v: &str) -> Op {
    Op::Put {
        key: "k".into(),
        value: v.into(),
    }
}

#[test]
fn figure8_previous_term_entry_is_not_committed_by_counting() {
    let all = [1, 2, 3, 4, 5];
    let mut h = Harness::new(5);
    // S1 leads term 1; its no-op (index 1) commits everywhere.
    h.campaign(1);
    h.deliver(&all, 10);
    assert_eq!(h.node(1).role(), Role::Leader);
    // S1 appends X at index 2, replicates it only to S2, then crashes.
    h.node(1).propose(1, 1, put("X")).unwrap();
    h.collect(1);
    h.deliver(&[1, 2], 2);
    h.crash(1);
    // S5 wins with votes from S3, S4 and appends entries only it stores.
    h.campaign(5);
    h.deliver(&[3, 4, 5], 2); // requests + votes; replication messages are dropped below
    assert_eq!(h.node(5).role(), Role::Leader);
    h.node(5).propose(2, 1, put("Y")).unwrap();
    h.queue.clear();
    h.crash(5);
    // S1 restarts and becomes leader again with S2, S3 (S4 is cut off).
    h.start(1);
    while h.node(1).role() != Role::Leader {
        h.campaign(1);
        h.deliver(&[1, 2, 3], 2);
    }
    // Replicate one entry (index 2 = X, from term 1) to S3. X is now on a
    // majority (S1, S2, S3), but S1's current-term no-op is not.
    let mut rounds = 0;
    while h.storage[&3].load_entries().iter().all(|e| e.index < 2) {
        h.deliver(&[1, 2, 3], 1);
        rounds += 1;
        assert!(rounds < 20);
    }
    h.queue.retain(
        |e| !matches!(e.msg, Message::AppendEntries { ref entries, .. } if entries.iter().any(|x| x.index >= 3)),
    );
    h.deliver(&[1, 2, 3], 1); // acks for index 2 reach S1
    assert!(
        h.node(1).commit_index() < 2,
        "leader committed a previous-term entry by counting replicas (Figure 8)"
    );
    // S1 crashes; S5 (with a higher last term) wins and overwrites index 2.
    h.crash(1);
    h.queue.clear();
    h.start(5);
    while h.node(5).role() != Role::Leader {
        h.campaign(5);
        h.deliver(&[2, 3, 4, 5], 2);
    }
    h.deliver(&[2, 3, 4, 5], 20);
    h.assert_state_machine_safety();
    assert!(h.node(5).commit_index() >= 3);
}
