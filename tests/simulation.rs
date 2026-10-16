//! Deterministic simulation tests. Each scenario is a pure function of its
//! seed; on failure the seed is printed so the run can be replayed exactly:
//! `RAFT_SIM_SEED=<seed> cargo test --test simulation <name>`.

use raft_kv::linearizability;
use raft_kv::raft::Role;
use raft_kv::sim::{Sim, SimConfig};

fn seeds(default: std::ops::Range<u64>) -> Vec<u64> {
    if let Ok(s) = std::env::var("RAFT_SIM_SEED") {
        return vec![s.parse().expect("RAFT_SIM_SEED must be a u64")];
    }
    let n: u64 = std::env::var("RAFT_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default.end - default.start);
    let offset: u64 = std::env::var("RAFT_SIM_SEED_OFFSET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    (default.start + offset..default.start + offset + n).collect()
}

/// Heals the cluster, waits for all clients, then checks convergence and
/// linearizability of the full history.
fn finish(mut sim: Sim, seed: u64) -> Sim {
    sim.heal();
    for id in 1..=sim.cfg.nodes {
        sim.restart(id);
    }
    assert!(
        sim.drain_clients(5_000),
        "seed {seed}: clients did not complete after healing (liveness)"
    );
    assert!(sim.run_until_converged(5_000), "nodes did not converge");
    sim.assert_converged();
    if let Err(key) = linearizability::check(&sim.history) {
        panic!("seed {seed}: history for key {key} is not linearizable");
    }
    sim
}

fn with_seed<F: Fn(u64) + std::panic::RefUnwindSafe>(seeds: Vec<u64>, f: F) {
    for seed in seeds {
        if let Err(e) = std::panic::catch_unwind(|| f(seed)) {
            eprintln!("FAILED with seed {seed}; replay with RAFT_SIM_SEED={seed}");
            std::panic::resume_unwind(e);
        }
    }
}

#[test]
fn elects_single_leader_and_replicates() {
    let mut sim = Sim::new(SimConfig {
        seed: 7,
        ..Default::default()
    });
    sim.run(100);
    let leader = sim.leader().expect("a leader is elected");
    let followers = (1..=5)
        .filter(|id| sim.node(*id).unwrap().role() == Role::Follower)
        .count();
    assert_eq!(followers, 4, "leader {leader}, all others follow");
    sim.run(500);
    let sim = finish(sim, 7);
    assert!(sim.stats.completed_ops > 100, "made progress: {:?}", sim.stats);
}

#[test]
fn leader_crash_triggers_failover_without_losing_writes() {
    with_seed(seeds(0..20), |seed| {
        let mut sim = Sim::new(SimConfig {
            seed,
            ..Default::default()
        });
        sim.run(100);
        for _ in 0..5 {
            let old = sim.leader().expect("leader");
            let term = sim.node(old).unwrap().term();
            sim.crash(old);
            sim.run(150);
            let new = sim.leader().expect("new leader elected after crash");
            assert_ne!(new, old);
            assert!(sim.node(new).unwrap().term() > term);
            sim.restart(old);
            sim.run(50);
        }
        finish(sim, seed);
    });
}

#[test]
fn isolated_leader_cannot_commit_and_majority_moves_on() {
    with_seed(seeds(0..20), |seed| {
        let mut sim = Sim::new(SimConfig {
            seed,
            ..Default::default()
        });
        sim.run(100);
        let old = sim.leader().expect("leader");
        let old_commit = sim.node(old).unwrap().commit_index();
        sim.partition(&[&[old]]);
        sim.run(300);
        // The old leader may still accept proposals but must not commit them.
        assert_eq!(
            sim.node(old).unwrap().commit_index(),
            old_commit,
            "seed {seed}: minority leader committed"
        );
        let new = sim.leader().expect("majority elects a leader");
        assert_ne!(new, old);
        let new_commit = sim.node(new).unwrap().commit_index();
        assert!(new_commit > old_commit, "majority side made progress");
        finish(sim, seed);
    });
}

#[test]
fn random_partitions_crashes_drops_are_safe_and_linearizable() {
    with_seed(seeds(0..40), |seed| {
        let mut sim = Sim::new(SimConfig {
            seed,
            drop_rate: 0.05,
            max_delay: 5,
            ..Default::default()
        });
        sim.run_with_nemesis(3_000);
        let sim = finish(sim, seed);
        assert!(sim.stats.completed_ops > 0);
    });
}

#[test]
fn snapshots_and_install_snapshot_under_faults() {
    with_seed(seeds(100..120), |seed| {
        let mut sim = Sim::new(SimConfig {
            seed,
            snapshot_threshold: 20,
            drop_rate: 0.02,
            ..Default::default()
        });
        sim.run(100);
        // Keep one follower down while the log is compacted past it.
        let lagger = (1..=5).find(|id| Some(*id) != sim.leader()).unwrap();
        sim.crash(lagger);
        sim.run(800);
        sim.restart(lagger);
        sim.run_with_nemesis(1_500);
        let sim = finish(sim, seed);
        assert!(
            sim.node(lagger).unwrap().snapshot_index() > 0,
            "seed {seed}: lagger compacted"
        );
        assert!(
            sim.stats.snapshots_installed > 0,
            "seed {seed}: InstallSnapshot exercised: {:?}",
            sim.stats
        );
    });
}

#[test]
fn three_node_cluster_with_heavy_loss() {
    with_seed(seeds(200..215), |seed| {
        let mut sim = Sim::new(SimConfig {
            seed,
            nodes: 3,
            drop_rate: 0.2,
            max_delay: 8,
            client_timeout: 60,
            ..Default::default()
        });
        sim.run_with_nemesis(2_000);
        finish(sim, seed);
    });
}

/// Very frequent crashes/restarts and partitions with short timeouts. This is
/// what catches subtle persistence and commit-rule bugs (e.g. not persisting
/// votes, or committing previous-term entries by counting replicas, Raft
/// paper Figure 8).
#[test]
fn chaos_frequent_crash_restart() {
    with_seed(seeds(300..340), |seed| {
        let mut sim = Sim::new(SimConfig {
            seed,
            chaos: 20,
            drop_rate: 0.05,
            max_delay: 6,
            ..Default::default()
        });
        sim.run_with_nemesis(4_000);
        finish(sim, seed);
    });
}

#[test]
fn same_seed_same_run() {
    let run = || {
        let mut s = Sim::new(SimConfig {
            seed: 99,
            drop_rate: 0.1,
            ..Default::default()
        });
        s.run_with_nemesis(1_000);
        (s.stats.messages, s.stats.completed_ops, s.history.len(), s.leader())
    };
    assert_eq!(run(), run(), "simulation must be deterministic");
}
