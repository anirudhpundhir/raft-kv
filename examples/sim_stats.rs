//! Prints statistics for one simulated run: `cargo run --release --example sim_stats -- <seed> <ticks>`.
use raft_kv::sim::{Sim, SimConfig};

fn main() {
    let mut args = std::env::args().skip(1);
    let seed = args.next().map_or(1, |s| s.parse().unwrap());
    let ticks = args.next().map_or(3000, |s| s.parse().unwrap());
    let mut sim = Sim::new(SimConfig {
        seed,
        drop_rate: 0.05,
        max_delay: 5,
        snapshot_threshold: 50,
        ..Default::default()
    });
    sim.run_with_nemesis(ticks);
    sim.heal();
    for id in 1..=sim.cfg.nodes {
        sim.restart(id);
    }
    let drained = sim.drain_clients(5000);
    assert!(sim.run_until_converged(5_000), "nodes did not converge");
    sim.assert_converged();
    let lin = raft_kv::linearizability::check(&sim.history);
    println!(
        "seed={seed} ticks={ticks} drained={drained} linearizable={:?}",
        lin.is_ok()
    );
    println!("{:?}", sim.stats);
    println!(
        "history ops={} indeterminate={}",
        sim.history.len(),
        sim.history.iter().filter(|o| o.ret.is_none()).count()
    );
}
