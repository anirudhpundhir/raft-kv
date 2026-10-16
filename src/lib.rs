//! A Raft-replicated key-value store.
//!
//! - [`raft`]: the consensus core (election, replication, commit, snapshots),
//!   written as a deterministic state machine with no I/O.
//! - [`storage`]: durable state ([`storage::FileStorage`]) and an in-memory
//!   store for simulation.
//! - [`kv`]: the replicated state machine with exactly-once client sessions.
//! - [`sim`]: a deterministic cluster simulator (partitions, drops, delays,
//!   crashes) with safety-invariant checks.
//! - [`linearizability`]: a Wing–Gong style checker for KV histories.
//! - [`server`]: a TCP transport + client protocol for real deployments.

pub mod kv;
pub mod linearizability;
pub mod raft;
pub mod rng;
pub mod sim;
pub mod storage;
pub mod types;
