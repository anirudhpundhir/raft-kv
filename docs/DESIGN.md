# Design notes (ADR log)

Short Architecture Decision Records. Each has context, the decision, and its consequences.

---

## ADR-001: The Raft core is a deterministic state machine with no I/O

**Context.** Consensus bugs live in rare interleavings: a message delayed past an election, or a crash between two writes. Tests that use real threads, sockets and clocks can't reproduce them reliably.

**Decision.** `raft::Node` has exactly three inputs: `tick()` (logical time), `step(Envelope)` (a peer message) and `propose(...)`. It has two outputs, drained by the caller: `take_messages()` and `take_applied()`. Its only side effect is calling `Storage` synchronously. Randomness (election timeouts) comes from a seeded SplitMix64, so it's stable across platforms and dependency upgrades. The TCP server and the simulator drive the **same** code.

**Consequences.** Any simulator failure replays exactly from its seed (`RAFT_SIM_SEED=n`). The core is single-threaded. The server serialises all access through one driver thread and a channel.

---

## ADR-002: Persistence ordering: durable before visible

**Context.** Raft's safety argument assumes that `currentTerm`, `votedFor` and log entries survive crashes *before* the node acts on them.

**Decision.**
- `Storage` methods return only after the change is durable. The core calls them **before** queuing any message that depends on the change: vote grants, append acks, and the leader's own append (which counts toward the commit quorum).
- `FileStorage`: `hardstate.json` and `snapshot.json` are replaced atomically (write tmp → fsync → rename → fsync dir). `log.jsonl` is appended and `fdatasync`'d. Truncation and compaction rewrite the log atomically; they're rare.
- On load, a torn or unterminated final log line is discarded and the file is repaired *before* new appends. A crash between saving a snapshot and rewriting the log is tolerated: entries ≤ snapshot index are ignored on load. All three cases have unit tests.
- Storage errors panic (fail-stop). A node that can't persist must not keep voting or acking.

**Consequences.** Mutation check: removing `persist_hard_state()` after granting a vote is caught by `chaos_frequent_crash_restart`, which sees two leaders in one term. JSON lines without checksums can't detect bit-rot in the *middle* of the log. A checksummed segmented WAL is what `wal-engine` builds, and it's the obvious replacement (follow-up).

---

## ADR-003: Safety rules implemented as in the paper, plus check-quorum

- **Election**: vote at most once per term (persisted). Grant only if the candidate's `(lastTerm, lastIndex)` ≥ ours. A follower that has heard from a live leader in the current term rejects same-term vote requests.
- **Commit**: a leader advances `commitIndex` only to an index **from its own term** stored on a majority (§5.4.2). Each new leader appends a `Noop` so earlier entries commit promptly. `tests/figure8.rs` hand-schedules the paper's Figure 8. With the term check removed, the test catches the leader committing a previous-term entry, and (with that assertion removed) the subsequent divergent apply at the same index.
- **Follower commit** is `min(leaderCommit, index of last entry verified by this message)`, *not* `lastIndex`. Stale entries past the match point may differ from the leader's. The mutant using `lastIndex` is caught by the simulator's state-machine-safety check.
- **Fast backtracking**: a rejection returns the first index of the conflicting term (never below `commit+1`), so the leader skips a whole term per round trip.
- **Check-quorum**: a leader that hasn't heard from a majority for 2× the election timeout steps down. Clients therefore stop waiting on a deposed, partitioned leader. This is a liveness feature and safety doesn't depend on it.

---

## ADR-004: Linearizable reads through the log; exactly-once via client sessions

**Context.** A leader serving reads locally can return stale data if it has been deposed without knowing it.

**Decision.**
- `GET` is a log entry like `PUT`/`DELETE`. It's the simplest correct choice, and it costs one replication round per read.
- Each request is `(client_id, seq)`. The state machine keeps the last `seq` and response per client, and re-applying the same `seq` returns the cached response. A client can therefore retry against any node after a timeout without double-applying. Sessions are part of the snapshot.

**Consequences.** Reads cost as much as writes (see follow-ups: ReadIndex / leases). Clients issue one request at a time per session. The simulator's clients do this and retry with the same `seq`. The TCP line protocol gives each *connection* a session, so a client that reconnects and retries gets at-least-once (documented in the server module).

---

## ADR-005: Deterministic simulation + linearizability checking as the primary test strategy

**Decision.** `sim::Sim` models a network with seeded per-message delay (reordering), loss, partitions, and crash/restart. A restart re-creates the `Node` from its `MemStorage`, so only durable state survives. A nemesis injects faults at random, and it may kill the leader preferentially while keeping a majority alive. Checked **every tick**:
- *Election safety*: at most one leader per term, ever.
- *State machine safety*: every node applies the identical `(term, command)` at each index.
- `applied ≤ commit ≤ lastIndex`.

At the end of every run: heal, restart all nodes, require every outstanding client op to complete (liveness), require all nodes to converge to identical state machines, then check the full client history with a **Wing–Gong linearizability checker** (per key, memoized; indeterminate writes may or may not have taken effect).

**Mutation testing (done by hand while building).** Each of these single-line bugs is caught by at least one test: vote without the up-to-date check; vote without the `votedFor` check; not persisting the vote; follower commit using `lastIndex`; disabling session dedup (linearizability failure); the Figure 8 commit rule (Figure 8 test only. Random simulation over 300 seeds did **not** find it, which is why the targeted test exists).

**Consequences.** 1000 seeds × 8 scenarios run in ≈10s in release mode on the build box. CI runs a 4-shard soak over distinct seed ranges.

---

## ADR-006: Replication flow control and group commit

**Context.** The first TCP load test lost leadership under 16 concurrent clients. Every proposal re-sent the entire unacknowledged suffix to every follower, followers fell behind on redundant messages, heartbeats queued up behind them, and elections fired. The driver also replayed missed ticks in a burst after a stall.

**Decision.**
- At most **one in-flight AppendEntries with entries per follower**. New proposals wait for the ack, and the next message carries everything appended since (batching). Heartbeats still retransmit, in case the in-flight message was lost.
- `propose_batch` appends many client ops with **one fsync**. The server drains its event channel, up to 1024 events, before proposing (group commit).
- The driver never replays missed ticks.
- Client sockets set `TCP_NODELAY`. Without it, Nagle plus delayed ACK added ~40ms per request (measured p50 44ms before, 0.26ms after).

**Consequences.** Measured on one box: about 3.3k ops/s with one closed-loop client, and about 23k ops/s with 64 (see README). There's still no pipelining beyond depth one per follower. That's a follow-up.

---

## Follow-ups / known scope cuts

- **Membership changes** (joint consensus or single-server changes): not implemented. Membership is static.
- **Pre-vote**: not implemented. A partitioned node that rejoins with a higher term disrupts the current leader. That's liveness only, and it's visible in the simulator as extra elections.
- **ReadIndex / lease reads** to avoid a log append per GET.
- **Snapshot streaming**: snapshots are sent whole in one message, which is fine for small state and wrong for GBs.
- **Checksummed log segments** (see `wal-engine`) instead of JSON lines. Storage I/O errors are fail-stop.
- The TCP transport is plain-text JSON with no TLS or auth, and there's no smart client library for cross-connection retries with stable sessions.
- No Jepsen-style test of the real binary under real partitions. The process-level test covers failover and full-cluster restart only.
