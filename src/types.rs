//! Wire and log types shared by the Raft core, storage and transports.

use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;
pub type ClientId = u64;

/// A key-value operation submitted by a client.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Delete { key: String },
}

impl Op {
    pub fn key(&self) -> &str {
        match self {
            Op::Get { key } | Op::Put { key, .. } | Op::Delete { key } => key,
        }
    }
}

/// What a log entry carries. `Noop` is appended by every new leader so it can
/// commit entries from earlier terms (Raft §5.4.2).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Command {
    Noop,
    /// `(client, seq)` identifies the request for exactly-once application.
    Client {
        client: ClientId,
        seq: u64,
        op: Op,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub command: Command,
}

/// Result of applying an operation to the state machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    /// `Get` result (None = absent).
    Value(Option<String>),
    /// `Put`/`Delete` acknowledged.
    Ok,
}

/// Persistent state that must be durable before responding to any RPC.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Last log index covered by the snapshot.
    pub index: Index,
    pub term: Term,
    /// Serialized state machine (data + client sessions).
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    RequestVote {
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
    },
    RequestVoteResponse {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    AppendEntriesResponse {
        term: Term,
        success: bool,
        /// On success: highest index known to match the leader.
        /// On failure: the index the leader should retry from.
        index: Index,
    },
    InstallSnapshot {
        term: Term,
        snapshot: Snapshot,
    },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResponse { term, .. }
            | Message::InstallSnapshot { term, .. } => *term,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}
