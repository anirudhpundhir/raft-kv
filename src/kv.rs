//! The replicated state machine: a string KV map plus a client session table
//! that makes retried requests exactly-once.

use crate::types::{ClientId, Command, Op, Response};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvStore {
    data: BTreeMap<String, String>,
    /// client -> (last applied seq, its response). Clients issue one request
    /// at a time, so remembering the last one suffices for deduplication.
    sessions: BTreeMap<ClientId, (u64, Response)>,
}

impl KvStore {
    /// Applies a committed command. Returns the response for client commands
    /// (cached if the request is a duplicate), `None` for no-ops and for
    /// stale requests older than the client's latest.
    pub fn apply(&mut self, cmd: &Command) -> Option<Response> {
        let Command::Client { client, seq, op } = cmd else {
            return None;
        };
        if let Some((last, resp)) = self.sessions.get(client) {
            if *seq == *last {
                return Some(resp.clone());
            }
            if *seq < *last {
                return None;
            }
        }
        let resp = match op {
            Op::Get { key } => Response::Value(self.data.get(key).cloned()),
            Op::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                Response::Ok
            }
            Op::Delete { key } => {
                self.data.remove(key);
                Response::Ok
            }
        };
        self.sessions.insert(*client, (*seq, resp.clone()));
        Some(resp)
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("kv serialization cannot fail")
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        serde_json::from_slice(b).expect("corrupt snapshot data")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(c: u64, s: u64, k: &str, v: &str) -> Command {
        Command::Client {
            client: c,
            seq: s,
            op: Op::Put {
                key: k.into(),
                value: v.into(),
            },
        }
    }

    #[test]
    fn duplicate_requests_apply_once() {
        let mut kv = KvStore::default();
        assert_eq!(kv.apply(&put(1, 1, "a", "x")), Some(Response::Ok));
        assert_eq!(kv.apply(&put(2, 1, "a", "y")), Some(Response::Ok));
        // client 1 retries seq 1: must not overwrite "y"
        assert_eq!(kv.apply(&put(1, 1, "a", "x")), Some(Response::Ok));
        assert_eq!(kv.get("a").map(String::as_str), Some("y"));
        let get = Command::Client {
            client: 1,
            seq: 2,
            op: Op::Get { key: "a".into() },
        };
        assert_eq!(kv.apply(&get), Some(Response::Value(Some("y".into()))));
        assert_eq!(kv.apply(&put(1, 1, "a", "x")), None, "stale seq ignored");
        assert_eq!(KvStore::from_bytes(&kv.to_bytes()), kv);
    }
}
