//! Linearizability checker for single-key register histories (Wing & Gong
//! search with memoization, as in Lowe / Porcupine).
//!
//! KV operations on different keys commute, so the history is checked one key
//! at a time (P-compositionality, Herlihy & Wing): the whole history is
//! linearizable iff every per-key sub-history is.
//!
//! Operations whose response was never observed (`ret == None`) may have taken
//! effect at any point after their invocation, or never. Indeterminate reads
//! carry no information and are discarded.

use crate::types::{Op, Response};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Debug)]
pub struct Operation {
    pub client: u64,
    pub op: Op,
    /// Logical time of invocation (strictly increasing event counter).
    pub call: u64,
    /// Logical time the response was received, if it was.
    pub ret: Option<u64>,
    pub result: Option<Response>,
}

/// Returns `Err(key)` naming the first key whose history is not linearizable.
pub fn check(history: &[Operation]) -> Result<(), String> {
    let mut by_key: BTreeMap<&str, Vec<&Operation>> = BTreeMap::new();
    for o in history {
        if o.ret.is_none() && matches!(o.op, Op::Get { .. }) {
            continue;
        }
        by_key.entry(o.op.key()).or_default().push(o);
    }
    for (key, ops) in by_key {
        if !check_register(&ops) {
            return Err(key.to_string());
        }
    }
    Ok(())
}

type State = Option<String>;

fn apply(o: &Operation, s: &State) -> Option<State> {
    match &o.op {
        Op::Put { value, .. } => Some(Some(value.clone())),
        Op::Delete { .. } => Some(None),
        Op::Get { .. } => match &o.result {
            Some(Response::Value(v)) if v == s => Some(s.clone()),
            _ => None,
        },
    }
}

fn check_register(ops: &[&Operation]) -> bool {
    let mut ops: Vec<&Operation> = ops.to_vec();
    ops.sort_by_key(|o| o.call);
    let n = ops.len();
    let words = n.div_ceil(64);
    let required: usize = ops.iter().filter(|o| o.ret.is_some()).count();
    let mut memo: HashSet<(Vec<u64>, State)> = HashSet::new();
    // Iterative DFS: (done bitset, state, number of completed ops linearized).
    let mut stack = vec![(vec![0u64; words], None::<String>, 0usize)];
    while let Some((done, state, completed)) = stack.pop() {
        if completed == required {
            return true;
        }
        let is_done = |i: usize| (done[i / 64] >> (i % 64)) & 1 == 1;
        // An op can be linearized next only if it was invoked before every
        // pending completed op returned.
        let min_ret = (0..n)
            .filter(|&i| !is_done(i))
            .filter_map(|i| ops[i].ret)
            .min()
            .unwrap_or(u64::MAX);
        for i in 0..n {
            if is_done(i) || ops[i].call > min_ret {
                continue;
            }
            if let Some(next) = apply(ops[i], &state) {
                let mut d = done.clone();
                d[i / 64] |= 1 << (i % 64);
                let c = completed + ops[i].ret.is_some() as usize;
                if memo.insert((d.clone(), next.clone())) {
                    stack.push((d, next, c));
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(c: u64, v: &str, call: u64, ret: Option<u64>) -> Operation {
        Operation {
            client: c,
            op: Op::Put {
                key: "k".into(),
                value: v.into(),
            },
            call,
            ret,
            result: ret.map(|_| Response::Ok),
        }
    }
    fn get(c: u64, v: Option<&str>, call: u64, ret: u64) -> Operation {
        Operation {
            client: c,
            op: Op::Get { key: "k".into() },
            call,
            ret: Some(ret),
            result: Some(Response::Value(v.map(String::from))),
        }
    }

    #[test]
    fn sequential_ok() {
        assert!(check(&[put(1, "a", 1, Some(2)), get(2, Some("a"), 3, 4)]).is_ok());
    }

    #[test]
    fn stale_read_rejected() {
        let h = [
            put(1, "a", 1, Some(2)),
            put(1, "b", 3, Some(4)),
            get(2, Some("a"), 5, 6),
        ];
        assert_eq!(check(&h), Err("k".into()));
    }

    #[test]
    fn concurrent_either_order_ok() {
        let h = [
            put(1, "a", 1, Some(10)),
            put(2, "b", 2, Some(9)),
            get(3, Some("a"), 11, 12),
        ];
        assert!(check(&h).is_ok());
    }

    #[test]
    fn read_cannot_flip_back() {
        // b observed, then a later read sees a again although a finished before b began.
        let h = [
            put(1, "a", 1, Some(2)),
            put(2, "b", 3, Some(8)),
            get(3, Some("b"), 4, 5),
            get(3, Some("a"), 6, 7),
        ];
        assert!(check(&h).is_err());
    }

    #[test]
    fn indeterminate_write_may_or_may_not_apply() {
        let h = [put(1, "a", 1, None), get(2, None, 2, 3), get(2, Some("a"), 4, 5)];
        assert!(check(&h).is_ok());
        let h = [put(1, "a", 1, None), get(2, Some("a"), 2, 3), get(2, None, 4, 5)];
        assert!(check(&h).is_err(), "a write cannot be undone");
    }
}
