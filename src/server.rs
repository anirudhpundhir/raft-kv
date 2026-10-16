//! A small TCP deployment of the Raft core.
//!
//! - Peers exchange newline-delimited JSON [`Envelope`]s over TCP. Sends are
//!   best-effort: a broken connection drops messages, which Raft tolerates.
//! - Clients speak a line protocol on a separate port:
//!   `PUT <key> <value>` → `OK`, `GET <key>` → `VALUE <v>` | `NIL`,
//!   `DEL <key>` → `OK`. Non-leaders answer `REDIRECT <id> <client-addr>`.
//!   Every operation (including GET) goes through the log, so reads are
//!   linearizable.
//! - One driver thread owns the [`Node`]; all other threads talk to it over a
//!   channel, so the core stays single-threaded exactly as in simulation.

use crate::raft::{Config, Node, ProposeError};
use crate::storage::FileStorage;
use crate::types::*;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct PeerAddrs {
    pub peer: String,
    pub client: String,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub id: NodeId,
    pub cluster: BTreeMap<NodeId, PeerAddrs>,
    pub data_dir: PathBuf,
    pub tick: Duration,
    /// Election timeout in ticks (randomized in [n, 2n)).
    pub election_ticks: u64,
    pub snapshot_threshold: u64,
    pub request_timeout: Duration,
}

enum Event {
    Peer(Envelope),
    Client {
        op: Op,
        client: ClientId,
        seq: u64,
        reply: Sender<String>,
    },
}

/// Runs the server until the process exits.
pub fn run(cfg: ServerConfig) -> std::io::Result<()> {
    let storage = FileStorage::open(&cfg.data_dir)?;
    let mut rc = Config::new(cfg.id, cfg.cluster.keys().copied().collect());
    rc.snapshot_threshold = cfg.snapshot_threshold;
    rc.election_ticks = cfg.election_ticks;
    rc.seed = cfg.id ^ SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    let mut node = Node::new(rc, storage);
    let (tx, rx) = mpsc::channel::<Event>();

    let me = &cfg.cluster[&cfg.id];
    let peer_l = TcpListener::bind(&me.peer)?;
    let client_l = TcpListener::bind(&me.client)?;
    {
        let tx = tx.clone();
        thread::spawn(move || accept_peers(peer_l, tx));
    }
    {
        let tx = tx.clone();
        let (id, timeout) = (cfg.id, cfg.request_timeout);
        thread::spawn(move || accept_clients(client_l, tx, id, timeout));
    }
    let mut outbound: HashMap<NodeId, Sender<Envelope>> = HashMap::new();
    for (id, addrs) in &cfg.cluster {
        if *id != cfg.id {
            let (otx, orx) = mpsc::channel();
            let addr = addrs.peer.clone();
            thread::spawn(move || peer_sender(addr, orx));
            outbound.insert(*id, otx);
        }
    }
    drive(&mut node, rx, &outbound, &cfg);
    Ok(())
}

fn drive(
    node: &mut Node<FileStorage>,
    rx: Receiver<Event>,
    out: &HashMap<NodeId, Sender<Envelope>>,
    cfg: &ServerConfig,
) {
    let mut pending: HashMap<(ClientId, u64), (Sender<String>, Instant)> = HashMap::new();
    let mut next_tick = Instant::now() + cfg.tick;
    let mut last_state = (node.term(), node.role());
    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        let first = match rx.recv_timeout(wait) {
            Ok(ev) => Some(ev),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        // Drain whatever else is queued so client proposals that arrived
        // together are appended (and fsync'd) together.
        let mut batch = Vec::new();
        let mut replies = Vec::new();
        for ev in first
            .into_iter()
            .chain(std::iter::from_fn(|| rx.try_recv().ok()).take(1024))
        {
            match ev {
                Event::Peer(env) => node.step(env),
                Event::Client { op, client, seq, reply } => {
                    batch.push((client, seq, op));
                    replies.push(((client, seq), reply));
                }
            }
        }
        if !batch.is_empty() {
            match node.propose_batch(batch) {
                Ok(_) => {
                    let deadline = Instant::now() + cfg.request_timeout;
                    for (k, reply) in replies {
                        pending.insert(k, (reply, deadline));
                    }
                }
                Err(ProposeError::NotLeader { leader }) => {
                    let msg = match leader.and_then(|l| cfg.cluster.get(&l).map(|a| (l, a))) {
                        Some((l, a)) => format!("REDIRECT {l} {}", a.client),
                        None => "ERR no leader".to_string(),
                    };
                    for (_, reply) in replies {
                        let _ = reply.send(msg.clone());
                    }
                }
            }
        }
        if Instant::now() >= next_tick {
            node.tick();
            // Never replay missed ticks in a burst after a slow fsync: that
            // would fire election timeouts without real silence from the leader.
            next_tick = Instant::now() + cfg.tick;
            // Forget requests whose clients have already timed out.
            let now = Instant::now();
            pending.retain(|_, (_, deadline)| *deadline > now);
        }
        if (node.term(), node.role()) != last_state {
            last_state = (node.term(), node.role());
            eprintln!(
                "raft-kv[{}]: term={} role={:?} leader={:?}",
                cfg.id,
                last_state.0,
                last_state.1,
                node.leader()
            );
        }
        for env in node.take_messages() {
            if let Some(s) = out.get(&env.to) {
                let _ = s.send(env);
            }
        }
        for a in node.take_applied() {
            if let (Command::Client { client, seq, .. }, Some(resp)) = (a.command, a.response) {
                if let Some((reply, _)) = pending.remove(&(client, seq)) {
                    let _ = reply.send(match resp {
                        Response::Ok => "OK".into(),
                        Response::Value(Some(v)) => format!("VALUE {v}"),
                        Response::Value(None) => "NIL".into(),
                    });
                }
            }
        }
    }
}

fn accept_peers(l: TcpListener, tx: Sender<Event>) {
    for stream in l.incoming().flatten() {
        let tx = tx.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                let Ok(line) = line else { return };
                match serde_json::from_str::<Envelope>(&line) {
                    Ok(env) => {
                        if tx.send(Event::Peer(env)).is_err() {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        });
    }
}

fn peer_sender(addr: String, rx: Receiver<Envelope>) {
    let mut conn: Option<TcpStream> = None;
    let mut backoff_until = Instant::now();
    for env in rx {
        if conn.is_none() && Instant::now() >= backoff_until {
            match TcpStream::connect_timeout(&addr.parse().expect("peer address"), Duration::from_millis(200)) {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    conn = Some(s);
                }
                Err(_) => backoff_until = Instant::now() + Duration::from_millis(100),
            }
        }
        if let Some(s) = conn.as_mut() {
            let mut line = serde_json::to_vec(&env).expect("serialize envelope");
            line.push(b'\n');
            if s.write_all(&line).is_err() {
                conn = None;
            }
        }
    }
}

static CLIENT_SEQ: AtomicU64 = AtomicU64::new(0);

fn accept_clients(l: TcpListener, tx: Sender<Event>, node: NodeId, timeout: Duration) {
    let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
    for stream in l.incoming().flatten() {
        let tx = tx.clone();
        // Unique per connection across nodes and restarts.
        let client = (node << 56) ^ (epoch << 8) ^ CLIENT_SEQ.fetch_add(1, Ordering::Relaxed);
        thread::spawn(move || {
            let _ = stream.set_nodelay(true);
            let mut w = stream.try_clone().expect("clone stream");
            let mut seq = 0;
            for line in BufReader::new(stream).lines() {
                let Ok(line) = line else { return };
                let reply = match parse(&line) {
                    Err(e) => format!("ERR {e}"),
                    Ok(op) => {
                        seq += 1;
                        let (rtx, rrx) = mpsc::channel();
                        if tx
                            .send(Event::Client {
                                op,
                                client,
                                seq,
                                reply: rtx,
                            })
                            .is_err()
                        {
                            return;
                        }
                        wait_reply(&rrx, timeout)
                    }
                };
                if writeln!(w, "{reply}").is_err() {
                    return;
                }
            }
        });
    }
}

fn wait_reply(rx: &Receiver<String>, timeout: Duration) -> String {
    rx.recv_timeout(timeout)
        .unwrap_or_else(|_| "ERR timeout (outcome unknown)".into())
}

/// Parses one client command line.
pub fn parse(line: &str) -> Result<Op, &'static str> {
    let mut it = line.trim().splitn(3, ' ');
    let cmd = it.next().unwrap_or("").to_ascii_uppercase();
    let key = it.next().filter(|k| !k.is_empty()).ok_or("missing key")?.to_string();
    match (cmd.as_str(), it.next()) {
        ("GET", None) => Ok(Op::Get { key }),
        ("DEL", None) => Ok(Op::Delete { key }),
        ("PUT", Some(v)) => Ok(Op::Put {
            key,
            value: v.to_string(),
        }),
        ("PUT", None) => Err("missing value"),
        _ => Err("usage: GET k | PUT k v | DEL k"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commands() {
        assert_eq!(
            parse("put a hello world"),
            Ok(Op::Put {
                key: "a".into(),
                value: "hello world".into()
            })
        );
        assert_eq!(parse("GET a"), Ok(Op::Get { key: "a".into() }));
        assert_eq!(parse("DEL a"), Ok(Op::Delete { key: "a".into() }));
        assert!(parse("PUT a").is_err());
        assert!(parse("GET").is_err());
        assert!(parse("NOPE a").is_err());
        assert!(parse("GET a b").is_err());
    }
}
