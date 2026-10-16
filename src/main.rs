//! `raft-kv --id 1 --cluster 1=127.0.0.1:7001/127.0.0.1:8001,2=...,3=... --data ./data/1`
//!
//! Each cluster member is `id=peer_addr/client_addr`.

use raft_kv::server::{self, PeerAddrs, ServerConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: raft-kv --id <n> --cluster <id=peer/client,...> --data <dir> [--tick-ms 10] [--snapshot-threshold 10000]"
    );
    std::process::exit(2)
}

fn main() {
    let mut id = None;
    let mut cluster = BTreeMap::new();
    let mut data = None;
    let mut tick_ms = 10;
    let mut snapshot_threshold = 10_000;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| usage());
        match a.as_str() {
            "--id" => id = val().parse().ok(),
            "--data" => data = Some(PathBuf::from(val())),
            "--tick-ms" => tick_ms = val().parse().unwrap_or_else(|_| usage()),
            "--snapshot-threshold" => snapshot_threshold = val().parse().unwrap_or_else(|_| usage()),
            "--cluster" => {
                for m in val().split(',') {
                    let (id, addrs) = m.split_once('=').unwrap_or_else(|| usage());
                    let (peer, client) = addrs.split_once('/').unwrap_or_else(|| usage());
                    let id: u64 = id.parse().unwrap_or_else(|_| usage());
                    cluster.insert(
                        id,
                        PeerAddrs {
                            peer: peer.into(),
                            client: client.into(),
                        },
                    );
                }
            }
            _ => usage(),
        }
    }
    let (Some(id), Some(data_dir)) = (id, data) else {
        usage()
    };
    if !cluster.contains_key(&id) {
        usage();
    }
    let cfg = ServerConfig {
        id,
        cluster,
        data_dir,
        tick: Duration::from_millis(tick_ms),
        election_ticks: 30,
        snapshot_threshold,
        request_timeout: Duration::from_secs(3),
    };
    if let Err(e) = server::run(cfg) {
        eprintln!("raft-kv: {e}");
        std::process::exit(1);
    }
}
