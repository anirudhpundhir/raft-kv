//! Closed-loop load generator for a running cluster.
//! `cargo run --release --example loadgen -- <leader-client-addr> <connections> <ops-per-conn>`
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn main() {
    let mut a = std::env::args().skip(1);
    let addr = a.next().expect("addr");
    let conns: usize = a.next().map_or(1, |s| s.parse().unwrap());
    let ops: usize = a.next().map_or(1000, |s| s.parse().unwrap());
    let start = Instant::now();
    let handles: Vec<_> = (0..conns)
        .map(|c| {
            let addr = addr.clone();
            std::thread::spawn(move || {
                let s = TcpStream::connect(&addr).expect("connect");
                s.set_nodelay(true).unwrap();
                let mut w = s.try_clone().unwrap();
                let mut r = BufReader::new(s);
                let mut lat = Vec::with_capacity(ops);
                let mut line = String::new();
                for i in 0..ops {
                    let t = Instant::now();
                    writeln!(w, "PUT c{c}k{} v{i}", i % 100).unwrap();
                    line.clear();
                    r.read_line(&mut line).unwrap();
                    assert_eq!(line.trim(), "OK", "unexpected reply: {line}");
                    lat.push(t.elapsed());
                }
                lat
            })
        })
        .collect();
    let mut lat: Vec<Duration> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    let el = start.elapsed();
    lat.sort();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    println!(
        "conns={conns} ops={} elapsed={:.2?} throughput={:.0} ops/s p50={:.2?} p99={:.2?} max={:.2?}",
        lat.len(),
        el,
        lat.len() as f64 / el.as_secs_f64(),
        p(0.5),
        p(0.99),
        lat[lat.len() - 1]
    );
}
