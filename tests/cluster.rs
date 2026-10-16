//! End-to-end test: three real `raft-kv` processes on localhost.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Cluster {
    procs: Vec<Option<Child>>,
    spec: String,
    clients: Vec<String>,
    dir: std::path::PathBuf,
}

impl Cluster {
    fn new(n: u16) -> Self {
        let base = 20000 + (std::process::id() % 2000) as u16 * 10;
        let mut spec = vec![];
        let mut clients = vec![];
        for i in 1..=n {
            let c = format!("127.0.0.1:{}", base + 5 + i);
            spec.push(format!("{i}=127.0.0.1:{}/{c}", base + i));
            clients.push(c);
        }
        let dir = std::env::temp_dir().join(format!("raftkv-cluster-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            procs: (0..n).map(|_| None).collect(),
            spec: spec.join(","),
            clients,
            dir,
        };
        for i in 1..=n {
            c.start(i);
        }
        c
    }
    fn start(&mut self, id: u16) {
        let child = Command::new(env!("CARGO_BIN_EXE_raft-kv"))
            .args(["--id", &id.to_string(), "--cluster", &self.spec, "--tick-ms", "10"])
            .arg("--data")
            .arg(self.dir.join(id.to_string()))
            .arg("--snapshot-threshold")
            .arg("50")
            .spawn()
            .expect("spawn raft-kv");
        self.procs[id as usize - 1] = Some(child);
    }
    fn kill(&mut self, id: u16) {
        if let Some(mut c) = self.procs[id as usize - 1].take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
    fn up(&self) -> Vec<usize> {
        (0..self.procs.len()).filter(|i| self.procs[*i].is_some()).collect()
    }
    /// Sends a command to live nodes, following redirects, until a definite answer.
    fn request(&self, line: &str) -> (String, usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut i = self.up()[0];
        while Instant::now() < deadline {
            if let Ok(r) = send(&self.clients[i], line) {
                if let Some(rest) = r.strip_prefix("REDIRECT ") {
                    let id: usize = rest.split(' ').next().unwrap().parse().unwrap();
                    i = id - 1;
                    if self.procs[i].is_some() {
                        continue;
                    }
                } else if !r.starts_with("ERR") {
                    return (r, i + 1);
                }
            }
            std::thread::sleep(Duration::from_millis(50));
            let up = self.up();
            i = up[(up.iter().position(|x| *x == i).unwrap_or(0) + 1) % up.len()];
        }
        panic!("no definite answer for {line}");
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 1..=self.procs.len() as u16 {
            self.kill(i);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn send(addr: &str, line: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(s, "{line}")?;
    let mut r = String::new();
    BufReader::new(s).read_line(&mut r)?;
    Ok(r.trim_end().to_string())
}

#[test]
fn three_process_cluster_failover_and_durability() {
    let mut c = Cluster::new(3);
    assert_eq!(c.request("PUT greeting hello").0, "OK");
    let (v, leader) = c.request("GET greeting");
    assert_eq!(v, "VALUE hello");
    for i in 0..100 {
        assert_eq!(c.request(&format!("PUT k{i} v{i}")).0, "OK"); // crosses the snapshot threshold
    }

    c.kill(leader as u16);
    let (r, new_leader) = c.request("PUT after failover");
    assert_eq!(r, "OK");
    assert_ne!(new_leader, leader);
    assert_eq!(c.request("GET greeting").0, "VALUE hello");

    // Bring the old leader back, then restart every node: state must survive.
    c.start(leader as u16);
    std::thread::sleep(Duration::from_millis(500));
    for i in 1..=3 {
        c.kill(i);
    }
    for i in 1..=3 {
        c.start(i);
    }
    assert_eq!(c.request("GET after").0, "VALUE failover");
    assert_eq!(c.request("GET k99").0, "VALUE v99");
    assert_eq!(c.request("DEL k99").0, "OK");
    assert_eq!(c.request("GET k99").0, "NIL");
}
