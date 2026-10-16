//! Durable Raft state. Every method returns only once the change is durable;
//! the core calls them *before* emitting any message that depends on them.
//!
//! Errors are treated as fatal by the core (fail-stop): a node that cannot
//! persist must not keep participating.

use crate::types::{Entry, HardState, Index, Snapshot};
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// State recovered at startup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    pub hard_state: HardState,
    pub snapshot: Option<Snapshot>,
    /// Entries strictly after the snapshot, contiguous.
    pub entries: Vec<Entry>,
}

pub trait Storage {
    fn load(&mut self) -> io::Result<Recovered>;
    fn save_hard_state(&mut self, hs: &HardState) -> io::Result<()>;
    /// Appends entries; their indices follow the current last entry.
    fn append(&mut self, entries: &[Entry]) -> io::Result<()>;
    /// Removes all entries with index >= `from`.
    fn truncate_from(&mut self, from: Index) -> io::Result<()>;
    /// Persists a snapshot and discards entries with index <= snapshot.index.
    fn save_snapshot(&mut self, snap: &Snapshot) -> io::Result<()>;
}

/// In-memory storage. Cloning shares the underlying state, so a simulated
/// node can "crash" (be dropped) and restart from what it had persisted.
#[derive(Clone, Default)]
pub struct MemStorage(Rc<RefCell<Recovered>>);

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MemStorage {
    /// Entries currently persisted (test inspection).
    pub fn load_entries(&self) -> Vec<Entry> {
        self.0.borrow().entries.clone()
    }
}

impl Storage for MemStorage {
    fn load(&mut self) -> io::Result<Recovered> {
        Ok(self.0.borrow().clone())
    }
    fn save_hard_state(&mut self, hs: &HardState) -> io::Result<()> {
        self.0.borrow_mut().hard_state = hs.clone();
        Ok(())
    }
    fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        self.0.borrow_mut().entries.extend_from_slice(entries);
        Ok(())
    }
    fn truncate_from(&mut self, from: Index) -> io::Result<()> {
        self.0.borrow_mut().entries.retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(&mut self, snap: &Snapshot) -> io::Result<()> {
        let mut s = self.0.borrow_mut();
        s.entries.retain(|e| e.index > snap.index);
        s.snapshot = Some(snap.clone());
        Ok(())
    }
}

/// File-backed storage in a directory:
///
/// - `hardstate.json`, `snapshot.json`: replaced atomically (write tmp, fsync, rename, fsync dir).
/// - `log.jsonl`: one JSON entry per line, appended + `fdatasync`. A torn
///   (unparseable or unterminated) final line from a crash is discarded at load.
///   Truncation and compaction rewrite the file atomically (rare operations).
pub struct FileStorage {
    dir: PathBuf,
    log: File,
    entries: Vec<Entry>,
}

impl FileStorage {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log.jsonl"))?;
        Ok(Self {
            dir,
            log,
            entries: Vec::new(),
        })
    }

    fn write_atomic(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        let tmp = self.dir.join(format!("{name}.tmp"));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, self.dir.join(name))?;
        File::open(&self.dir)?.sync_all()
    }

    fn rewrite_log(&mut self) -> io::Result<()> {
        let mut buf = Vec::new();
        for e in &self.entries {
            serde_json::to_writer(&mut buf, e)?;
            buf.push(b'\n');
        }
        self.write_atomic("log.jsonl", &buf)?;
        self.log = OpenOptions::new().append(true).open(self.dir.join("log.jsonl"))?;
        Ok(())
    }
}

fn read_json<T: serde::de::DeserializeOwned>(p: &Path) -> io::Result<Option<T>> {
    match fs::read(p) {
        Ok(b) => Ok(Some(serde_json::from_slice(&b)?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl Storage for FileStorage {
    fn load(&mut self) -> io::Result<Recovered> {
        let hard_state = read_json(&self.dir.join("hardstate.json"))?.unwrap_or_default();
        let snapshot: Option<Snapshot> = read_json(&self.dir.join("snapshot.json"))?;
        let snap_index = snapshot.as_ref().map_or(0, |s| s.index);
        let mut entries: Vec<Entry> = Vec::new();
        let mut torn = false;
        let raw = fs::read(self.dir.join("log.jsonl"))?;
        let mut reader = BufReader::new(&raw[..]);
        let mut line = Vec::new();
        loop {
            line.clear();
            let n = reader.read_until(b'\n', &mut line)?;
            if n == 0 {
                break;
            }
            let complete = line.last() == Some(&b'\n');
            match serde_json::from_slice::<Entry>(&line) {
                Ok(e) if complete => {
                    // Snapshot may have been saved before the log rewrite.
                    if e.index > snap_index {
                        entries.push(e);
                    }
                }
                _ => {
                    torn = true;
                    break;
                }
            }
        }
        let contiguous = entries.windows(2).all(|w| w[1].index == w[0].index + 1)
            && entries.first().is_none_or(|e| e.index == snap_index + 1);
        if !contiguous {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "raft log is not contiguous"));
        }
        self.entries = entries.clone();
        if torn || entries.len() != raw.iter().filter(|b| **b == b'\n').count() {
            self.rewrite_log()?;
        }
        Ok(Recovered {
            hard_state,
            snapshot,
            entries,
        })
    }

    fn save_hard_state(&mut self, hs: &HardState) -> io::Result<()> {
        self.write_atomic("hardstate.json", &serde_json::to_vec(hs)?)
    }

    fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        let mut buf = Vec::new();
        for e in entries {
            serde_json::to_writer(&mut buf, e)?;
            buf.push(b'\n');
        }
        self.log.write_all(&buf)?;
        self.log.sync_data()?;
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn truncate_from(&mut self, from: Index) -> io::Result<()> {
        self.entries.retain(|e| e.index < from);
        self.rewrite_log()
    }

    fn save_snapshot(&mut self, snap: &Snapshot) -> io::Result<()> {
        self.write_atomic("snapshot.json", &serde_json::to_vec(snap)?)?;
        self.entries.retain(|e| e.index > snap.index);
        self.rewrite_log()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("raftkv-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }
    fn e(index: Index, term: u64) -> Entry {
        Entry {
            term,
            index,
            command: Command::Noop,
        }
    }

    #[test]
    fn file_storage_roundtrip_truncate_snapshot() {
        let d = tmpdir("rt");
        {
            let mut s = FileStorage::open(&d).unwrap();
            assert_eq!(s.load().unwrap(), Recovered::default());
            s.save_hard_state(&HardState {
                term: 3,
                voted_for: Some(2),
            })
            .unwrap();
            s.append(&[e(1, 1), e(2, 1), e(3, 2)]).unwrap();
            s.truncate_from(3).unwrap();
            s.append(&[e(3, 3), e(4, 3)]).unwrap();
        }
        let mut s = FileStorage::open(&d).unwrap();
        let r = s.load().unwrap();
        assert_eq!(
            r.hard_state,
            HardState {
                term: 3,
                voted_for: Some(2)
            }
        );
        assert_eq!(r.entries, vec![e(1, 1), e(2, 1), e(3, 3), e(4, 3)]);
        s.save_snapshot(&Snapshot {
            index: 2,
            term: 1,
            data: b"{}".to_vec(),
        })
        .unwrap();
        s.append(&[e(5, 3)]).unwrap();
        let r = FileStorage::open(&d).unwrap().load().unwrap();
        assert_eq!(r.snapshot.unwrap().index, 2);
        assert_eq!(r.entries, vec![e(3, 3), e(4, 3), e(5, 3)]);
        fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn torn_tail_is_discarded() {
        let d = tmpdir("torn");
        {
            let mut s = FileStorage::open(&d).unwrap();
            s.load().unwrap();
            s.append(&[e(1, 1), e(2, 1)]).unwrap();
        }
        // Simulate a crash mid-append: half of a third record.
        let mut f = OpenOptions::new().append(true).open(d.join("log.jsonl")).unwrap();
        f.write_all(br#"{"term":1,"index":3,"comm"#).unwrap();
        drop(f);
        let mut s = FileStorage::open(&d).unwrap();
        assert_eq!(s.load().unwrap().entries, vec![e(1, 1), e(2, 1)]);
        s.append(&[e(3, 2)]).unwrap();
        let r = FileStorage::open(&d).unwrap().load().unwrap();
        assert_eq!(
            r.entries,
            vec![e(1, 1), e(2, 1), e(3, 2)],
            "tail was repaired before appending"
        );
        fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn snapshot_saved_before_log_rewrite_is_consistent() {
        // Crash between writing snapshot.json and rewriting log.jsonl:
        // stale entries <= snapshot index must be ignored on load.
        let d = tmpdir("snapcrash");
        let mut s = FileStorage::open(&d).unwrap();
        s.load().unwrap();
        s.append(&[e(1, 1), e(2, 1), e(3, 1)]).unwrap();
        s.write_atomic(
            "snapshot.json",
            &serde_json::to_vec(&Snapshot {
                index: 2,
                term: 1,
                data: vec![],
            })
            .unwrap(),
        )
        .unwrap();
        let r = FileStorage::open(&d).unwrap().load().unwrap();
        assert_eq!(r.entries, vec![e(3, 1)]);
        fs::remove_dir_all(&d).unwrap();
    }
}
