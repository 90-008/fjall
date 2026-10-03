mod common;

use common::{check, env_or, matches, models, open, Model, Op, Rng};
use fjall::PersistMode;
use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use test_log::test;

// Crash recovery against the model in tests/common. The seed counts and sizes
// scale up through RECOVERY_CRASH_* for fuzzing (in release mode).

/// The single journal file in `folder`
fn journal(folder: &Path) -> std::io::Result<PathBuf> {
    let mut journals = vec![];
    for entry in std::fs::read_dir(folder)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "jnl") {
            journals.push(path);
        }
    }
    assert_eq!(journals.len(), 1, "expected a single journal in {folder:?}");
    Ok(journals.remove(0))
}

fn file_len(path: &Path) -> std::io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

/// Copies everything but `skip`, which is the preallocated journal and too
/// large to copy for every cut
fn copy_dir(from: &Path, to: &Path, skip: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.path() == skip {
            continue;
        }
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target, skip)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// Where the written part of a journal ends: journals are preallocated with
/// zeroes and every batch ends in a nonzero trailer
fn written_len(journal: &[u8]) -> u64 {
    journal
        .iter()
        .rposition(|b| *b != 0)
        .map_or(0, |i| i as u64 + 1)
}

/// Offsets to cut a batch spanning `start..end` at. A cut anywhere inside one
/// key or value reads the same way, so only the edges of the batch, where its
/// start header and end trailer are, and a few points in between
fn cuts(start: u64, end: u64, rng: &mut Rng) -> Vec<u64> {
    let len = end - start;
    let mut cuts: Vec<u64> = [0, 1, 2, 13, len - 12, len - 8, len - 2, len - 1]
        .into_iter()
        .chain((0..3).map(|_| rng.below(len)))
        .filter(|cut| *cut < len)
        .map(|cut| start + cut)
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    cuts
}

// A batch written partly before a crash must vanish on reopen, along with the
// torn bytes, and writes after it must still land. Every op is synced before
// the next one starts, so only the last batch can be torn. Its tail is either
// preallocated zeroes or, once a recovery has truncated the journal, missing.
// (A torn batch with the tables after its op, as power loss can leave them, is
// out of scope: a clear would then be half applied.)
#[test]
fn recovery_drops_a_torn_last_batch() -> fjall::Result<()> {
    // large enough that only explicit flushes happen, so every op is still in
    // the journal or in a table when the snapshot is taken
    const MEMTABLE: u64 = 64 << 20;
    const SENTINEL: &[u8] = b"zz-sentinel";
    const START_TAG: u8 = 1;

    let seeds = env_or("RECOVERY_CRASH_TORN_SEEDS", 1);
    let ops = env_or("RECOVERY_CRASH_TORN_OPS", 15);
    let first = env_or("RECOVERY_CRASH_FIRST_SEED", 1);

    for seed in first..first + seeds {
        let folder = tempfile::tempdir()?;
        let (db, keyspaces) = open(folder.path(), MEMTABLE)?;
        let journal_path = journal(folder.path())?;
        let journal_name = journal_path.file_name().unwrap().to_owned();
        let mut rng = Rng::new(seed, 0);
        let mut models = models();
        let mut start = written_len(&std::fs::read(&journal_path)?);

        for n in 0..ops {
            let before = models.clone();
            // a batch is torn while it's being written, before the op touches
            // any tables (a clear drops them only after its marker is written)
            let snapshot = tempfile::tempdir()?;
            copy_dir(folder.path(), snapshot.path(), &journal_path)?;

            let op = Op::random(&mut rng, n, &models);
            op.apply(&db, &keyspaces)?;
            op.apply_model(&mut models);
            db.persist(PersistMode::SyncAll)?;

            assert_eq!(journal(folder.path())?, journal_path, "journal rotated");
            let bytes = std::fs::read(&journal_path)?;
            let end = written_len(&bytes);
            if end == start {
                continue;
            }
            assert_eq!(
                bytes[start as usize], START_TAG,
                "op {n} doesn't start a batch at {start}"
            );

            for cut in cuts(start, end, &mut rng) {
                for preallocated in [true, false] {
                    let context = format!(
                        "seed {seed} op {n} cut {} of {} (preallocated: {preallocated})",
                        cut - start,
                        end - start,
                    );
                    let crashed = tempfile::tempdir()?;
                    let crashed_journal = crashed.path().join(&journal_name);
                    copy_dir(snapshot.path(), crashed.path(), &journal_path)?;
                    std::fs::write(&crashed_journal, &bytes[..cut as usize])?;
                    if preallocated {
                        OpenOptions::new()
                            .write(true)
                            .open(&crashed_journal)?
                            .set_len(bytes.len() as u64)?;
                    }

                    {
                        let (db, keyspaces) = open(crashed.path(), MEMTABLE)?;
                        check(&context, &keyspaces, &before)?;
                        assert_eq!(
                            file_len(&crashed_journal)?,
                            start,
                            "{context}: journal length"
                        );
                        keyspaces[0].insert(SENTINEL, n.to_le_bytes())?;
                        db.persist(PersistMode::SyncAll)?;
                    }
                    let mut expected = before.clone();
                    expected[0].put(SENTINEL, &n.to_le_bytes());
                    let (_db, keyspaces) = open(crashed.path(), MEMTABLE)?;
                    check(&format!("{context}, then a write"), &keyspaces, &expected)?;
                }
            }
            start = end;
        }
    }
    Ok(())
}

const CHILD: &str = "RECOVERY_CRASH_CHILD";
// small, so flushes also happen in the background while the child is killed
const KILL_MEMTABLE: u64 = 64 * 1_024;

fn op_at(seed: u64, n: u64, models: &[Model]) -> Op {
    Op::random(&mut Rng::new(seed, n + 1), n, models)
}

/// Runs ops `start..` against the database at `folder`, printing how many are
/// done after each one is synced, until it gets killed
fn child(args: &str) -> fjall::Result<()> {
    let mut args = args.split(' ');
    let folder = PathBuf::from(args.next().unwrap());
    let seed: u64 = args.next().unwrap().parse().unwrap();
    let start: u64 = args.next().unwrap().parse().unwrap();

    let (db, keyspaces) = open(&folder, KILL_MEMTABLE)?;
    let mut models = models();
    for n in 0..start {
        op_at(seed, n, &models).apply_model(&mut models);
    }
    for n in start..start + 100_000 {
        let op = op_at(seed, n, &models);
        op.apply(&db, &keyspaces)?;
        op.apply_model(&mut models);
        db.persist(PersistMode::SyncAll)?;
        println!("ACK {}", n + 1);
    }
    Ok(())
}

/// Starts a child at op `start`, SIGKILLs it after `delay` and returns how
/// many ops it reported done
fn run_child(folder: &Path, seed: u64, start: u64, delay: Duration) -> u64 {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["recovery_survives_sigkill", "--exact", "--nocapture"])
        .env(CHILD, format!("{} {seed} {start}", folder.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let acked = Arc::new(AtomicU64::new(start));
    let stdout = child.stdout.take().unwrap();
    let reader = {
        let acked = acked.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if let Some(n) = line.unwrap().strip_prefix("ACK ") {
                    acked.fetch_max(n.parse().unwrap(), Ordering::SeqCst);
                }
            }
        })
    };

    std::thread::sleep(delay);
    if let Some(status) = child.try_wait().unwrap() {
        let mut stderr = String::new();
        std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();
        panic!("child exited on its own with {status}:\n{stderr}");
    }
    child.kill().unwrap();
    child.wait().unwrap();
    reader.join().unwrap();
    acked.load(Ordering::SeqCst)
}

// Kills a writer process at random points, including mid-flush, mid-compaction
// and mid-recovery. Each reopen must hold every op the writer reported synced,
// plus at most the one it was in the middle of.
#[test]
fn recovery_survives_sigkill() -> fjall::Result<()> {
    if let Ok(args) = std::env::var(CHILD) {
        return child(&args);
    }

    let seeds = env_or("RECOVERY_CRASH_KILL_SEEDS", 2);
    let kills = env_or("RECOVERY_CRASH_KILLS", 8);
    let first = env_or("RECOVERY_CRASH_FIRST_SEED", 1);

    for seed in first..first + seeds {
        let folder = tempfile::tempdir()?;
        let mut models = models();
        let mut done = 0;

        for kill in 0..kills {
            let delay = Duration::from_millis(50 + Rng::new(seed, u64::MAX - kill).below(400));
            let acked = run_child(folder.path(), seed, done, delay);
            for n in done..acked {
                op_at(seed, n, &models).apply_model(&mut models);
            }
            done = acked;

            let mut with_next = models.clone();
            op_at(seed, done, &models).apply_model(&mut with_next);

            let (_db, keyspaces) = open(folder.path(), KILL_MEMTABLE)?;
            if matches(&keyspaces, &models)? {
                continue;
            }
            if matches(&keyspaces, &with_next)? {
                models = with_next;
                done += 1;
                continue;
            }
            check(
                &format!("seed {seed} kill {kill} after {done} ops"),
                &keyspaces,
                &models,
            )?;
        }
    }
    Ok(())
}
