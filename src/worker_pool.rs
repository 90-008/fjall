// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::worker::run as run_compaction, flush::worker::run as run_flush, poison::PoisonDart,
    stats::Stats, supervisor::Supervisor, Keyspace,
};
use lsm_tree::MemtableId;
use std::{
    borrow::Cow,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering::{Relaxed, SeqCst},
        },
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

pub enum WorkerMessage {
    Flush,
    Compact(Keyspace),
    Close,
    RotateMemtable(Keyspace, MemtableId),
}

impl std::fmt::Debug for WorkerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Flush => Cow::Borrowed("WorkerMessage:Flush"),
                Self::Compact(k) => Cow::Owned(format!("WorkerMessage:Compact({:?})", k.name)),
                Self::Close => Cow::Borrowed("WorkerMessage:Close"),
                Self::RotateMemtable(k, memtable_id) =>
                    Cow::Owned(format!("WorkerMessage:Rotate({:?}, {memtable_id})", k.name)),
            }
        )
    }
}

type WorkerHandle = JoinHandle<Result<(), crate::Error>>;

pub struct WorkerPool {
    thread_handles: Mutex<Vec<WorkerHandle>>,
    pool_size: usize,
    pub(crate) rx: flume::Receiver<WorkerMessage>,
    pub(crate) sender: flume::Sender<WorkerMessage>,

    // what workers 1.. read. worker 0 never does, so it stays free for
    // flushes and memtable rotations. with a single worker this is just the
    // channel above
    pub(crate) compaction_rx: flume::Receiver<WorkerMessage>,
    pub(crate) compaction_sender: flume::Sender<WorkerMessage>,

    /// see `Keyspace::send_spare`
    pub(crate) spare_messages: Arc<AtomicUsize>,

    #[cfg(test)]
    ticks: Arc<AtomicUsize>,
}

impl WorkerPool {
    pub fn prepare(pool_size: usize) -> Self {
        let (sender, rx) = flume::bounded(1_000);

        // unbounded so a close or a flush never waits on it, and
        // `Keyspace::request_compaction` caps the compactions in it
        let (compaction_sender, compaction_rx) = if pool_size > 1 {
            flume::unbounded()
        } else {
            (sender.clone(), rx.clone())
        };

        Self {
            thread_handles: Mutex::default(),
            pool_size,
            rx,
            sender,
            compaction_rx,
            compaction_sender,
            spare_messages: Arc::default(),
            #[cfg(test)]
            ticks: Arc::default(),
        }
    }

    pub fn start(
        &self,
        supervisor: &Supervisor,
        stats: &Arc<Stats>,
        poison_dart: &PoisonDart,
        thread_counter: &Arc<AtomicUsize>,
    ) -> crate::Result<()> {
        let pool_size = self.pool_size;

        log::debug!("Starting worker pool with {pool_size} threads");

        let thread_handles = claim_and_spawn(pool_size, thread_counter, |i| {
            std::thread::Builder::new()
                .name("fjall:worker".to_string())
                .spawn({
                    log::trace!("Starting fjall worker thread #{i}");

                    let rx = if i == 0 {
                        self.rx.clone()
                    } else {
                        self.compaction_rx.clone()
                    };

                    let worker_state = WorkerState {
                        worker_id: i,
                        rx,
                        supervisor: supervisor.clone(),
                        stats: stats.clone(),
                        spare_messages: self.spare_messages.clone(),
                        #[cfg(test)]
                        ticks: self.ticks.clone(),
                    };

                    let thread_counter = thread_counter.clone();
                    let poison_dart = poison_dart.clone();

                    move || {
                        // The counter must drop on *every* way out of this
                        // thread, not just the graceful one: `Database::drop`
                        // spins on it (`while counter > 0`), so a worker that
                        // returns an error or unwinds would keep the database
                        // closing forever.
                        let _counter_guard = ActiveThreadGuard(thread_counter);

                        loop {
                            match worker_tick(&worker_state) {
                                Ok(should_abort) => {
                                    if should_abort {
                                        log::debug!("Worker #{i} closes because DB is dropping");
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    log::error!("Worker #{i} crashed: {e:?}");
                                    poison_dart.poison();
                                    return Err(e);
                                }
                            }
                        }
                    }
                })
        })?;

        *self.thread_handles.lock().expect("lock is poisoned") = thread_handles;

        Ok(())
    }

    pub(crate) fn close(&self, thread_counter: &AtomicUsize) {
        // workers 1.. leave on the first close they read, and their channel
        // is unbounded, so this can't block
        for _ in 1..self.pool_size {
            self.compaction_sender.send(WorkerMessage::Close).ok();
        }

        // only worker 0 reads this one. don't wait on a full channel for
        // good, worker 0 may have crashed and then nobody makes room
        let mut close_sent = false;

        while thread_counter.load(Relaxed) > 0 {
            if !close_sent {
                close_sent = self
                    .sender
                    .send_timeout(WorkerMessage::Close, Duration::from_millis(1))
                    .is_ok();
            }

            std::thread::sleep(Duration::from_micros(10));
        }
    }

    pub(crate) fn clear(&self) {
        let _ = self.rx.drain().count();
        let _ = self.compaction_rx.drain().count();
    }
}

/// Claims one slot in the active thread counter per worker, immediately before
/// that worker is spawned, and hands the slot straight back if the spawn fails.
///
/// Claiming the whole pool up front would leak the slots of the workers that failed
/// spawn never reaches: nothing ever decrements them, because those threads do
/// not exist, and `DatabaseInner::drop` waits for the counter to reach zero.
///
/// The spawn is a parameter so the failure path can be tested without having to
/// exhaust the operating system's thread limit.
fn claim_and_spawn<H, S: FnMut(usize) -> std::io::Result<H>>(
    pool_size: usize,
    thread_counter: &Arc<AtomicUsize>,
    mut spawn: S,
) -> std::io::Result<Vec<H>> {
    (0..pool_size)
        .map(|i| {
            thread_counter.fetch_add(1, Relaxed);

            spawn(i).inspect_err(|_| {
                thread_counter.fetch_sub(1, Relaxed);
            })
        })
        .collect()
}

/// Decrements the pool's active thread counter when a worker thread leaves,
/// whatever the reason: graceful close, error return or unwinding panic.
///
/// `DatabaseInner::drop` waits for this counter to reach zero, so a leaked
/// increment makes closing the database hang forever.
struct ActiveThreadGuard(Arc<AtomicUsize>);

impl Drop for ActiveThreadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

struct WorkerState {
    worker_id: usize,
    supervisor: Supervisor,
    rx: flume::Receiver<WorkerMessage>,
    stats: Arc<Stats>,
    spare_messages: Arc<AtomicUsize>,
    #[cfg(test)]
    ticks: Arc<AtomicUsize>,
}

fn worker_tick(ctx: &WorkerState) -> crate::Result<bool> {
    let Ok(item) = ctx.rx.recv() else {
        return Ok(true);
    };

    #[cfg(test)]
    ctx.ticks.fetch_add(1, Relaxed);

    log::trace!("Worker #{} got message: {item:?}", ctx.worker_id);

    // flushes and rotations only reach the other workers as spares
    if ctx.worker_id > 0
        && matches!(
            item,
            WorkerMessage::Flush | WorkerMessage::RotateMemtable(..)
        )
    {
        ctx.spare_messages.fetch_sub(1, SeqCst);
    }

    match item {
        WorkerMessage::Close => {
            return Ok(true);
        }
        WorkerMessage::RotateMemtable(keyspace, memtable_id) => {
            log::trace!("acquiring journal lock");
            let journal_writer = keyspace.supervisor.journal.get_writer()?;
            keyspace.inner_rotate_memtable(journal_writer, memtable_id)?;
        }
        WorkerMessage::Flush => {
            let Some(task) = ctx.supervisor.flush_manager.dequeue() else {
                return Ok(false);
            };

            {
                log::trace!("acquiring journal lock to maybe rotate journal");
                let mut journal_writer = ctx.supervisor.journal.get_writer()?;

                if journal_writer.pos()? > 64_000_000 {
                    #[expect(clippy::expect_used)]
                    let mut journal_manager = ctx
                        .supervisor
                        .journal_manager
                        .write()
                        .expect("lock is poisoned");

                    let seqno_map = {
                        #[expect(clippy::expect_used)]
                        let keyspaces = ctx.supervisor.keyspaces.write().expect("lock is poisoned");

                        ctx.supervisor.build_seqno_map(&keyspaces)
                    };

                    journal_manager.rotate_journal(&mut journal_writer, seqno_map)?;

                    if journal_manager.disk_space_used()
                        >= ctx.supervisor.db_config.max_journaling_size_in_bytes
                    {
                        let stragglers =
                            journal_manager.get_keyspaces_to_flush_for_oldest_journal_eviction();

                        for keyspace in stragglers {
                            log::info!(
                                "Rotating {:?} to try to reduce journal size",
                                keyspace.name,
                            );
                            keyspace.request_rotation();
                        }
                    }
                } else {
                    // https://github.com/fjall-rs/fjall/issues/329
                    journal_writer.persist(crate::PersistMode::SyncAll)?;
                }
            }

            run_flush(
                &task,
                &ctx.supervisor.write_buffer_size,
                &ctx.supervisor.snapshot_tracker,
                &ctx.stats,
            )?;

            task.keyspace.request_compaction();

            ctx.supervisor
                .journal_manager
                .write()
                .expect("lock is poisoned")
                .maintenance()?;
        }
        WorkerMessage::Compact(keyspace) => {
            keyspace.pending_compactions.fetch_sub(1, SeqCst);

            run_compaction(&keyspace, &ctx.supervisor.snapshot_tracker, &ctx.stats)?;
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict},
        AbstractTree, Database, KeyspaceCreateOptions,
    };
    use std::{
        sync::{atomic::AtomicBool, Condvar},
        thread::sleep,
        time::{Duration, Instant},
    };
    use test_log::test;

    // stands in for a long compaction: a merge stops at its first item until
    // the test opens the gate. make_filter would be too early, it runs under
    // the version lock that a flush needs
    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        opened: Condvar,
        entered: AtomicUsize,
    }

    impl Gate {
        fn open(&self) {
            *self.open.lock().expect("lock is poisoned") = true;
            self.opened.notify_all();
        }

        fn pass(&self) {
            self.entered.fetch_add(1, SeqCst);

            let open = self.open.lock().expect("lock is poisoned");
            drop(
                self.opened
                    .wait_while(open, |open| !*open)
                    .expect("lock is poisoned"),
            );
        }
    }

    // opens the gate when a test ends, by panic too, because closing the
    // database waits for the stuck compaction
    struct OpenOnDrop<'a>(&'a Gate);

    impl Drop for OpenOnDrop<'_> {
        fn drop(&mut self) {
            self.0.open();
        }
    }

    struct GateFilter(Arc<Gate>);

    impl Factory for GateFilter {
        fn name(&self) -> &'static str {
            "gate"
        }

        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(Self(self.0.clone()))
        }
    }

    impl CompactionFilter for GateFilter {
        fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> lsm_tree::Result<Verdict> {
            self.0.pass();
            Ok(Verdict::Keep)
        }
    }

    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);

        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            sleep(Duration::from_millis(1));
        }
    }

    // two workers, with worker 1 stuck in a compaction behind the gate
    fn open_with_busy_compaction_worker(
        folder: &tempfile::TempDir,
        gate: &Arc<Gate>,
    ) -> crate::Result<(Database, Keyspace)> {
        let factory: Arc<dyn Factory> = Arc::new(GateFilter(gate.clone()));

        let db = Database::builder(folder)
            .worker_threads(2)
            .with_compaction_filter_factories(Arc::new(move |_| Some(factory.clone())))
            .open()?;

        let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;

        // overlapping tables pile up in l0 until the leveled strategy merges them
        for _ in 0..6 {
            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;
        }

        wait_until("a compaction to reach the gate", || {
            gate.entered.load(SeqCst) > 0
        });

        Ok((db, ks))
    }

    #[test]
    fn flush_worker_idles_while_compactions_wait() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let gate = Arc::new(Gate::default());
        let (db, ks) = open_with_busy_compaction_worker(&folder, &gate)?;
        let _open = OpenOnDrop(&gate);

        // the flush can't wait on worker 1, and it asks for more compactions
        // that have nobody to run them
        ks.insert("b", "b")?;
        ks.rotate_memtable_and_wait()?;

        let ticks_before = db.worker_pool.ticks.load(SeqCst);
        sleep(Duration::from_millis(200));
        let ticks = db.worker_pool.ticks.load(SeqCst) - ticks_before;

        assert!(
            ticks <= 2,
            "workers handled {ticks} messages while they should all wait",
        );

        // the stuck run and at least one queued one, so nothing got lost
        let completed = db.stats.compactions_completed.load(SeqCst);
        gate.open();
        wait_until("the queued compactions to run", || {
            db.stats.compactions_completed.load(SeqCst) >= completed + 2
        });

        Ok(())
    }

    #[test]
    fn spare_messages_are_capped_while_compaction_workers_are_busy() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let gate = Arc::new(Gate::default());
        let (db, ks) = open_with_busy_compaction_worker(&folder, &gate)?;
        let _open = OpenOnDrop(&gate);

        for _ in 0..100 {
            ks.request_rotation();
        }

        // two compaction runs of the keyspace and one spare for worker 1
        assert!(db.worker_pool.compaction_rx.len() <= 3);
        assert!(db.worker_pool.spare_messages.load(SeqCst) <= 1);

        Ok(())
    }

    #[test]
    fn compaction_requests_are_capped_per_keyspace() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;
        let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;

        for _ in 0..10 {
            ks.request_compaction();
        }

        assert_eq!(1, db.worker_pool.compaction_rx.len());

        Ok(())
    }

    #[test]
    fn drop_closes_busy_workers_behind_a_full_queue() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let gate = Arc::new(Gate::default());
        let (db, ks) = open_with_busy_compaction_worker(&folder, &gate)?;
        let _open = OpenOnDrop(&gate);

        // keeps the bounded queue full the whole time the database closes
        let stop_filling = Arc::new(AtomicBool::new(false));
        let filler = std::thread::spawn({
            let sender = db.worker_pool.sender.clone();
            let stop_filling = stop_filling.clone();
            move || {
                while !stop_filling.load(SeqCst) {
                    sender.try_send(WorkerMessage::Flush).ok();
                }
            }
        });
        wait_until("the queue to fill", || db.worker_pool.sender.is_full());

        let closing = std::thread::spawn(move || drop((ks, db)));
        sleep(Duration::from_millis(50));
        gate.open();
        wait_until("the database to close", || closing.is_finished());

        stop_filling.store(true, SeqCst);
        filler.join().expect("filler should not panic");

        Ok(())
    }

    // https://github.com/fjall-rs/fjall/pull/303
    #[test]
    fn keyspace_compact_after_startup() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        {
            let db = Database::builder(&folder).open()?;

            let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;

            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;

            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;

            ks.insert("a", "a")?;
            ks.rotate_memtable_and_wait()?;

            assert!(ks.tree.l0_run_count() > 0);
        }

        {
            let db = Database::builder(&folder)
                .worker_threads_unchecked(0)
                .open()?;

            assert_eq!(
                1,
                db.worker_pool.rx.len(),
                "worker message should be enqueued on startup",
            );
            let item = db.worker_pool.rx.try_recv().expect("should get message");
            assert!(
                matches!(item, WorkerMessage::Compact(_)),
                "worker message should be compaction request",
            );
        }

        Ok(())
    }

    /// Every worker that is actually spawned holds exactly one slot.
    #[test]
    fn claim_and_spawn_claims_one_slot_per_worker() {
        let counter = Arc::new(AtomicUsize::new(0));

        let handles = claim_and_spawn(3, &counter, |i| Ok::<_, std::io::Error>(i))
            .expect("all spawns succeed");

        assert_eq!(handles, vec![0, 1, 2]);
        assert_eq!(counter.load(Relaxed), 3);
    }

    /// A failed spawn releases its own slot and never claims one for the workers
    /// it did not get to. `DatabaseInner::drop` spins until the counter reaches
    /// zero, so a slot held by a thread that does not exist hangs the close
    /// forever.
    #[test]
    fn claim_and_spawn_counts_live_workers_only() {
        let counter = Arc::new(AtomicUsize::new(0));

        let result = claim_and_spawn(4, &counter, |i| {
            if i == 2 {
                Err(std::io::Error::other("cannot spawn thread"))
            } else {
                Ok(i)
            }
        });

        assert!(result.is_err());
        assert_eq!(
            counter.load(Relaxed),
            2,
            "only the two workers that started should hold a slot",
        );
    }

    /// A worker leaving normally releases its slot in the counter.
    #[test]
    fn active_thread_guard_decrements_on_scope_exit() {
        let counter = Arc::new(AtomicUsize::new(1));
        {
            let _guard = ActiveThreadGuard(counter.clone());
            assert_eq!(counter.load(Relaxed), 1);
        }
        assert_eq!(counter.load(Relaxed), 0);
    }

    /// A worker that returns an error releases its slot too: `Database::drop`
    /// spins until the counter reaches zero, so a leaked slot would hang the
    /// close forever.
    #[test]
    fn active_thread_guard_decrements_on_early_return() {
        let counter = Arc::new(AtomicUsize::new(1));

        fn failing_worker(counter: Arc<AtomicUsize>) -> Result<(), ()> {
            let _guard = ActiveThreadGuard(counter);
            Err(())
        }

        assert!(failing_worker(counter.clone()).is_err());
        assert_eq!(counter.load(Relaxed), 0);
    }

    /// A panicking worker releases its slot as well.
    #[test]
    fn active_thread_guard_decrements_on_panic() {
        let counter = Arc::new(AtomicUsize::new(1));
        let counter_in_thread = counter.clone();

        let outcome = std::thread::spawn(move || {
            let _guard = ActiveThreadGuard(counter_in_thread);
            panic!("worker crashed");
        })
        .join();

        assert!(outcome.is_err());
        assert_eq!(counter.load(Relaxed), 0);
    }
}
