use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

/// Incompressible, so a journal really grows past its 64MB rotation threshold
fn incompressible_mib() -> Vec<u8> {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    (0..1 << 20)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect()
}

// Recovery skips journal items a keyspace has already flushed, so a journal
// holding both flushed and unflushed items of one keyspace must keep the latter
#[test]
fn recovery_keeps_unflushed_items_of_flushed_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let big = incompressible_mib();

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;

        // Cold never flushes, so every journal stays around to be replayed
        cold.insert("pinned", "yes")?;

        // Past the 64MB rotation threshold, so this flush seals the journal
        for i in 0..80u32 {
            hot.insert(format!("big{i:03}"), &big)?;
        }
        hot.insert("k", "old")?;
        hot.rotate_memtable_and_wait()?;
        assert!(db.journal_count() >= 2);

        // The active journal gets a flushed write and unflushed ones after it
        hot.insert("k", "mid")?;
        hot.rotate_memtable_and_wait()?;
        hot.insert("k", "new")?;
        hot.insert("only-journal", "x")?;
        hot.remove("big000")?;
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;

        assert_eq!(hot.get("k")?.as_deref(), Some(&b"new"[..]));
        assert_eq!(hot.get("only-journal")?.as_deref(), Some(&b"x"[..]));
        assert!(hot.get("big000")?.is_none());
        assert_eq!(hot.get("big079")?.as_deref(), Some(&big[..]));
        assert_eq!(cold.get("pinned")?.as_deref(), Some(&b"yes"[..]));
    }

    Ok(())
}

// Replaying a clear drops the keyspace's tables, so items after the clear that
// had been flushed into them must be replayed again, not skipped
#[test]
fn recovery_replays_flushed_items_after_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
        cold.insert("pinned", "yes")?;

        hot.insert("before", "x")?;
        hot.clear()?;
        hot.insert("after", "y")?;
        hot.rotate_memtable_and_wait()?;
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;

        assert!(hot.get("before")?.is_none());
        assert_eq!(hot.get("after")?.as_deref(), Some(&b"y"[..]));
    }

    Ok(())
}

#[test]
fn recovery_replays_flushed_items_after_clear_in_sealed_journal() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let big = incompressible_mib();

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
        cold.insert("pinned", "yes")?;

        hot.insert("before", "x")?;
        hot.rotate_memtable_and_wait()?;
        hot.clear()?;
        hot.insert("after", "y")?;
        for i in 0..80u32 {
            hot.insert(format!("big{i:03}"), &big)?;
        }
        hot.rotate_memtable_and_wait()?;
        assert!(db.journal_count() >= 2);
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;

        assert!(hot.get("before")?.is_none());
        assert_eq!(hot.get("after")?.as_deref(), Some(&b"y"[..]));
        assert_eq!(hot.get("big079")?.as_deref(), Some(&big[..]));
    }

    Ok(())
}
