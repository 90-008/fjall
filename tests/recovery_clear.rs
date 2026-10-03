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

// A clear is applied to the tables when it happens, so replaying it must not
// drop tables written after it: ingested data isn't in the journal to replay
#[test]
fn recovery_keeps_tables_written_after_a_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
        cold.insert("pinned", "yes")?;

        hot.insert("before", "x")?;
        hot.clear()?;
        let mut ingestion = hot.start_ingestion()?;
        ingestion.write("ingested", "z")?;
        ingestion.finish()?;
        hot.insert("after", "y")?;
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;

        assert!(hot.get("before")?.is_none());
        assert_eq!(hot.get("ingested")?.as_deref(), Some(&b"z"[..]));
        assert_eq!(hot.get("after")?.as_deref(), Some(&b"y"[..]));
    }

    Ok(())
}

#[test]
fn recovery_keeps_tables_written_after_a_clear_in_sealed_journal() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let big = incompressible_mib();

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
        cold.insert("pinned", "yes")?;

        hot.insert("before", "x")?;
        hot.clear()?;
        let mut ingestion = hot.start_ingestion()?;
        ingestion.write("ingested", "z")?;
        ingestion.finish()?;
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
        assert_eq!(hot.get("ingested")?.as_deref(), Some(&b"z"[..]));
        assert_eq!(hot.get("big079")?.as_deref(), Some(&big[..]));
    }

    Ok(())
}

// With no table newer than the clear, replaying it takes the full clear path
#[test]
fn recovery_replays_clear_onto_older_tables() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
        cold.insert("pinned", "yes")?;

        hot.insert("before", "x")?;
        hot.rotate_memtable_and_wait()?;
        hot.clear()?;
        hot.insert("after", "y")?;
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;

        assert!(hot.get("before")?.is_none());
        assert_eq!(hot.get("after")?.as_deref(), Some(&b"y"[..]));
    }

    Ok(())
}

// A clear leaves no item behind, so if it was the last write, reopening must
// still continue the seqno counter after it. Otherwise later writes sort
// below the clear, and replaying it drops their tables.
#[test]
fn seqno_continues_after_a_trailing_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let cold = db.keyspace("cold", KeyspaceCreateOptions::default)?;
        cold.insert("pinned", "yes")?;

        hot.insert("before", "x")?;
        hot.clear()?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;
        let mut ingestion = hot.start_ingestion()?;
        ingestion.write("ingested", "z")?;
        ingestion.finish()?;
        hot.insert("after", "y")?;
        hot.rotate_memtable_and_wait()?;
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let hot = db.keyspace("hot", KeyspaceCreateOptions::default)?;

        assert!(hot.get("before")?.is_none());
        assert_eq!(hot.get("ingested")?.as_deref(), Some(&b"z"[..]));
        assert_eq!(hot.get("after")?.as_deref(), Some(&b"y"[..]));
    }

    Ok(())
}
