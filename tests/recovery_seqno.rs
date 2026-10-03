use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

// Keyspace deletion writes a tombstone into the meta keyspace with a seqno of
// its own, so a keyspace recreated after a reopen must sort above it
#[test]
fn keyspace_recreated_after_reopen_survives_another_reopen() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let ks = db.keyspace("x", KeyspaceCreateOptions::default)?;
        ks.insert("old", "1")?;
        db.delete_keyspace(ks)?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let ks = db.keyspace("x", KeyspaceCreateOptions::default)?;
        ks.insert("new", "2")?;
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        assert!(db.keyspace_exists("x"));
        let ks = db.keyspace("x", KeyspaceCreateOptions::default)?;
        assert!(ks.get("old")?.is_none());
        assert_eq!(ks.get("new")?.as_deref(), Some(&b"2"[..]));
    }

    Ok(())
}

// With every journal gone (as #317's misnamed journal left a database), the
// counter still has to continue after the tables, or new writes sort below
// the old versions they replace
#[test]
fn seqno_continues_after_tables_without_journals() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let ks = db.keyspace("ks", KeyspaceCreateOptions::default)?;
        ks.insert("other", "kept")?;
        for i in 0..100u32 {
            ks.insert("k", i.to_be_bytes())?;
        }
        ks.rotate_memtable_and_wait()?;
    }

    for entry in std::fs::read_dir(&folder)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "jnl") {
            std::fs::remove_file(path)?;
        }
    }

    {
        let db = Database::builder(&folder).open()?;
        let ks = db.keyspace("ks", KeyspaceCreateOptions::default)?;
        assert_eq!(ks.get("other")?.as_deref(), Some(&b"kept"[..]));
        ks.insert("k", "new")?;
        ks.rotate_memtable_and_wait()?;
        ks.major_compact()?;
        assert_eq!(ks.get("k")?.as_deref(), Some(&b"new"[..]));
    }

    let db = Database::builder(&folder).open()?;
    let ks = db.keyspace("ks", KeyspaceCreateOptions::default)?;
    assert_eq!(ks.get("other")?.as_deref(), Some(&b"kept"[..]));
    assert_eq!(ks.get("k")?.as_deref(), Some(&b"new"[..]));

    Ok(())
}
