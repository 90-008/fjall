use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

// Ingested tables aren't journaled, so a write the ingestion replaced must not
// come back from the journal on reopen and shadow the ingested value
#[test]
fn recovery_keeps_ingested_value_over_older_journaled_write() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let ks = db.keyspace("ks", KeyspaceCreateOptions::default)?;
        ks.insert("k", "old")?;
        ks.rotate_memtable_and_wait()?;

        let mut ingestion = ks.start_ingestion()?;
        ingestion.write("k", "ingested")?;
        ingestion.finish()?;

        ks.insert("other", "x")?;
        assert_eq!(ks.get("k")?.as_deref(), Some(&b"ingested"[..]));
    }

    for _ in 0..2 {
        let db = Database::builder(&folder).open()?;
        let ks = db.keyspace("ks", KeyspaceCreateOptions::default)?;
        assert_eq!(ks.get("k")?.as_deref(), Some(&b"ingested"[..]));
    }

    Ok(())
}
