use crate::{Database, KeyspaceCreateOptions, UserKey, UserValue};

#[test_log::test]
#[ignore = "flimsy because of the compaction check, probably race condition... run the compaction synchronously"]
fn keyspace_ingest() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).worker_threads(0).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [0u8, 1, 2, 3, 4, 5]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    };
    assert_eq!(6, items.len()?);
    assert_eq!(1, items.table_count());

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [1u8, 6, 7, 8, 9]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    }
    assert_eq!(10, items.len()?);
    assert_eq!(2, items.table_count());

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [10u8, 11, 12]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    }
    assert_eq!(13, items.len()?);
    assert_eq!(3, items.table_count());

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [13u8, 14]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    }
    assert_eq!(15, items.len()?);
    assert_eq!(4, items.table_count());

    while !db.worker_pool.sender.is_empty() {}
    assert_eq!(1, items.table_count());

    Ok(())
}

#[test]
fn keyspace_manual_compaction() -> crate::Result<()> {
    use crate::compaction::Leveled;
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).worker_threads(0).open()?;
    let keyspace = db.keyspace("items", KeyspaceCreateOptions::default)?;

    // Use start_ingestion which creates tables directly.
    {
        let mut ingest = keyspace.start_ingestion()?;
        ingest.write("k1", "v1")?;
        ingest.finish()?;
    }
    {
        let mut ingest = keyspace.start_ingestion()?;
        ingest.write("k2", "v2")?;
        ingest.finish()?;
    }

    assert_eq!(2, keyspace.l0_table_count());

    let strategy = Arc::new(Leveled::default().with_l0_threshold(2));
    keyspace.compact(strategy)?;

    assert_eq!(0, keyspace.l0_table_count());
    assert_eq!(2, keyspace.table_count()); // Should have been moved to L6 (Lmax)

    Ok(())
}
