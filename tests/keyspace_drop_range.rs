use fjall::{Database, KeyspaceCreateOptions};

#[test]
fn keyspace_drop_range_drops_contained_tables() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    for key in b'a'..=b'e' {
        tree.insert([key], "")?;
        tree.rotate_memtable_and_wait()?;
    }

    assert_eq!(5, tree.table_count());

    tree.drop_range("a".."d")?;

    assert!(!tree.contains_key("a")?);
    assert!(!tree.contains_key("b")?);
    assert!(!tree.contains_key("c")?);
    assert!(tree.contains_key("d")?);
    assert!(tree.contains_key("e")?);
    assert_eq!(2, tree.table_count());

    Ok(())
}
