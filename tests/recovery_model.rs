mod common;

use common::{check, env_or, models, open, Op, Rng, KEYSPACES};
use fjall::PersistMode;
use test_log::test;

// Randomized check of journal recovery against an in-memory model: random
// writes, batches, clears, ingestions, flushes and compactions across a few
// keyspaces, with reopens at random points. After every reopen each keyspace
// must hold exactly what the model says.
//
// RECOVERY_MODEL_SEEDS, RECOVERY_MODEL_STEPS and RECOVERY_MODEL_FIRST_SEED scale
// it up for fuzzing (in release mode).

const MEMTABLE: u64 = 64 * 1_024;

fn run(seed: u64, steps: u64) -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let mut rng = Rng::new(seed, 0);
    let mut models = models();
    let (mut db, mut keyspaces) = open(folder.path(), MEMTABLE)?;

    for step in 0..steps {
        match rng.below(100) {
            0..90 => {
                let op = Op::random(&mut rng, step, &models);
                op.apply(&db, &keyspaces)?;
                op.apply_model(&mut models);
            }
            90..92 => {
                db.persist(PersistMode::SyncAll)?;
                drop(keyspaces);
                drop(db);
                (db, keyspaces) = open(folder.path(), MEMTABLE)?;
                check(&format!("seed {seed} step {step}"), &keyspaces, &models)?;
            }
            _ => {
                let i = rng.below(KEYSPACES.len() as u64) as usize;
                let k = rng.key();
                assert_eq!(
                    keyspaces[i].get(&k)?.as_deref(),
                    models[i].data.get(&k).map(Vec::as_slice),
                    "seed {seed} step {step} live read",
                );
            }
        }
    }

    db.persist(PersistMode::SyncAll)?;
    drop(keyspaces);
    drop(db);
    let (_db, keyspaces) = open(folder.path(), MEMTABLE)?;
    check(&format!("seed {seed} end"), &keyspaces, &models)
}

#[test]
fn recovery_matches_model() -> fjall::Result<()> {
    let seeds = env_or("RECOVERY_MODEL_SEEDS", 2);
    let steps = env_or("RECOVERY_MODEL_STEPS", 750);
    let first = env_or("RECOVERY_MODEL_FIRST_SEED", 1);
    for seed in first..first + seeds {
        run(seed, steps)?;
    }
    Ok(())
}
