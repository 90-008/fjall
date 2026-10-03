#![allow(dead_code)]

use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

// Shared by the recovery tests: a seeded random op stream over a few keyspaces
// and an in-memory model of what each keyspace must contain.

pub const KEYSPACES: [&str; 3] = ["a", "b", "cold"];
const KEYS: u64 = 64;

pub struct Rng(u64);

impl Rng {
    /// Independent streams for one seed, so op `n` can be regenerated on its own
    pub fn new(seed: u64, stream: u64) -> Self {
        Self(
            (seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ stream.wrapping_mul(0xd6e8_feb8_6659_fd93))
                | 1,
        )
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn bytes(&mut self, max: u64) -> Vec<u8> {
        let len = 1 + self.below(max);
        (0..len).map(|_| self.next() as u8).collect()
    }

    pub fn key(&mut self) -> Vec<u8> {
        format!("k{:03}", self.below(KEYS)).into_bytes()
    }
}

#[derive(Clone, Default)]
pub struct Model {
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    // puts since the last delete; weak removes are only defined right after one put
    puts: BTreeMap<Vec<u8>, u32>,
}

impl Model {
    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        self.data.insert(k.to_vec(), v.to_vec());
        *self.puts.entry(k.to_vec()).or_default() += 1;
    }

    fn del(&mut self, k: &[u8]) {
        self.data.remove(k);
        self.puts.remove(k);
    }

    fn can_weak_remove(&self, k: &[u8]) -> bool {
        self.puts.get(k) == Some(&1)
    }
}

pub enum Op {
    Insert(usize, Vec<u8>, Vec<u8>),
    /// keyspace, key, value or `None` for a remove
    Batch(Vec<(usize, Vec<u8>, Option<Vec<u8>>)>),
    Remove(usize, Vec<u8>),
    RemoveWeak(usize, Vec<u8>),
    Flush(usize),
    Clear(usize),
    Ingest(usize, Vec<(Vec<u8>, Vec<u8>)>),
    MajorCompact(usize),
}

impl Op {
    /// Values start with `n`, so applying op `n` twice never looks like applying it once
    pub fn random(rng: &mut Rng, n: u64, models: &[Model]) -> Self {
        let value = |rng: &mut Rng, max| {
            let mut v = n.to_le_bytes().to_vec();
            v.extend(rng.bytes(max));
            v
        };

        // "cold" is written rarely, so it pins journals
        let i = if rng.below(20) == 0 {
            2
        } else {
            rng.below(2) as usize
        };

        match rng.below(905) {
            0..400 => Self::Insert(i, rng.key(), value(rng, 2_000)),
            400..550 => {
                let mut used = BTreeSet::new();
                let mut items = vec![];
                for _ in 0..1 + rng.below(8) {
                    let j = rng.below(KEYSPACES.len() as u64) as usize;
                    let k = rng.key();
                    if !used.insert((j, k.clone())) {
                        continue;
                    }
                    let v = (rng.below(4) != 0).then(|| value(rng, 2_000));
                    items.push((j, k, v));
                }
                Self::Batch(items)
            }
            550..650 => Self::Remove(i, rng.key()),
            650..700 => {
                let k = rng.key();
                if models[i].can_weak_remove(&k) {
                    Self::RemoveWeak(i, k)
                } else {
                    Self::Remove(i, k)
                }
            }
            700..850 => Self::Flush(i),
            850..865 => Self::Clear(i),
            865..885 => {
                let keys: BTreeSet<_> = (0..1 + rng.below(10)).map(|_| rng.key()).collect();
                Self::Ingest(i, keys.into_iter().map(|k| (k, value(rng, 500))).collect())
            }
            _ => Self::MajorCompact(i),
        }
    }

    pub fn apply(&self, db: &Database, keyspaces: &[Keyspace]) -> fjall::Result<()> {
        match self {
            Self::Insert(i, k, v) => keyspaces[*i].insert(k, v),
            Self::Batch(items) => {
                let mut batch = db.batch();
                for (j, k, v) in items {
                    match v {
                        Some(v) => batch.insert(&keyspaces[*j], k.clone(), v.clone()),
                        None => batch.remove(&keyspaces[*j], k.clone()),
                    }
                }
                batch.commit()
            }
            Self::Remove(i, k) => keyspaces[*i].remove(k),
            Self::RemoveWeak(i, k) => keyspaces[*i].remove_weak(k),
            Self::Flush(i) => keyspaces[*i].rotate_memtable_and_wait(),
            Self::Clear(i) => keyspaces[*i].clear(),
            Self::Ingest(i, items) => {
                let mut ingestion = keyspaces[*i].start_ingestion()?;
                for (k, v) in items {
                    ingestion.write(k.clone(), v.clone())?;
                }
                ingestion.finish()
            }
            Self::MajorCompact(i) => keyspaces[*i].major_compact(),
        }
    }

    pub fn apply_model(&self, models: &mut [Model]) {
        match self {
            Self::Insert(i, k, v) => models[*i].put(k, v),
            Self::Batch(items) => {
                for (j, k, v) in items {
                    match v {
                        Some(v) => models[*j].put(k, v),
                        None => models[*j].del(k),
                    }
                }
            }
            Self::Remove(i, k) | Self::RemoveWeak(i, k) => models[*i].del(k),
            Self::Clear(i) => models[*i] = Model::default(),
            Self::Ingest(i, items) => {
                for (k, v) in items {
                    models[*i].put(k, v);
                }
            }
            Self::Flush(_) | Self::MajorCompact(_) => {}
        }
    }
}

pub fn models() -> Vec<Model> {
    KEYSPACES.iter().map(|_| Model::default()).collect()
}

pub fn open(path: &Path, max_memtable_size: u64) -> fjall::Result<(Database, Vec<Keyspace>)> {
    let db = Database::builder(path).open()?;
    let keyspaces = KEYSPACES
        .iter()
        .map(|name| {
            db.keyspace(name, || {
                KeyspaceCreateOptions::default().max_memtable_size(max_memtable_size)
            })
        })
        .collect::<fjall::Result<Vec<_>>>()?;
    Ok((db, keyspaces))
}

fn contents(ks: &Keyspace) -> fjall::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut actual = BTreeMap::new();
    for guard in ks.iter() {
        let (k, v) = guard.into_inner()?;
        actual.insert(k.to_vec(), v.to_vec());
    }
    Ok(actual)
}

pub fn matches(keyspaces: &[Keyspace], models: &[Model]) -> fjall::Result<bool> {
    for (ks, model) in keyspaces.iter().zip(models) {
        if contents(ks)? != model.data {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Panics with `context` and a summary of the first keyspace that differs
pub fn check(context: &str, keyspaces: &[Keyspace], models: &[Model]) -> fjall::Result<()> {
    for ((ks, model), name) in keyspaces.iter().zip(models).zip(KEYSPACES) {
        let actual = contents(ks)?;
        if actual != model.data {
            let missing: Vec<_> = model
                .data
                .keys()
                .filter(|k| !actual.contains_key(*k))
                .collect();
            let extra: Vec<_> = actual
                .keys()
                .filter(|k| !model.data.contains_key(*k))
                .collect();
            let differ = model
                .data
                .iter()
                .filter(|(k, v)| actual.get(*k).is_some_and(|a| a != *v))
                .count();
            panic!(
                "{context} keyspace {name}: {} missing {missing:?}, {} extra {extra:?}, {differ} with wrong values",
                missing.len(),
                extra.len(),
            );
        }
    }
    Ok(())
}

pub fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
