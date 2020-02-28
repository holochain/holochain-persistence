use holochain_persistence_api::{
    eav::*,
    txn::{DefaultPersistenceManager, NonTransactionalCursor},
};

use crate::{cas::pickle::PickleStorage, eav::pickle::EavPickleStorage};
use serde::de::DeserializeOwned;
use std::{convert::TryFrom, path::Path};

pub type PickleManager<A> = DefaultPersistenceManager<
    A,
    PickleStorage,
    EavPickleStorage<A>,
    NonTransactionalCursor<A, PickleStorage, EavPickleStorage<A>>,
>;

/// Creates non transactional persistence manager for a pickle db
/// backed database. Cursors are *not* atomic.
pub fn new_manager<
    A: Attribute + DeserializeOwned + TryFrom<String> + std::fmt::Display,
    P: AsRef<Path> + Clone,
    EP: AsRef<Path> + Clone,
>(
    cas_dir: P,
    eav_dir: EP,
) -> PickleManager<A> {
    let cas_db = PickleStorage::new(cas_dir);

    let eav_db: EavPickleStorage<A> = EavPickleStorage::new(eav_dir);

    let cursor_provider = NonTransactionalCursor::new(cas_db.clone(), eav_db.clone());
    DefaultPersistenceManager::new(cas_db, eav_db, cursor_provider)
}
