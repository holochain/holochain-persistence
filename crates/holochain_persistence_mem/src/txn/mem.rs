use holochain_persistence_api::{
    eav::*,
    txn::{DefaultPersistenceManager, NonTransactionalCursor},
};

use serde::de::DeserializeOwned;

use crate::{cas::memory::MemoryStorage, eav::memory::EavMemoryStorage};

pub type MemoryManager<A> = DefaultPersistenceManager<
    A,
    MemoryStorage,
    EavMemoryStorage<A>,
    NonTransactionalCursor<A, MemoryStorage, EavMemoryStorage<A>>,
>;

/// Creates non transactional persistence manager for a memory
/// backed database. Cursors are *not* atomic.
pub fn new_manager<A: Attribute + DeserializeOwned>() -> MemoryManager<A> {
    let cas_db = MemoryStorage::new();

    let eav_db: EavMemoryStorage<A> = EavMemoryStorage::new();

    let cursor_provider = NonTransactionalCursor::new(cas_db.clone(), eav_db.clone());
    DefaultPersistenceManager::new(cas_db, eav_db, cursor_provider)
}
