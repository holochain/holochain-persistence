use holochain_json_api::json::JsonString;
use holochain_persistence_api::{
    eav::*,
    error::*,
    txn::{DefaultPersistenceManager, NonTransactionalCursor},
};

use serde::de::DeserializeOwned;
use std::path::Path;

use crate::{cas::file::FilesystemStorage, eav::file::EavFileStorage};

pub type FilesystemManager<A> = DefaultPersistenceManager<
    A,
    FilesystemStorage,
    EavFileStorage<A>,
    NonTransactionalCursor<A, FilesystemStorage, EavFileStorage<A>>,
>;

/// Creates non transactional persistence manager for a file system
/// backed database. Cursors are *not* atomic.
pub fn new_manager<
    A: Attribute
        + DeserializeOwned
        + From<JsonString>
        + From<String>
        + Into<JsonString>
        + std::fmt::Display,
    P: AsRef<Path> + Clone,
    EP: AsRef<Path> + Clone,
>(
    cas_dir: P,
    eav_dir: EP,
) -> PersistenceResult<FilesystemManager<A>> {
    let cas_db = FilesystemStorage::new(cas_dir)?;

    let eav_db: EavFileStorage<A> = EavFileStorage::new(eav_dir)?;

    let cursor_provider = NonTransactionalCursor::new(cas_db.clone(), eav_db.clone());
    Ok(DefaultPersistenceManager::new(
        cas_db,
        eav_db,
        cursor_provider,
    ))
}
