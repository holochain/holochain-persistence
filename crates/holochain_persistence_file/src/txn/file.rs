use holochain_json_api::json::JsonString;
use holochain_persistence_api::{
    eav::*,
    error::*,
    txn::{DefaultPersistenceManager, NonTransactionalCursor},
};

use serde::de::DeserializeOwned;
use std::{convert::TryFrom, path::Path};

use crate::{cas::file::FilesystemStorage, eav::file::EavFileStorage};

pub type Manager<A> = DefaultPersistenceManager<
    A,
    FilesystemStorage,
    EavFileStorage<A>,
    NonTransactionalCursor<A, FilesystemStorage, EavFileStorage<A>>,
>;

#[derive(Clone, Debug, Shrinkwrap)]
pub struct FilesystemManager<
    A: Attribute
        + DeserializeOwned
        + TryFrom<JsonString>
        + TryFrom<String>
        + Into<JsonString>
        + std::fmt::Display,
>(Manager<A>);

impl<
        A: Attribute
            + DeserializeOwned
            + TryFrom<JsonString>
            + TryFrom<String>
            + Into<JsonString>
            + std::fmt::Display,
    > Default for FilesystemManager<A>
{
    fn default() -> Self {
        let base_path = tempfile::tempdir().unwrap();
        let base_path = base_path.path();
        let cas_path = base_path.join("cas");
        let eav_path = base_path.join("eav");
        new_manager(cas_path.clone(), eav_path.clone()).expect(
            format!(
                "Failed to create CAS/EAV file storage using temporary path locations: {:?}, {:?}",
                cas_path, eav_path
            )
            .as_str(),
        )
    }
}

/// Creates non transactional persistence manager for a file system
/// backed database. Cursors are *not* atomic.
pub fn new_manager<
    A: Attribute
        + DeserializeOwned
        + TryFrom<JsonString>
        + TryFrom<String>
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
    Ok(FilesystemManager(DefaultPersistenceManager::new(
        cas_db,
        eav_db,
        cursor_provider,
    )))
}

#[cfg(test)]
mod tests {

    use super::FilesystemManager;
    use holochain_persistence_api::eav::ExampleAttribute;

    #[test]
    fn can_create_default_file_persistence_manager() {
        let _default : FilesystemManager<ExampleAttribute> =
            FilesystemManager::default();
    }

}
