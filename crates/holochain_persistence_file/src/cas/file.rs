use holochain_json_api::json::JsonString;
use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::{AddContent, FetchContent},
    },
    error::PersistenceResult,
    reporting::ReportStorage,
};

use std::{
    fs::{create_dir_all, read_to_string, write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use holochain_persistence_api::has_uuid::HasUuid;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct FilesystemStorage {
    /// path to the directory where content will be saved to disk
    dir_path: PathBuf,
    id: Uuid,
    lock: Arc<RwLock<()>>,
}

impl PartialEq for FilesystemStorage {
    fn eq(&self, other: &FilesystemStorage) -> bool {
        self.id == other.id
    }
}

impl FilesystemStorage {
    pub fn new<P: AsRef<Path>>(dir_path: P) -> PersistenceResult<FilesystemStorage> {
        let dir_path = dir_path.as_ref().into();

        Ok(FilesystemStorage {
            dir_path,
            id: Uuid::new_v4(),
            lock: Arc::new(RwLock::new(())),
        })
    }

    /// builds an absolute path for an AddressableContent address
    fn address_to_path(&self, address: &Address) -> PathBuf {
        // using .txt extension because content is arbitrary and controlled by the
        // AddressableContent trait implementation
        self.dir_path
            .join(address.to_string())
            .with_extension("txt")
    }
}

impl AddContent for FilesystemStorage {
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        let _guard = self.lock.write()?;
        // @TODO be more efficient here
        // @see https://github.com/holochain/holochain-rust/issues/248
        create_dir_all(&self.dir_path)?;

        write(
            self.address_to_path(&content.address()),
            content.content().to_string(),
        )?;

        Ok(())
    }
}
impl FetchContent for FilesystemStorage {
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        let _guard = self.lock.read()?;
        Ok(Path::new(&self.address_to_path(address)).is_file())
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let _guard = self.lock.read()?;
        if self.contains(&address)? {
            Ok(Some(JsonString::from_json(&read_to_string(
                self.address_to_path(address),
            )?)))
        } else {
            Ok(None)
        }
    }
}

impl HasUuid for FilesystemStorage {
    fn get_id(&self) -> Uuid {
        self.id
    }
}

impl ReportStorage for FilesystemStorage {}

#[cfg(test)]
pub mod tests {
    use crate::cas::file::FilesystemStorage;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::cas::{
        content::{ExampleAddressableContent, OtherExampleAddressableContent},
        storage::StorageTestSuite,
    };
    use tempfile::{tempdir, TempDir};

    pub fn test_file_cas() -> (FilesystemStorage, TempDir) {
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        (FilesystemStorage::new(dir.path()).unwrap(), dir)
    }

    #[test]
    /// show that content of different types can round trip through the same storage
    /// this is copied straight from the example with a file CAS
    fn file_content_round_trip_test() {
        let (cas, _dir) = test_file_cas();
        let test_suite = StorageTestSuite::new(cas);
        test_suite.round_trip_test::<ExampleAddressableContent, OtherExampleAddressableContent>(
            RawString::from("foo").into(),
            RawString::from("bar").into(),
        );
    }
}
