use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::ContentAddressableStorage,
    },
    error::{PersistenceResult, PersistenceError},
    reporting::{ReportStorage, StorageReport},
};
use holochain_json_api::json::JsonString;
// use lmdb_zero as lmdb;
// use lmdb_zero::error::Error as LmdbError;
use kv::{Config, Manager, Store, Error as KvError};
use std::{
    fmt::{Debug, Error, Formatter},
    path::Path,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct LmdbStorage {
    id: Uuid,
    store: Arc<RwLock<Store>>,
}

impl Debug for LmdbStorage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.debug_struct("LmdbStorage")
            .field("id", &self.id)
            .finish()
    }
}

impl LmdbStorage {
    pub fn new<P: AsRef<Path> + Clone>(db_path: P) -> LmdbStorage {
        let cas_db_path = db_path.as_ref().join("cas").with_extension("db");
        std::fs::create_dir_all(cas_db_path.clone()).unwrap();
        
        let mut mgr = Manager::new();

        // Next configure a database
        let mut cfg = Config::default(cas_db_path);

        // Add a bucket named `cas`
        cfg.bucket("cas", None);

        let handle = mgr.open(cfg).unwrap();

        LmdbStorage {
            id: Uuid::new_v4(),
            store: handle,
        }
    }
}

impl ContentAddressableStorage for LmdbStorage {
    fn add(&mut self, content: &dyn AddressableContent) -> PersistenceResult<()> {        
        let store = self.store.read()?;
        let bucket = store.bucket::<Address, String>(Some("cas")).unwrap();
        let mut txn = store.write_txn().unwrap();

        txn.set(
            &bucket,
            content.address(),
            content.content().to_string(),
        ).unwrap();

        txn.commit().unwrap();

        Ok(())
    }

    // TODO: optimize this to not do a full read
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        self.fetch(address).map(|result| {
            match result {
                Some(_) => true,
                None => false,
            }
        })
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let store = self.store.read()?;
        let bucket = store.bucket::<Address, String>(Some("cas")).unwrap();
        let txn = store.read_txn().unwrap();

        match txn.get(&bucket, address.clone()) {
            Ok(result) => Ok(Some(JsonString::from_json(&result))),
            Err(KvError::NotFound) => Ok(None),
            Err(e) => Err(PersistenceError::new(&format!("{:?}", e)))
        }
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

impl ReportStorage for LmdbStorage {
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        Ok(StorageReport::new(0)) // TODO: implement this
    }
}

#[cfg(test)]
mod tests {
    use crate::cas::lmdb::LmdbStorage;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::{
        cas::{
            content::{Content, ExampleAddressableContent, OtherExampleAddressableContent},
            storage::{ContentAddressableStorage, StorageTestSuite},
        },
        reporting::{ReportStorage, StorageReport},
    };
    use tempfile::{tempdir, TempDir};

    pub fn test_lmdb_cas() -> (LmdbStorage, TempDir) {
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        (LmdbStorage::new(dir.path()), dir)
    }

    #[test]
    /// show that content of different types can round trip through the same storage
    /// this is copied straight from the example with a file CAS
    fn lmdb_content_round_trip_test() {
        let (cas, _dir) = test_lmdb_cas();
        let test_suite = StorageTestSuite::new(cas);
        test_suite.round_trip_test::<ExampleAddressableContent, OtherExampleAddressableContent>(
            RawString::from("foo").into(),
            RawString::from("bar").into(),
        );
    }

    #[test]
    fn lmdb_report_storage_test() {
        let (mut cas, _) = test_lmdb_cas();
        // add some content
        cas.add(&Content::from_json("some bytes"))
            .expect("could not add to CAS");
        assert_eq!(cas.get_storage_report().unwrap(), StorageReport::new(0),);

        // add some more
        cas.add(&Content::from_json("more bytes"))
            .expect("could not add to CAS");
        assert_eq!(
            cas.get_storage_report().unwrap(),
            StorageReport::new(0 + 0),
        );
    }
}
