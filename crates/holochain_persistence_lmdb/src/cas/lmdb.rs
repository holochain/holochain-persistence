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

const CAS_BUCKET: &str = "cas";

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
        std::fs::create_dir_all(cas_db_path.clone()).expect("Could not create file path for CAS store");
        
        // create a manager
        let mut mgr = Manager::new();

        // Next configure a database
        let mut cfg = Config::default(cas_db_path);
        // These flags makes write much much faster (https://docs.rs/lmdb-rkv/0.11.4/lmdb/struct.EnvironmentFlags.html)
        // at the expense of some safety during application crashes
        cfg.flag(lmdb::EnvironmentFlags::WRITE_MAP | lmdb::EnvironmentFlags::MAP_ASYNC);

        // Add a bucket named `cas`
        cfg.bucket(CAS_BUCKET, None);

        let handle = mgr.open(cfg).expect("Could not get a handle to the CAS database");

        LmdbStorage {
            id: Uuid::new_v4(),
            store: handle,
        }
    }
}

impl LmdbStorage {
    fn lmdb_add(&mut self, content: &dyn AddressableContent) -> Result<(), KvError> {        
        let store = self.store.read()?;
        let bucket = store.bucket::<Address, String>(Some(CAS_BUCKET))?;
        let mut txn = store.write_txn()?;

        txn.set(
            &bucket,
            content.address(),
            content.content().to_string(),
        )?;

        txn.commit()?;

        Ok(())
    }

    fn lmdb_fetch(&self, address: &Address) -> Result<Option<Content>, KvError> {
        let store = self.store.read()?;
        let bucket = store.bucket::<Address, String>(Some(CAS_BUCKET)).unwrap();
        let txn = store.read_txn().unwrap();

        match txn.get(&bucket, address.clone()) {
            Ok(result) => Ok(Some(JsonString::from_json(&result))),
            Err(KvError::NotFound) => Ok(None),
            Err(e) => Err(e)
        }
    }
}

impl ContentAddressableStorage for LmdbStorage {

    fn add(&mut self, content: &dyn AddressableContent) -> PersistenceResult<()> {        
        self.lmdb_add(content)
            .map_err(|e| PersistenceError::from(format!("CAS add error: {}", e)))
    }

    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        self.fetch(address).map(|result| {
            match result {
                Some(_) => true,
                None => false,
            }
        })
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        self.lmdb_fetch(address)
            .map_err(|e| PersistenceError::from(format!("CAS fetch error: {}", e)))
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
            storage::{ContentAddressableStorage, StorageTestSuite, CasBencher},
        },
        reporting::{ReportStorage, StorageReport},
    };
    use tempfile::{tempdir, TempDir};

    pub fn test_lmdb_cas() -> (LmdbStorage, TempDir) {
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        (LmdbStorage::new(dir.path()), dir)
    }

    #[bench]
    fn bench_lmdb_cas_add(b: &mut test::Bencher) {
        let (store, _) = test_lmdb_cas();
        CasBencher::bench_add(b, store);
    }

    #[bench]
    fn bench_lmdb_cas_fetch(b: &mut test::Bencher) {
        let (store, _) = test_lmdb_cas();
        CasBencher::bench_fetch(b, store);        
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
