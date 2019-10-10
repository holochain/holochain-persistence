use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::ContentAddressableStorage,
    },
    error::{PersistenceResult, PersistenceError},
    reporting::{ReportStorage, StorageReport},
};
use holochain_json_api::json::JsonString;
use rkv::{Manager, Rkv, SingleStore, Value, StoreOptions, DatabaseFlags, EnvironmentFlags, error::{StoreError, DataError}};
use std::{
    fmt::{Debug, Error, Formatter},
    path::Path,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

const CAS_BUCKET: &str = "cas";
const MAX_SIZE_BYTES: usize = 104857600; // TODO: Discuss what this should be

#[derive(Clone)]
pub struct LmdbStorage {
    id: Uuid,
    store: SingleStore,
    manager: Arc<RwLock<Rkv>>,
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
        
        let manager = Manager::singleton().write().unwrap().get_or_create(cas_db_path.as_path(), |path: &Path| {
            let mut env_builder = Rkv::environment_builder();
            env_builder
                // max size of memory map, can be changed later 
                .set_map_size(MAX_SIZE_BYTES)
                // max number of DBs in this environment
                .set_max_dbs(1) 
                // Thes flags make writes waaaaay faster by async writing to disk rather than blocking
                // There is some loss of data integrity guarantees that comes with this
                .set_flags(EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC);
            Rkv::from_env(path, env_builder)
        }).expect("Could not create the environment");

        let env = manager.read().expect("Could not get a read lock on the manager");

        // Then you can use the environment handle to get a handle to a datastore:
        let options = StoreOptions{create: true, flags: DatabaseFlags::empty()};
        let store: SingleStore = env.open_single(CAS_BUCKET, options).expect("Could not create CAS store");

        LmdbStorage {
            id: Uuid::new_v4(),
            store: store,
            manager: manager.clone(),
        }
    }
}

impl LmdbStorage {
    fn lmdb_add(&mut self, content: &dyn AddressableContent) -> Result<(), StoreError> {     
        let env = self.manager.read().unwrap();
        let mut writer = env.write()?;

        self.store.put(
            &mut writer,
            content.address(),
            &Value::Str(&content.content().to_string()),
        )?;

        writer.commit()?;

        Ok(())
    }

    fn lmdb_fetch(&self, address: &Address) -> Result<Option<Content>, StoreError> {
        let env = self.manager.read().unwrap();
        let reader = env.read()?;

        match self.store.get(&reader, address.clone()) {
            Ok(Some(value)) => {
                match value {
                    Value::Str(s) => Ok(Some(JsonString::from_json(s))),
                    _ => Err(StoreError::DataError(DataError::Empty))
                }
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
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
