use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::ContentAddressableStorage,
    },
    error::{PersistenceResult, PersistenceError},
    reporting::{ReportStorage, StorageReport},
};
use holochain_json_api::json::JsonString;
use lmdb_zero as lmdb;
use lmdb_zero::error::Error as LmdbError;
use std::{
    fmt::{Debug, Error, Formatter},
    path::Path,
    sync::{Arc},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct LmdbStorage {
    id: Uuid,
    db: Arc<lmdb::Database<'static>>,
    env: Arc<lmdb::Environment>,
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
        let env_wrap = unsafe {
            lmdb::EnvBuilder::new().unwrap().open(
                cas_db_path.to_str().unwrap(),
                lmdb::open::Flags::empty(),
                0o600
            ).unwrap()
        };
        let env = Arc::new(env_wrap);

        let db = lmdb::Database::open(
            env.clone(),
            None,
            &lmdb::DatabaseOptions::defaults()
        ).unwrap();

        LmdbStorage {
            id: Uuid::new_v4(),
            db: Arc::new(db),
            env,
        }
    }
}

impl ContentAddressableStorage for LmdbStorage {
    fn add(&mut self, content: &dyn AddressableContent) -> PersistenceResult<()> {        
        let txn = lmdb::WriteTransaction::new(self.env.clone()).unwrap();
        // An accessor is used to control memory access.
        // NB You can only have one live accessor from a particular transaction
        // at a time. Violating this results in a panic at runtime.
        {
            let mut access = txn.access();
            access.put(
                &self.db,
                content.address().to_string().as_bytes(), // TODO: Actually handle this conversion properly
                content.content().to_string().as_bytes(),
                lmdb::put::Flags::empty()
            ).unwrap();
        }
        // Commit the changes so they are visible to later transactions
        txn.commit().unwrap();

        Ok(())
    }

    // TODO: optimize this to not do a full read
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        let txn = lmdb::ReadTransaction::new(self.env.clone()).unwrap();
        let access = txn.access();
        let get_result = access.get::<_, str>(&self.db, address.to_string().as_bytes());
        match get_result {
            Ok(_) => Ok(true),
            Err(LmdbError::Code(-30798)) => Ok(false), // This is the code for `value not found`
            Err(e) => Err(PersistenceError::new(&format!("{:?}", e)))
        }
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let txn = lmdb::ReadTransaction::new(self.env.clone()).unwrap();
        let access = txn.access();
        let get_result = access.get::<_, str>(&self.db, address.to_string().as_bytes());
        match get_result {
            Ok(result) => Ok(Some(JsonString::from_json(result))),
            Err(LmdbError::Code(-30798)) => Ok(None), // This is the code for `value not found`
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
