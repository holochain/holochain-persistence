use holochain_json_api::error::JsonError;
use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::{AddContent, FetchContent},
    },
    error::PersistenceResult,
    reporting::{ReportStorage, StorageReport},
};

use holochain_persistence_api::has_uuid::HasUuid;
use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
use std::{
    fmt::{Debug, Error, Formatter},
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};
use uuid::Uuid;

const PERSISTENCE_INTERVAL: Duration = Duration::from_millis(5000);

#[derive(Clone)]
pub struct PickleStorage {
    id: Uuid,
    db: Arc<RwLock<PickleDb>>,
}

impl Debug for PickleStorage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.debug_struct("PickleStorage")
            .field("id", &self.id)
            .finish()
    }
}

impl PickleStorage {
    pub fn new<P: AsRef<Path> + Clone>(db_path: P) -> PickleStorage {
        let cas_db = db_path.as_ref().join("cas").with_extension("db");
        PickleStorage {
            id: Uuid::new_v4(),
            db: Arc::new(RwLock::new(
                PickleDb::load(
                    cas_db.clone(),
                    PickleDbDumpPolicy::PeriodicDump(PERSISTENCE_INTERVAL),
                    SerializationMethod::Cbor,
                )
                .unwrap_or_else(|_| {
                    PickleDb::new(
                        cas_db,
                        PickleDbDumpPolicy::PeriodicDump(PERSISTENCE_INTERVAL),
                        SerializationMethod::Cbor,
                    )
                }),
            )),
        }
    }
}

impl AddContent for PickleStorage {
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        let mut inner = self.db.write().unwrap();

        inner
            .set(&content.address().to_string(), &content.content())
            .map_err(|e| JsonError::ErrorGeneric(e.to_string()))?;

        Ok(())
    }
}
impl FetchContent for PickleStorage {
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        let inner = self.db.read().unwrap();

        Ok(inner.exists(&address.to_string()))
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let inner = self.db.read().unwrap();

        Ok(inner.get(&address.to_string()))
    }
}

impl HasUuid for PickleStorage {
    fn get_id(&self) -> Uuid {
        self.id
    }
}

impl ReportStorage for PickleStorage {
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        let db = self.db.read()?;
        let bytes_total = db.iter().fold(0, |total_bytes, kv| {
            let value = kv.get_value::<Content>().unwrap();
            total_bytes + value.to_string().bytes().len()
        });
        Ok(StorageReport::new(bytes_total))
    }
}

#[cfg(test)]
mod tests {
    use crate::cas::pickle::PickleStorage;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::{
        cas::{
            content::{Content, ExampleAddressableContent, OtherExampleAddressableContent},
            storage::{AddContent, CasBencher, StorageTestSuite},
        },
        reporting::{ReportStorage, StorageReport},
    };
    use tempfile::{tempdir, TempDir};

    pub fn test_pickle_cas() -> (PickleStorage, TempDir) {
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        (PickleStorage::new(dir.path()), dir)
    }

    #[bench]
    fn bench_pickle_cas_add(b: &mut test::Bencher) {
        let (store, _) = test_pickle_cas();
        CasBencher::bench_add(b, store);
    }

    #[bench]
    fn bench_pickle_cas_fetch(b: &mut test::Bencher) {
        let (store, _) = test_pickle_cas();
        CasBencher::bench_fetch(b, store);
    }

    #[test]
    /// show that content of different types can round trip through the same storage
    /// this is copied straight from the example with a file CAS
    fn pickle_content_round_trip_test() {
        let (cas, _dir) = test_pickle_cas();
        let test_suite = StorageTestSuite::new(cas);
        test_suite.round_trip_test::<ExampleAddressableContent, OtherExampleAddressableContent>(
            RawString::from("foo").into(),
            RawString::from("bar").into(),
        );
    }

    #[test]
    fn pickle_report_storage_test() {
        let (cas, _) = test_pickle_cas();
        // add some content
        cas.add(&Content::from_json("some bytes"))
            .expect("could not add to CAS");
        assert_eq!(cas.get_storage_report().unwrap(), StorageReport::new(10),);

        // add some more
        cas.add(&Content::from_json("more bytes"))
            .expect("could not add to CAS");
        assert_eq!(
            cas.get_storage_report().unwrap(),
            StorageReport::new(10 + 10),
        );
    }
}
