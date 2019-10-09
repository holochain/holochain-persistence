use holochain_persistence_api::{
    eav::{Attribute, EaviQuery, EntityAttributeValueIndex, EntityAttributeValueStorage},
    cas::content::Address,
    error::PersistenceResult,
    reporting::{ReportStorage, StorageReport},
        cas::content::AddressableContent,

};
use kv::{Config, Manager, Store};
use std::{
    collections::BTreeSet,
    fmt::{Debug, Error, Formatter},
    marker::{PhantomData, Send, Sync},
    path::Path,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

const EAV_BUCKET: &str = "EAV";

#[derive(Clone)]
pub struct EavLmdbStorage<A: Attribute> {
    id: Uuid,
    store: Arc<RwLock<Store>>,
    attribute: PhantomData<A>,
}

impl<A: Attribute> EavLmdbStorage<A> {
    pub fn new<P: AsRef<Path> + Clone>(db_path: P) -> EavLmdbStorage<A> {
        let eav_db_path = db_path.as_ref().join("eav").with_extension("db");
        std::fs::create_dir_all(eav_db_path.clone()).unwrap();

        // create a manager
        let mut mgr = Manager::new();

        // Next configure a database
        let mut cfg = Config::default(eav_db_path);
        // These flags makes write much much faster (https://docs.rs/lmdb-rkv/0.11.4/lmdb/struct.EnvironmentFlags.html)
        // at the expense of some safety during application crashes
        cfg.flag(lmdb::EnvironmentFlags::WRITE_MAP | lmdb::EnvironmentFlags::MAP_ASYNC);

        // Add a bucket named `cas`
        cfg.bucket(EAV_BUCKET, None);

        let handle = mgr.open(cfg).expect("Could not get a handle to the EAV database");

        EavLmdbStorage {
            id: Uuid::new_v4(),
            store: handle,
            attribute: PhantomData,
        }
    }
}

impl<A: Attribute> Debug for EavLmdbStorage<A> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.debug_struct("EavLmdbStorage")
            .field("id", &self.id)
            .finish()
    }
}

impl<A: Attribute> EntityAttributeValueStorage<A> for EavLmdbStorage<A>
where
    A: Sync + Send + serde::de::DeserializeOwned,
{
    fn add_eavi(
        &mut self,
        eav: &EntityAttributeValueIndex<A>,
    ) -> PersistenceResult<Option<EntityAttributeValueIndex<A>>> {

        let store = self.store.read()?;
        let bucket = store.bucket::<String, String>(Some(EAV_BUCKET)).unwrap();
        let mut txn = store.write_txn().unwrap();

        txn.set(
            &bucket,
            eav.index().to_string(),
            eav.content().to_string(),
        ).unwrap();

        txn.commit().unwrap();

        Ok(Some(eav.clone()))
    }

    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        let mut result = BTreeSet::new();
        
        let store = self.store.read()?;
        let bucket = store.bucket::<Address, String>(Some(EAV_BUCKET)).unwrap();
        let txn = store.read_txn().unwrap();

        // literally iterate the entire database
        let cursor = txn.read_cursor(&bucket).unwrap();
        let mut maybe_kv = cursor.get(None, kv::CursorOp::First);
        while let Ok((_key, value)) = maybe_kv {
            let record = serde_json::from_str(&value)?;
            result.insert(record);
            maybe_kv = cursor.get(None, kv::CursorOp::Next);
        }        

        Ok(query.run(result.iter().cloned()))
    }
}

impl<A: Attribute> ReportStorage for EavLmdbStorage<A>
where
    A: Sync + Send + serde::de::DeserializeOwned,
{
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        Ok(StorageReport::new(0)) // TODO: implement this
    }
}

#[cfg(test)]
pub mod tests {
    use crate::eav::lmdb::EavLmdbStorage;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::{
        cas::{
            content::{AddressableContent, ExampleAddressableContent},
            storage::{EavTestSuite},
        },
        eav::{
            Attribute, ExampleAttribute, storage::EavBencher,
        },
    };
    use tempfile::tempdir;

    #[test]
    fn lmdb_eav_round_trip() {
        let temp = tempdir().expect("test was supposed to create temp dir");

        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let entity_content =
            ExampleAddressableContent::try_from_content(&RawString::from("foo").into()).unwrap();
        let attribute = ExampleAttribute::WithPayload("favourite-color".to_string());
        let value_content =
            ExampleAddressableContent::try_from_content(&RawString::from("blue").into()).unwrap();

        EavTestSuite::test_round_trip(
            EavLmdbStorage::new(temp_path),
            entity_content,
            attribute,
            value_content,
        )
    }

    fn new_store<A: Attribute>() -> EavLmdbStorage<A> {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        EavLmdbStorage::new(temp_path)
    }

    #[bench]
    fn bench_lmdb_add(b: &mut test::Bencher) {
        let store = new_store();
        EavBencher::bench_add(b, store);
    }

    #[bench]
    fn bench_lmdb_fetch(b: &mut test::Bencher) {
        let store = new_store();
        EavBencher::bench_fetch(b, store);        
    }

    #[test]
    fn lmdb_eav_one_to_many() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavLmdbStorage::new(temp_path);
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavLmdbStorage<ExampleAttribute>,
        >(eav_storage, &ExampleAttribute::default());
    }

    #[test]
    fn lmdb_eav_many_to_one() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavLmdbStorage::new(temp_path);
        EavTestSuite::test_many_to_one::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavLmdbStorage<ExampleAttribute>,
        >(eav_storage, &ExampleAttribute::default());
    }

    #[test]
    fn lmdb_eav_range() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavLmdbStorage::new(temp_path);
        EavTestSuite::test_range::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavLmdbStorage<ExampleAttribute>,
        >(eav_storage, &ExampleAttribute::default());
    }

    #[test]
    fn lmdb_eav_prefixes() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavLmdbStorage::new(temp_path);
        EavTestSuite::test_multiple_attributes::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavLmdbStorage<ExampleAttribute>,
        >(
            eav_storage,
            vec!["a_", "b_", "c_", "d_"]
                .into_iter()
                .map(|p| ExampleAttribute::WithPayload(p.to_string() + "one_to_many"))
                .collect(),
        );
    }

    #[test]
    fn lmdb_tombstone() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavLmdbStorage::new(temp_path);
        EavTestSuite::test_tombstone::<ExampleAddressableContent, EavLmdbStorage<_>>(eav_storage)
    }
}
