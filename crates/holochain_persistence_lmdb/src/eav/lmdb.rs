use holochain_persistence_api::{
    eav::{Attribute, EaviQuery, EntityAttributeValueIndex, EntityAttributeValueStorage},
    error::PersistenceResult,
    reporting::{ReportStorage, StorageReport},
        cas::content::AddressableContent,

};
use lmdb_zero as lmdb;
use std::{
    collections::BTreeSet,
    fmt::{Debug, Error, Formatter},
    marker::{PhantomData, Send, Sync},
    path::Path,
    sync::{Arc},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct EavLmdbStorage<A: Attribute> {
    id: Uuid,
    db: Arc<lmdb::Database<'static>>,
    env: Arc<lmdb::Environment>,
    attribute: PhantomData<A>,
}

impl<A: Attribute> EavLmdbStorage<A> {
    pub fn new<P: AsRef<Path> + Clone>(db_path: P) -> EavLmdbStorage<A> {
        let eav_db_path = db_path.as_ref().join("eav").with_extension("db");
        std::fs::create_dir_all(eav_db_path.clone()).unwrap();
        let env_wrap = unsafe {

            let mut flags = lmdb::open::Flags::empty();
            flags.insert(lmdb::open::MAPASYNC); // When using WRITEMAP, use asynchronous flushes to disk.
            flags.insert(lmdb::open::WRITEMAP); // Use a writeable memory map unless RDONLY is set. 
            //This is faster and uses fewer mallocs, but loses protection from application bugs like 
            // wild pointer writes and other bad updates into the database
            // Combined these flags VERY significantly reduce write times

            let mut env_builder = lmdb::EnvBuilder::new().unwrap();
            env_builder.set_mapsize(10485760).unwrap();
            env_builder.open(
                eav_db_path.to_str().unwrap(),
                flags,
                0o600
            ).unwrap()
        };
        let env = Arc::new(env_wrap);

        let db = lmdb::Database::open(
            env.clone(),
            None,
            &lmdb::DatabaseOptions::defaults()
        ).unwrap();

        EavLmdbStorage {
            id: Uuid::new_v4(),
            db: Arc::new(db),
            env,
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
        let txn = lmdb::WriteTransaction::new(self.env.clone()).unwrap();
        {
            let mut access = txn.access();
            access.put(
                &self.db,
                eav.index().to_string().as_bytes(),
                eav.content().to_string().as_bytes(),
                lmdb::put::Flags::empty()
            ).unwrap();
        }
        txn.commit().unwrap();
        Ok(Some(eav.clone()))
    }

    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        let mut result = BTreeSet::new();
        
        let txn = lmdb::ReadTransaction::new(self.env.clone()).unwrap();
        let access = txn.access();
        let mut cursor = txn.cursor(self.db.clone()).unwrap();

        // literally iterate the entire database
        let mut maybe_kv = cursor.first::<str,str>(&access);
        while let Ok((_key, value)) = maybe_kv {
            let record = serde_json::from_str(value)?;
            result.insert(record);
            maybe_kv = cursor.next(&access);
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
