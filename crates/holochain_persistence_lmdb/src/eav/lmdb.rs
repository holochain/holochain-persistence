use holochain_persistence_api::{
    cas::content::AddressableContent,
    eav::{
        Attribute, EavFilter, EaviQuery, EntityAttributeValueIndex, EntityAttributeValueStorage,
    },
    error::{PersistenceError, PersistenceResult},
    reporting::{ReportStorage, StorageReport},
};
// use kv::{Config, Manager, Store, Error as KvError};
use rkv::{
    error::StoreError, DatabaseFlags, EnvironmentFlags, Manager, Rkv, SingleStore, StoreOptions,
    Value,
};
use std::{
    collections::BTreeSet,
    fmt::{Debug, Error, Formatter},
    marker::{PhantomData, Send, Sync},
    path::Path,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

const EAV_BUCKET: &str = "EAV";
const MAX_SIZE_BYTES: usize = 104857600; // TODO: Discuss what this should be, currently 100MB

#[derive(Clone)]
pub struct EavLmdbStorage<A: Attribute> {
    id: Uuid,
    store: SingleStore,
    manager: Arc<RwLock<Rkv>>,
    attribute: PhantomData<A>,
}

impl<A: Attribute> EavLmdbStorage<A> {
    pub fn new<P: AsRef<Path> + Clone>(db_path: P) -> EavLmdbStorage<A> {
        let cas_db_path = db_path.as_ref().join("eav").with_extension("db");
        std::fs::create_dir_all(cas_db_path.clone())
            .expect("Could not create file path for CAS store");

        let manager = Manager::singleton()
            .write()
            .unwrap()
            .get_or_create(cas_db_path.as_path(), |path: &Path| {
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
            })
            .expect("Could not create the environment");

        let env = manager
            .read()
            .expect("Could not get a read lock on the manager");

        // Then you can use the environment handle to get a handle to a datastore:
        let options = StoreOptions {
            create: true,
            flags: DatabaseFlags::empty(),
        };
        let store: SingleStore = env
            .open_single(EAV_BUCKET, options)
            .expect("Could not create EAV store");

        EavLmdbStorage {
            id: Uuid::new_v4(),
            store: store,
            manager: manager.clone(),
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

impl<A: Attribute> EavLmdbStorage<A>
where
    A: Sync + Send + serde::de::DeserializeOwned,
{
    fn add_lmdb_eavi(
        &mut self,
        eav: &EntityAttributeValueIndex<A>,
    ) -> Result<Option<EntityAttributeValueIndex<A>>, StoreError> {
        let env = self.manager.read().unwrap();
        let mut writer = env.write()?;

        // use a clever key naming scheme to speed up exact match queries on the entity
        let key = format!("{}::{}", eav.entity(), eav.index());

        self.store
            .put(&mut writer, key, &Value::Json(&eav.content().to_string()))?;

        writer.commit()?;

        Ok(Some(eav.clone()))
    }

    fn fetch_lmdb_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> Result<BTreeSet<EntityAttributeValueIndex<A>>, StoreError> {
        let env = self.manager.read().unwrap();
        let reader = env.read()?;

        let entries = match &query.entity {
            EavFilter::Exact(entity) => {
                // Can optimize here thanks to the sorted keys and only iterate matching entities
                self.store
                    .iter_from(&reader, format!("{}::{}", entity, 0))? // start at the first key containing the entity address
                    .filter_map(Result::ok)
                    .take_while(|(k, _v)| {
                        // stop at the first key that doesn't match
                        String::from_utf8(k.to_vec())
                            .unwrap()
                            .contains(&entity.to_string())
                    })
                    .filter_map(|(_k, v)| match v {
                        Some(Value::Json(s)) => serde_json::from_str(&s).ok(),
                        _ => None,
                    })
                    .collect::<BTreeSet<EntityAttributeValueIndex<A>>>()
            }

            _ => {
                // In this case all we can do is iterate the entire database
                self.store
                    .iter_start(&reader)?
                    .filter_map(Result::ok)
                    .filter_map(|(_k, v)| match v {
                        Some(Value::Json(s)) => serde_json::from_str(&s).ok(),
                        _ => None,
                    })
                    .collect::<BTreeSet<EntityAttributeValueIndex<A>>>()
            }
        };
        let entries_iter = entries.iter().cloned();
        Ok(query.run(entries_iter))
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
        self.add_lmdb_eavi(eav)
            .map_err(|e| PersistenceError::from(format!("EAV add error: {}", e)))
    }

    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        self.fetch_lmdb_eavi(query)
            .map_err(|e| PersistenceError::from(format!("EAV fetch error: {}", e)))
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
            storage::EavTestSuite,
        },
        eav::{storage::EavBencher, Attribute, ExampleAttribute},
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
    fn bench_lmdb_eav_add(b: &mut test::Bencher) {
        let store = new_store();
        EavBencher::bench_add(b, store);
    }

    #[bench]
    fn bench_lmdb_eav_fetch_all(b: &mut test::Bencher) {
        let store = new_store();
        EavBencher::bench_fetch_all(b, store);
    }

    #[bench]
    fn bench_lmdb_eav_fetch_exact(b: &mut test::Bencher) {
        let store = new_store();
        EavBencher::bench_fetch_exact(b, store);
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
