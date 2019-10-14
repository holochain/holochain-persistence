use holochain_persistence_api::{
    cas::content::AddressableContent,
    eav::{
        Attribute, EavFilter, EaviQuery, EntityAttributeValueIndex, EntityAttributeValueStorage,
    },
    error::{PersistenceError, PersistenceResult},
    reporting::{ReportStorage, StorageReport},
};
// use kv::{Config, Manager, Store, Error as KvError};
use crate::common::LmdbInstance;
use rkv::{error::StoreError, Value};
use std::{
    collections::BTreeSet,
    fmt::{Debug, Error, Formatter},
    marker::{PhantomData, Send, Sync},
    path::Path,
};
use uuid::Uuid;

const EAV_BUCKET: &str = "EAV";

#[derive(Clone)]
pub struct EavLmdbStorage<A: Attribute> {
    id: Uuid,
    lmdb: LmdbInstance,
    attribute: PhantomData<A>,
}

impl<A: Attribute> EavLmdbStorage<A> {
    pub fn new<P: AsRef<Path> + Clone>(
        db_path: P,
        initial_map_bytes: Option<usize>,
    ) -> EavLmdbStorage<A> {
        EavLmdbStorage {
            id: Uuid::new_v4(),
            lmdb: LmdbInstance::new(EAV_BUCKET, db_path, initial_map_bytes),
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

fn handle_cursor_result<A: Attribute>(
    result: Result<(&[u8], Option<rkv::Value>), StoreError>,
) -> Result<EntityAttributeValueIndex<A>, StoreError>
where
    A: Sync + Send + serde::de::DeserializeOwned,
{
    match result {
        Ok((_k, Some(Value::Json(s)))) => Ok(serde_json::from_str(&s).unwrap()),
        Ok((_k, None)) => Err(StoreError::DataError(rkv::DataError::Empty)),
        Ok((_k, Some(_v))) => Err(StoreError::DataError(rkv::DataError::UnexpectedType {
            actual: rkv::value::Type::Json,
            expected: rkv::value::Type::Json,
        })),
        Err(e) => Err(e),
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
        let env = self.lmdb.manager.read().unwrap();
        let reader = env.read()?;

        // use a clever key naming scheme to speed up exact match queries on the entity
        let mut new_eav = eav.clone();
        let mut key = format!("{}::{}", new_eav.entity(), new_eav.index());

        // need to check there isn't a duplicate key though and if there is create a new EAVI which
        // will have a more recent timestamp
        while let Ok(Some(_)) = self.lmdb.store.get(&reader, key.clone()) {
            new_eav = EntityAttributeValueIndex::new(&eav.entity(), &eav.attribute(), &eav.value())
                .unwrap();
            key = format!("{}::{}", new_eav.entity(), new_eav.index());
        }

        self.lmdb
            .add(key, &Value::Json(&new_eav.content().to_string()))?;
        Ok(Some(eav.clone()))
    }

    fn fetch_lmdb_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> Result<BTreeSet<EntityAttributeValueIndex<A>>, StoreError> {
        let env = self.lmdb.manager.read().unwrap();
        let reader = env.read()?;

        let entries = match &query.entity {
            EavFilter::Exact(entity) => {
                // Can optimize here thanks to the sorted keys and only iterate matching entities
                self.lmdb
                    .store
                    .iter_from(&reader, format!("{}::{}", entity, 0))? // start at the first key containing the entity address
                    .take_while(|r| {
                        // stop at the first key that doesn't match (but keep taking errors)
                        match r {
                            Ok((k, _)) => String::from_utf8(k.to_vec())
                                .unwrap()
                                .contains(&entity.to_string()),
                            _ => true,
                        }
                    })
                    .map(handle_cursor_result)
                    .collect::<Result<BTreeSet<EntityAttributeValueIndex<A>>, StoreError>>()?
            }

            _ => {
                // In this case all we can do is iterate the entire database
                self.lmdb
                    .store
                    .iter_start(&reader)?
                    .map(handle_cursor_result)
                    .collect::<Result<BTreeSet<EntityAttributeValueIndex<A>>, StoreError>>()?
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
            EavLmdbStorage::new(temp_path, None),
            entity_content,
            attribute,
            value_content,
        )
    }

    fn new_store<A: Attribute>() -> EavLmdbStorage<A> {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        EavLmdbStorage::new(temp_path, None)
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
        let eav_storage = EavLmdbStorage::new(temp_path, None);
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
        let eav_storage = EavLmdbStorage::new(temp_path, None);
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
        let eav_storage = EavLmdbStorage::new(temp_path, None);
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
        let eav_storage = EavLmdbStorage::new(temp_path, None);
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
        let eav_storage = EavLmdbStorage::new(temp_path, None);
        EavTestSuite::test_tombstone::<ExampleAddressableContent, EavLmdbStorage<_>>(eav_storage)
    }
}
