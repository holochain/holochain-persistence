use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::{AddContent, FetchContent},
    },
    error::PersistenceResult,
    reporting::ReportStorage,
};

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

use holochain_persistence_api::has_uuid::HasUuid;

#[derive(Clone, Debug)]
pub struct MemoryStorage {
    storage: Arc<RwLock<HashMap<Address, Content>>>,
    id: Uuid,
}

impl PartialEq for MemoryStorage {
    fn eq(&self, other: &MemoryStorage) -> bool {
        self.id == other.id
    }
}
impl Default for MemoryStorage {
    fn default() -> MemoryStorage {
        MemoryStorage {
            storage: Arc::new(RwLock::new(HashMap::new())),
            id: Uuid::new_v4(),
        }
    }
}

impl MemoryStorage {
    pub fn new() -> MemoryStorage {
        Default::default()
    }
}

impl AddContent for MemoryStorage {
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        let mut map = self.storage.write()?;
        map.insert(content.address(), content.content());
        Ok(())
    }
}

impl FetchContent for MemoryStorage {
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        let map = self.storage.read()?;
        Ok(map.contains_key(address))
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let map = self.storage.read()?;
        Ok(map.get(address).cloned())
    }
}

impl HasUuid for MemoryStorage {
    fn get_id(&self) -> Uuid {
        self.id
    }
}

impl ReportStorage for MemoryStorage {}

#[cfg(test)]
pub mod tests {
    use crate::cas::memory::MemoryStorage;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::cas::{
        content::{ExampleAddressableContent, OtherExampleAddressableContent},
        storage::StorageTestSuite,
    };

    pub fn test_memory_storage() -> MemoryStorage {
        MemoryStorage::new()
    }

    #[test]
    fn memory_round_trip() {
        let test_suite = StorageTestSuite::new(test_memory_storage());
        test_suite.round_trip_test::<ExampleAddressableContent, OtherExampleAddressableContent>(
            RawString::from("foo").into(),
            RawString::from("bar").into(),
        );
    }
}
