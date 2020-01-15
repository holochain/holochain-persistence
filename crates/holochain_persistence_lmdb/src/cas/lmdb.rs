use crate::common::LmdbInstance;
//use holochain_json_api::json::JsonString;
use crate::error::to_api_error;
use holochain_persistence_api::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::ContentAddressableStorage,
    },
    error::{PersistenceError, PersistenceResult},
    hash::HashString,
    reporting::{ReportStorage, StorageReport},
};
use rkv::{error::StoreError, Reader, Value};
use std::{
    fmt::{Debug, Error, Formatter},
    path::Path,
};
use uuid::Uuid;

pub const CAS_BUCKET: &str = "cas";

#[derive(Clone)]
pub struct LmdbStorage {
    pub id: Uuid,
    pub lmdb: LmdbInstance,
}

impl Debug for LmdbStorage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.debug_struct("LmdbStorage").field("id", &self.id).finish()
    }
}

impl LmdbStorage {
    pub fn new<P: AsRef<Path> + Clone>(db_path: P, initial_map_bytes: Option<usize>) -> Self {
        Self::wrap(&LmdbInstance::new(CAS_BUCKET, db_path, initial_map_bytes))
    }

    pub fn wrap(lmdb: &LmdbInstance) -> Self {
        Self {
            id: Uuid::new_v4(),
            lmdb: lmdb.clone(),
        }
    }
}

fn handle_cursor_result(
    result: Result<Option<rkv::Value>, StoreError>,
) -> Result<Option<Content>, StoreError> {
    match result {
        Ok(Some(Value::Json(s))) => Ok(Some(serde_json::from_str(&s).unwrap())),
        Ok(None) => Ok(None),
        Ok(Some(_v)) => Err(StoreError::DataError(rkv::DataError::UnexpectedType {
            actual: rkv::value::Type::Json,
            expected: rkv::value::Type::Json,
        })),
        Err(e) => Err(e),
    }
}

fn handle_cursor_tuple_result(
    result: Result<(&[u8], Option<rkv::Value>), StoreError>,
) -> Result<(Address, Option<Content>), StoreError> {
    match result {
        Ok((address, Some(Value::Json(s)))) => Ok((
            HashString::from(address.to_vec()),
            Some(serde_json::from_str(&s).unwrap()),
        )),
        Ok((address, None)) => Ok((HashString::from(address.to_vec()), None)),
        Ok((_address, Some(_v))) => Err(StoreError::DataError(rkv::DataError::UnexpectedType {
            actual: rkv::value::Type::Json,
            expected: rkv::value::Type::Json,
        })),
        Err(e) => Err(e),
    }
}

impl LmdbStorage {
    pub fn lmdb_add<'env>(
        &self,
        mut writer: &mut rkv::Writer<'env>,
        content: &dyn AddressableContent,
    ) -> Result<(), StoreError> {
        self.lmdb.add(
            &mut writer,
            content.address(),
            &Value::Json(&content.content().to_string()),
        )
    }

    pub fn lmdb_resizable_add<'env>(
        &self,
        content: &dyn AddressableContent,
    ) -> Result<(), StoreError> {
        self.lmdb.resizable_add(
            &content.address(),
            &Value::Json(&content.content().to_string()),
        )
    }

    pub fn lmdb_fetch(
        &self,
        reader: &Reader,
        address: &Address,
    ) -> Result<Option<Content>, StoreError> {
        let result = self.lmdb.store.get(reader, address.clone());
        handle_cursor_result(result)
    }

    pub fn lmdb_iter(
        &self,
        reader: &Reader,
    ) -> Result<Vec<(Address, Option<Content>)>, StoreError> {
        self.lmdb
            .store
            .iter_start(reader)?
            .map(handle_cursor_tuple_result)
            .collect::<Result<Vec<(Address, Option<Content>)>, StoreError>>()
    }
}

impl ContentAddressableStorage for LmdbStorage {
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        self.lmdb_resizable_add(content)
            .map_err(|e| PersistenceError::from(format!("CAS add error: {}", e)))
    }

    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        let rkv = self.lmdb.rkv.read().unwrap();
        let reader: rkv::Reader = rkv.read().map_err(to_api_error)?;

        self.lmdb_fetch(&reader, address)
            .map_err(|e| PersistenceError::from(format!("CAS fetch error: {}", e)))
            .map(|result| match result {
                Some(_) => true,
                None => false,
            })
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let rkv = self.lmdb.rkv.read().unwrap();
        let reader = rkv.read().map_err(to_api_error)?;

        self.lmdb_fetch(&reader, address)
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
            storage::{CasBencher, ContentAddressableStorage, StorageTestSuite},
        },
        reporting::{ReportStorage, StorageReport},
    };
    use tempfile::{tempdir, TempDir};

    pub fn test_lmdb_cas() -> (LmdbStorage, TempDir) {
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        (LmdbStorage::new(dir.path(), None), dir)
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
        let (cas, _) = test_lmdb_cas();
        // add some content
        cas.add(&Content::from_json("some bytes"))
            .expect("could not add to CAS");
        assert_eq!(cas.get_storage_report().unwrap(), StorageReport::new(0),);

        // add some more
        cas.add(&Content::from_json("more bytes"))
            .expect("could not add to CAS");
        assert_eq!(cas.get_storage_report().unwrap(), StorageReport::new(0 + 0),);
    }
}
