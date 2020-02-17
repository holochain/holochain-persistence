use crate::{
    cas::lmdb::LmdbStorage,
    common::{map_growth_factor, LmdbInstance},
    eav::lmdb::EavLmdbStorage,
    error::{is_store_full_error, is_store_full_result, to_api_error},
};
use holochain_logging::prelude::*;
use holochain_persistence_api::{
    cas::{content::*, storage::*},
    eav::*,
    error::*,
    has_uuid::HasUuid,
    reporting::{ReportStorage, StorageReport},
    txn::*,
    univ_map::*,
};
use rkv::EnvironmentFlags;
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;

/// A cursor over an lmdb environment
#[derive(Clone, Debug)]
pub struct LmdbCursor<A: Attribute> {
    cas_db: LmdbStorage,
    eav_db: EavLmdbStorage<A>,
    staging_cas_db: LmdbStorage,
    staging_eav_db: EavLmdbStorage<A>,
}

impl<A: Attribute + Sync + Send + DeserializeOwned> LmdbCursor<A> {
    /// Internal commit function which extracts `StoreError::MapFull` into the success value of
    /// a result where `true` indicates the commit is successful, and `false` means the map was
    /// full and retry is required with the newly allocated map size.
    fn commit_internal(&self) -> PersistenceResult<bool> {
        trace!("writer: commit_internal start");
        let staging_env_lock = self.staging_cas_db.lmdb.rkv().read().unwrap();
        trace!("writer: commit_internal got staging env lock");
        let staging_reader = staging_env_lock.read().map_err(to_api_error)?;
        trace!("writer: commit_internal got staging reader");

        let env_lock = self.cas_db.lmdb.rkv().write().unwrap();
        trace!("writer: commit_internal got env write lock");
        let mut writer = env_lock.write().unwrap();
        trace!("writer: commit_internal got writer");

        let copy_result = self
            .staging_cas_db
            .copy_all(&staging_reader, &self.cas_db, &mut writer);

        if is_store_full_result(&copy_result) {
            drop(writer);
            trace!("writer: commit_internal store full while adding cas data");
            let map_size = env_lock.info().map_err(to_api_error)?.map_size();
            env_lock
                .set_map_size(map_size * map_growth_factor())
                .map_err(to_api_error)?;
            return Ok(false);
        }
        copy_result.map_err(to_api_error)?;

        let copy_result = self
            .staging_eav_db
            .copy_all(&staging_reader, &self.eav_db, &mut writer);

        if is_store_full_result(&copy_result) {
            drop(writer);
            trace!("writer: commit_internal store full while adding eav data");
            let map_size = env_lock.info().map_err(to_api_error)?.map_size();
            env_lock
                .set_map_size(map_size * map_growth_factor())
                .map_err(to_api_error)?;
            return Ok(false);
        }

        drop(staging_reader);
        drop(staging_env_lock);
        writer
            .commit()
            .map(|()| {
                trace!("writer: commit_internal success");
                Ok(true)
            })
            .unwrap_or_else(|e| {
                trace!("writer: commit_internal error on commit");
                if is_store_full_error(&e) {
                    trace!("writer: commit_internal store full on commit");
                    let map_size = env_lock.info().map_err(to_api_error)?.map_size();
                    env_lock
                        .set_map_size(map_size * map_growth_factor())
                        .map_err(to_api_error)?;
                    Ok(false)
                } else {
                    trace!("writer: commit_internal generic error on commit");
                    Err(to_api_error(e))
                }
            })
    }
}

impl<A: Attribute + Sync + Send + DeserializeOwned> holochain_persistence_api::txn::Writer
    for LmdbCursor<A>
{
    fn commit(self) -> PersistenceResult<()> {
        loop {
            let committed = self.commit_internal()?;
            if committed {
                return Ok(());
            }
        }
    }
}

impl<A: Attribute> ReportStorage for LmdbCursor<A> {
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        self.cas_db.get_storage_report()
    }
}

impl<A: Attribute> LmdbCursor<A> {
    pub fn new(
        cas_db: LmdbStorage,
        eav_db: EavLmdbStorage<A>,
        staging_cas_db: LmdbStorage,
        staging_eav_db: EavLmdbStorage<A>,
    ) -> Self {
        Self {
            cas_db,
            eav_db,
            staging_cas_db,
            staging_eav_db,
        }
    }
}

impl<A: Attribute> AddContent for LmdbCursor<A> {
    /// Adds `content` only to the staging CAS database. Use `commit()` to write to the
    /// primary.
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        self.staging_cas_db.add(content)
    }
}

impl<A: Attribute> FetchContent for LmdbCursor<A> {
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        self.fetch(address)
            .map(|maybe_content| maybe_content.is_some())
    }

    /// First try the staging CAS database, then the primary. Cache the results from the
    /// primary into the staging database.
    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let maybe_content = self.staging_cas_db.fetch(address)?;

        if maybe_content.is_some() {
            trace!(
                "maybe_content_is_some for address {:?} in staging: {:?},",
                address,
                maybe_content
            );
            return Ok(maybe_content);
        }

        let maybe_content = self.cas_db.fetch(address)?;
        trace!(
            "maybe_content in primary for {:?}: {:?}",
            address,
            maybe_content
        );
        if let Some(content) = maybe_content {
            self.staging_cas_db.add(&content)?;
            Ok(Some(content))
        } else {
            Ok(None)
        }
    }
}

impl<A: Attribute> HasUuid for LmdbCursor<A> {
    fn get_id(&self) -> uuid::Uuid {
        self.cas_db.get_id()
    }
}

impl<A: Attribute + serde::de::DeserializeOwned> AddEavi<A> for LmdbCursor<A> {
    /// Adds `content` only to the staging EAVI database. Use `commit()` to write to the
    /// primary.
    fn add_eavi(
        &self,
        eav: &EntityAttributeValueIndex<A>,
    ) -> PersistenceResult<Option<EntityAttributeValueIndex<A>>> {
        self.staging_eav_db
            .resizable_add_lmdb_eavi(eav)
            .map_err(to_api_error)
    }
}

impl<A: Attribute + serde::de::DeserializeOwned> FetchEavi<A> for LmdbCursor<A> {
    /// First query the staging EAVI database, then the primary. Cache the results from the
    /// primary into the staging database.
    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        let eavis = self.staging_eav_db.fetch_eavi(query)?;

        if !eavis.is_empty() {
            return Ok(eavis);
        }

        let eavis = self.eav_db.fetch_eavi(query)?;

        for eavi in &eavis {
            self.staging_eav_db.add_eavi(eavi)?;
        }
        Ok(eavis)
    }
}

impl<A: Attribute + DeserializeOwned> Cursor<A> for LmdbCursor<A> {}

#[derive(Clone, Debug)]
pub struct LmdbCursorProvider<A: Attribute> {
    /// Primary CAS lmdb store
    cas_db: LmdbStorage,

    /// Primary EAV lmdb store, transactionally linked to the CAS
    eav_db: EavLmdbStorage<A>,

    /// Path prefix to generate staging databases
    staging_path_prefix: PathBuf,

    /// Initial map size of staging databases
    staging_initial_map_size: Option<usize>,

    /// Environment flags for staging databases.
    staging_env_flags: Option<EnvironmentFlags>,
}

pub struct LmdbCrossTxnCursorProvider {
    /// Path prefix to generate staging databases
    staging_path_prefix: PathBuf,

    /// Initial map size of staging databases
    staging_initial_map_size: Option<usize>,

    /// Environment flags for staging databases.
    staging_env_flags: Option<EnvironmentFlags>,

    managers: UniversalMap<String>,
}

impl CrossTxnCursorProvider for LmdbCrossTxnCursorProvider {
    fn create_cursor_rw(&self) -> PersistenceResult<Box<dyn CursorRwDyn<A>>> {}

    fn create_cursor(&self) -> PersistenceResult<Box<dyn Cursor<A>>> {}
}

pub struct LmdbCrossTxnCursor {
    cursors: UniversalMap<String>,
}

impl CrossTxnCursor for LmdbCrossTxnCursor {
    fn cursor_rw(&self, key: &CursorRwKey<A>) -> PersistenceResult<Box<dyn CursorRw<A>>> {
        self.cursors
            .get(key)
            .map(|x| Ok(x))
            .unwrap_or_else(|| Err(format!("Database {:?} does not exist", key)))
    }
}

impl LmdbCrossTxnCursor {
    fn add_database(&self, key: &CursorRwKey<A>, cursor: Box<dyn CursorRw<A>>) -> Self {
        self.cursors.insert(key, cursor);
        self
    }
}

/// Name of CAS staging database
const STAGING_CAS_BUCKET: &str = "staging_cas";

/// Name of EAV staging database
const STAGING_EAV_BUCKET: &str = "staging_eav";

impl<A: Attribute + DeserializeOwned> CursorProvider<A> for LmdbCursorProvider<A> {
    type Cursor = LmdbCursor<A>;
    type CursorRw = LmdbCursor<A>;
    fn create_cursor_rw(&self) -> PersistenceResult<Self::CursorRw> {
        let db_names = vec![STAGING_CAS_BUCKET, STAGING_EAV_BUCKET];

        let mut staging_path = self.staging_path_prefix.clone();
        staging_path.push(format!("{}", Uuid::new_v4()));

        // TODO do we need this if the environment flags are set correctly? That is, it should just
        // be an in memory only database with no file system handles?
        fs::create_dir_all(staging_path.as_path())?;

        // This avoids using the singleton rkv manager which caches all environments for all
        // eternity. As these are randomly pathed staging databases, there is no need for a
        // singleton to ensure only one environment is created per path.
        let use_rkv_manager = false;

        let staging_dbs = LmdbInstance::new_all(
            db_names.as_slice(),
            staging_path,
            self.staging_initial_map_size,
            self.staging_env_flags,
            use_rkv_manager.into(),
        );

        let staging_cas_db = LmdbStorage::wrap(
            staging_dbs
                .get(&STAGING_CAS_BUCKET.to_string())
                .unwrap()
                .clone(),
        );
        let staging_eav_db = EavLmdbStorage::wrap(
            staging_dbs
                .get(&STAGING_EAV_BUCKET.to_string())
                .unwrap()
                .clone(),
        );

        Ok(LmdbCursor::new(
            self.cas_db.clone(),
            self.eav_db.clone(),
            staging_cas_db,
            staging_eav_db,
        ))
    }

    fn create_cursor(&self) -> PersistenceResult<Self::Cursor> {
        self.create_cursor_rw()
    }
}

pub type LmdbManager<A> =
    DefaultPersistenceManager<A, LmdbStorage, EavLmdbStorage<A>, LmdbCursorProvider<A>>;

pub fn new_manager<A: Attribute + DeserializeOwned, EP: AsRef<Path> + Clone, SP: AsRef<Path>>(
    env_path: EP,
    staging_path_prefix: Option<SP>,
    initial_map_size: Option<usize>,
    env_flags: Option<EnvironmentFlags>,
    staging_initial_map_size: Option<usize>,
    staging_env_flags: Option<EnvironmentFlags>,
) -> LmdbManager<A> {
    let cas_db_name = crate::cas::lmdb::CAS_BUCKET;
    let eav_db_name = crate::eav::lmdb::EAV_BUCKET;
    let db_names = vec![cas_db_name, eav_db_name];

    // Ensure exactly one enviroment for the primary database by taking
    // advantage of the `rkv::Manager`.
    let use_rkv_manager = true;

    let dbs = LmdbInstance::new_all(
        db_names.as_slice(),
        env_path,
        initial_map_size,
        env_flags,
        use_rkv_manager.into(),
    );

    let cas_db = LmdbStorage::wrap(dbs.get(&cas_db_name.to_string()).unwrap().clone());
    let eav_db: EavLmdbStorage<A> =
        EavLmdbStorage::wrap(dbs.get(&eav_db_name.to_string()).unwrap().clone());

    let staging_path_prefix = staging_path_prefix
        .map(|p| p.as_ref().to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir());

    let cursor_provider = LmdbCursorProvider {
        cas_db: cas_db.clone(),
        eav_db: eav_db.clone(),
        staging_path_prefix,
        staging_initial_map_size,
        staging_env_flags,
    };

    DefaultPersistenceManager::new(cas_db, eav_db, cursor_provider)
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::{
        cas::{
            content::{AddressableContent, ExampleAddressableContent},
            storage::ExampleLink,
        },
        eav::{AddEavi, Attribute, EntityAttributeValueIndex, ExampleAttribute},
        txn::*,
    };
    use tempfile::tempdir;

    use super::LmdbManager;

    fn enable_logging_for_test(enable: bool) {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "trace");
        }

        let _ = env_logger::builder()
            .default_format_timestamp(true)
            .default_format_module_path(true)
            .is_test(enable)
            .try_init();
    }

    fn new_test_manager<A: Attribute + serde::de::DeserializeOwned>() -> super::LmdbManager<A> {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let staging_path: Option<String> = None;
        super::new_manager(
            temp_path,
            staging_path,
            Some(1024 * 1024),
            None,
            Some(1024 * 1024),
            None,
        )
    }

    #[test]
    fn txn_lmdb_cas_round_trip() {
        enable_logging_for_test(true);
        let entity_content = RawString::from("foo").into();
        let other_content = RawString::from("blue").into();

        let manager: LmdbManager<ExampleAttribute> = new_test_manager();
        let tombstone_manager: LmdbManager<ExampleLink> = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.cas_round_trip_test::<ExampleAddressableContent, ExampleAddressableContent>(
            entity_content,
            other_content,
        )
    }

    #[test]
    fn txn_lmdb_eav_round_trip() {
        let entity_content =
            ExampleAddressableContent::try_from_content(&RawString::from("foo").into()).unwrap();
        let attribute = ExampleAttribute::WithPayload("favourite-color".to_string());
        let value_content =
            ExampleAddressableContent::try_from_content(&RawString::from("blue").into()).unwrap();

        let manager = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.eav_test_round_trip(entity_content, attribute, value_content)
    }

    #[test]
    fn txn_lmdb_eav_one_to_many() {
        let manager = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.eav_test_one_to_many::<ExampleAddressableContent>(&ExampleAttribute::default());
    }

    #[test]
    fn txn_lmdb_eav_many_to_one() {
        let manager = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.eav_test_many_to_one::<ExampleAddressableContent>(&ExampleAttribute::default());
    }

    #[test]
    fn txn_lmdb_eav_range() {
        let manager = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.eav_test_range::<ExampleAddressableContent>(&ExampleAttribute::default());
    }

    #[test]
    fn txn_lmdb_eav_prefixes() {
        let manager = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.eav_test_multiple_attributes::<ExampleAddressableContent>(
            vec!["a_", "b_", "c_", "d_"]
                .into_iter()
                .map(|p| ExampleAttribute::WithPayload(p.to_string() + "one_to_many"))
                .collect(),
        );
    }

    #[test]
    fn txn_lmdb_can_write_cas_entry_larger_than_map() {
        let manager: LmdbManager<ExampleAttribute> = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);

        let inititial_mmap_size = 1024 * 1024;

        enable_logging_for_test(true);
        test_suite.with_cursor("txn_can_write_cas_entry_larger_than_map", |cursor| {
            // can write a single entry that is much larger than the current mmap
            let data: Vec<u8> = std::iter::repeat(0)
                .take(10 * inititial_mmap_size)
                .collect();

            let example_content: ExampleAddressableContent =
                ExampleAddressableContent::try_from_content(
                    &RawString::from(String::from_utf8_lossy(data.as_slice()).to_string()).into(),
                )
                .unwrap();
            cursor.add(&example_content).unwrap();
        })
    }

    #[ignore = "slow test"]
    #[test]
    fn txn_lmdb_can_write_eav_entry_larger_than_map() {
        let manager: LmdbManager<ExampleAttribute> = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);

        enable_logging_for_test(true);
        test_suite.with_cursor("txn_can_write_eav_entry_larger_than_map", |cursor| {
            for i in 0..10000 {
                trace!("iter [{}]", i);
                // can write a single entry that is much larger than the current mmap
                let data: Vec<u8> = Vec::from(format!("{}", i).as_bytes());

                let data: String =
                    RawString::from(String::from_utf8_lossy(data.as_slice()).to_string()).into();
                let eavi = EntityAttributeValueIndex::new(
                    &holochain_persistence_api::hash::HashString::from(data.clone()),
                    &ExampleAttribute::WithoutPayload,
                    &data.into(),
                )
                .unwrap();
                cursor.add_eavi(&eavi).unwrap();
            }
        })
    }

    #[ignore = "slow test"]
    #[test]
    fn txn_lmdb_can_create_infinite_environments() {
        let manager: LmdbManager<ExampleAttribute> = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);

        enable_logging_for_test(true);
        for i in 0..10000 {
            test_suite.with_cursor("txn_lmdb_can_create_infinite_environments", |cursor| {
                trace!("iter [{}]", i);
                // can write a single entry that is much larger than the current mmap
                let data: Vec<u8> = Vec::from(format!("{}", i).as_bytes());

                let data: String =
                    RawString::from(String::from_utf8_lossy(data.as_slice()).to_string()).into();
                let eavi = EntityAttributeValueIndex::new(
                    &holochain_persistence_api::hash::HashString::from(data.clone()),
                    &ExampleAttribute::WithoutPayload,
                    &data.into(),
                )
                .unwrap();
                cursor.add_eavi(&eavi).unwrap();
            })
        }
    }

    #[ignore = "fails to compile"]
    #[test]
    fn txn_lmdb_tombstone() {
        /* Does not yet compile!
        let manager = new_test_manager();
        let tombstone_manager = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);
        test_suite.eav_test_tombstone::<ExampleAddressableContent>()
        */
    }

    #[test]
    fn txn_lmdb_cas_eav_test_transaction_abort() {
        enable_logging_for_test(true);
        let entity_content = RawString::from("red").into();
        let attribute = ExampleAttribute::WithoutPayload;
        let transient_content = RawString::from("green").into();

        let manager: LmdbManager<ExampleAttribute> = new_test_manager();
        let tombstone_manager: LmdbManager<ExampleLink> = new_test_manager();
        let test_suite = PersistenceManagerTestSuite::new(manager, tombstone_manager);

        test_suite
            .cas_eav_test_transaction_abort::<ExampleAddressableContent, ExampleAddressableContent>(
                entity_content,
                attribute,
                transient_content,
            );
    }

    #[test]
    fn txn_lmdb_manager_can_be_cast_as_dyn() {
        enable_logging_for_test(true);
        let manager: LmdbManager<ExampleAttribute> = new_test_manager();
        let manager_dyn: Box<dyn PersistenceManagerDyn<_>> = Box::new(manager);
        let data: Vec<u8> = Vec::from(format!("{}", "abc").as_bytes());

        let entity_content: Content = RawString::from("red").into();
        let eavi = EntityAttributeValueIndex::new(
            &holochain_persistence_api::hash::HashString::from(data.clone()),
            &ExampleAttribute::WithoutPayload,
            &data.into(),
        )
        .unwrap();

        let cursor = manager_dyn.create_cursor_rw().unwrap();
        assert!(cursor.add(&entity_content).is_ok());
        assert!(cursor.add_eavi(&eavi).is_ok());
        assert!(cursor.commit().is_ok());
    }
}
