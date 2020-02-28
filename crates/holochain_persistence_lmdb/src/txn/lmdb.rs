use crate::{
    cas::lmdb::LmdbStorage,
    common::{map_growth_factor, LmdbEnv, LmdbInstance},
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
    iter::IntoIterator,
    path::{Path, PathBuf},
};

use std::sync::Arc;

use uuid::Uuid;
/// A cursor over an lmdb environment
#[derive(Clone, Debug)]
pub struct LmdbCursor<A: Attribute> {
    cas_db: LmdbStorage,
    eav_db: EavLmdbStorage<A>,
    staging_cas_db: LmdbStorage,
    staging_eav_db: EavLmdbStorage<A>,
}

impl<A: Attribute> IntoIterator for LmdbCursor<A> {
    type Item = (LmdbInstance, LmdbInstance);
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        let dbs = vec![
            (self.staging_cas_db.lmdb, self.cas_db.lmdb),
            (self.staging_eav_db.lmdb, self.eav_db.lmdb),
        ];
        dbs.into_iter()
    }
}

impl<A: Attribute> Into<Vec<(LmdbInstance, LmdbInstance)>> for LmdbCursor<A> {
    fn into(self) -> Vec<(LmdbInstance, LmdbInstance)> {
        let x = self.into_iter();

        let mut dbs = Vec::new();
        for v in x {
            dbs.push(v);
        }
        dbs
    }
}

trait LmdbScratchSpace {
    fn dbs(self) -> Vec<(LmdbInstance, LmdbInstance)>;
}

impl<A: Attribute> LmdbScratchSpace for LmdbCursor<A> {
    fn dbs(self) -> Vec<(LmdbInstance, LmdbInstance)> {
        self.into()
    }
}

/// Internal commit function which extracts `StoreError::MapFull` into the success value of
/// a result where `true` indicates the commit is successful, and `false` means the map was
/// full and retry is required with the newly allocated map size.
fn commit_internal(
    // [(staging, primary)]
    dbs: Vec<(LmdbInstance, LmdbInstance)>,
) -> PersistenceResult<bool> {
    let opt = dbs.iter().next();

    if opt.is_none() {
        return Ok(true);
    }

    let (staging_rkv, prim_rkv) = opt
        .map(|(staging, prim)| (staging.rkv(), prim.rkv()))
        .unwrap();

    trace!("writer: commit_internal start");
    let staging_env_lock = staging_rkv.read().unwrap();
    trace!("writer: commit_internal got staging env lock");
    let staging_reader = staging_env_lock.read().map_err(to_api_error)?;
    trace!("writer: commit_internal got staging reader");

    let env_lock = prim_rkv.write().unwrap();
    trace!("writer: commit_internal got env write lock");
    let mut writer = env_lock.write().unwrap();
    trace!("writer: commit_internal got writer");

    for (primary_db, staging_db) in &dbs {
        let copy_result = staging_db.copy_all(&staging_reader, &primary_db, &mut writer);

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

impl<A: Attribute + Sync + Send + DeserializeOwned> holochain_persistence_api::txn::Writer
    for LmdbCursor<A>
{
    fn commit(self) -> PersistenceResult<()> {
        let dbs: Vec<(LmdbInstance, LmdbInstance)> = self.into();

        loop {
            let committed = commit_internal(dbs.clone())?;
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

pub struct LmdbEnvironment {
    /// Path prefix to generate staging databases
    staging_path_prefix: PathBuf,

    /// Initial map size of staging databases
    staging_initial_map_size: Option<usize>,

    /// Environment flags for staging databases.
    staging_env_flags: Option<EnvironmentFlags>,
    env: LmdbEnv,
    cursor_providers: UniversalMap<String>,
}

impl LmdbEnvironment {
    pub fn new<EP: AsRef<Path> + Clone, SP: AsRef<Path>>(
        env_path: EP,
        max_dbs: usize,
        staging_path_prefix: Option<SP>,
        initial_map_size: Option<usize>,
        env_flags: Option<EnvironmentFlags>,
        staging_initial_map_size: Option<usize>,
        staging_env_flags: Option<EnvironmentFlags>,
    ) -> Self {
        let env = LmdbEnv::new(env_path, max_dbs, initial_map_size, env_flags, None);
        let staging_path_prefix = staging_path_prefix
            .map(|p| p.as_ref().to_path_buf())
            .unwrap_or_else(|| std::env::temp_dir());

        Self {
            env,
            cursor_providers: UniversalMap::default(),
            staging_env_flags,
            staging_path_prefix,
            staging_initial_map_size,
        }
    }

    pub fn add_database<A: Attribute + 'static>(
        &mut self,
        database_prefix: &str,
    ) -> CursorRwKey<A> {
        let Self {
            staging_path_prefix,
            staging_initial_map_size,
            staging_env_flags,
            env,
            ..
        } = self;

        let cas_db_name = format!("{}.cas", database_prefix);
        let eav_db_name = format!("{}.eav", database_prefix);
        let cas_db = env.open_database(cas_db_name.as_str());
        let eav_db = env.open_database(eav_db_name.as_str());

        let cursor_provider: LmdbCursorProvider<A> = LmdbCursorProvider {
            cas_db: LmdbStorage::wrap(cas_db),
            eav_db: EavLmdbStorage::wrap(eav_db),
            staging_path_prefix: staging_path_prefix.clone(),
            staging_env_flags: *staging_env_flags,
            staging_initial_map_size: *staging_initial_map_size,
        };

        let key = Key::new(database_prefix.to_string());
        self.cursor_providers.insert(key.clone(), cursor_provider);
        let key_ret = key.with_value_type::<Box<dyn CursorRw<A>>>();
        key_ret
    }
}

impl Environment for LmdbEnvironment {
    type EnvCursor = LmdbEnvCursor;
    fn create_cursor(self: Arc<Self>) -> PersistenceResult<Self::EnvCursor> {
        Ok(LmdbEnvCursor::new(self))
    }
}

impl Writer for LmdbEnvCursor {
    fn commit(self) -> PersistenceResult<()> {
        loop {
            let committed = commit_internal(self.dbs.clone())?;
            if committed {
                return Ok(());
            }
        }
    }
}

pub struct LmdbEnvCursor {
    env: Arc<LmdbEnvironment>,
    dbs: Vec<(LmdbInstance, LmdbInstance)>,
    cursors: UniversalMap<String>,
}

impl LmdbEnvCursor {
    fn new(env: Arc<LmdbEnvironment>) -> Self {
        Self {
            env,
            dbs: Vec::new(),
            cursors: UniversalMap::new(),
        }
    }
}

impl EnvCursor for LmdbEnvCursor {
    fn cursor_rw<A: 'static + Attribute + DeserializeOwned>(
        &mut self,
        key: &CursorRwKey<A>,
    ) -> PersistenceResult<Box<dyn CursorRw<A>>> {
        let maybe_cursor = self
            .cursors
            .get(key)
            .map(|x| Ok(x.clone()))
            .unwrap_or_else(|| Err(format!("Cursor {:?} does not exist", key).into()));

        if maybe_cursor.is_ok() {
            return maybe_cursor;
        } else {
            let prov_key: Key<String, LmdbCursorProvider<A>> = Key::with_value_type(key);
            let provider: Option<&LmdbCursorProvider<A>> = self.env.cursor_providers.get(&prov_key);
            if let Some(provider) = provider {
                let cursor = Box::new(CursorProvider::create_cursor(provider)?);
                let mut dbs = cursor.clone().dbs();
                self.dbs.append(&mut dbs);
                let _result = self.cursors.insert(key.clone(), cursor);
                self.cursor_rw(key)
            } else {
                Err(format!("Cursor provider {:?} does not exist", prov_key).into())
            }
        }
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
        CursorProvider::create_cursor_rw(self)
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

    fn new_test_environment<EP: AsRef<Path> + Clone, SP: AsRef<Path>>(
        env_path: EP,
        staging_path_prefix: Option<SP>,
        initial_map_size: Option<usize>,
        env_flags: Option<EnvironmentFlags>,
        staging_initial_map_size: Option<usize>,
        staging_env_flags: Option<EnvironmentFlags>,
    ) -> LmdbEnvironment {
        let max_dbs = 100;
        let env = LmdbEnvironment::new(
            env_path,
            max_dbs,
            staging_path_prefix,
            initial_map_size,
            env_flags,
            staging_initial_map_size,
            staging_env_flags,
        );
        env
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

    #[test]
    fn can_commit_transactions_across_databases() {
        enable_logging_for_test(true);
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let staging_path: Option<String> = None;
        let mut environment: LmdbEnvironment = new_test_environment(
            temp_path,
            staging_path,
            Some(1024 * 1024),
            None,
            Some(1024 * 1024),
            None,
        );

        let key = environment.add_database::<ExampleAttribute>("test_dbs");

        let environment = Arc::new(environment);
        let data: Vec<u8> = Vec::from(format!("{}", "abc").as_bytes());

        let entity_content: Content = RawString::from("red").into();
        let eavi = EntityAttributeValueIndex::new(
            &holochain_persistence_api::hash::HashString::from(data.clone()),
            &ExampleAttribute::WithoutPayload,
            &data.into(),
        )
        .unwrap();

        let mut env_cursor: LmdbEnvCursor = environment.create_cursor().unwrap();
        let cursor_key: Key<_, Box<dyn CursorRw<ExampleAttribute>>> = key.clone().with_value_type();

        let cursor = env_cursor.cursor_rw(&cursor_key).unwrap();
        assert!(cursor.add(&entity_content).is_ok());
        assert!(cursor.add_eavi(&eavi).is_ok());
        assert!(env_cursor.commit().is_ok());
    }
}
