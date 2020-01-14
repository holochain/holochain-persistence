use crate::{cas::lmdb::LmdbStorage, common::LmdbInstance, eav::lmdb::EavLmdbStorage};
use holochain_persistence_api::{
    cas::{content::*, storage::*},
    eav::*,
    error::*,
    reporting::{ReportStorage, StorageReport},
};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct EnvCursor<A: Attribute> {
    cas_db: LmdbStorage,
    eav_db: EavLmdbStorage<A>,
    staging_cas_db: LmdbStorage,
    staging_eav_db: EavLmdbStorage<A>,
    phantom: std::marker::PhantomData<A>,
}

impl<A: Attribute> holochain_persistence_api::txn::Writer for EnvCursor<A> {
    fn commit(&mut self) -> PersistenceResult<()> {
        let env_lock = self.cas_db.lmdb.rkv.write().unwrap();
        let mut writer = env_lock.write().unwrap();

        let staging_env_lock = self.staging_cas_db.lmdb.rkv.read().unwrap();
        let staging_reader = staging_env_lock.read().unwrap();

        let staged = self
            .staging_cas_db
            .lmdb_iter(&staging_reader)
            .map_err(to_api_error)?;

        for (_address, maybe_content) in staged {
            maybe_content
                .as_ref()
                .map(|content| self.cas_db.lmdb_add(&mut writer, content))
                .unwrap_or_else(|| Ok(()))
                .map_err(to_api_error)?;
        }

        writer.commit().map_err(to_api_error)
    }
}

impl<A: Attribute> ReportStorage for EnvCursor<A> {
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        Ok(StorageReport::new(0)) // TODO: implement this
    }
}

impl<A: Attribute> EnvCursor<A> {
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
            phantom: std::marker::PhantomData,
        }
    }
}

fn to_api_error(e: rkv::error::StoreError) -> PersistenceError {
    // Convert to lmdb persistence error
    let e: crate::error::PersistenceError = e.into();

    // Convert into api persistence error
    let e: PersistenceError = e.into();
    e
}

impl<A: Attribute> ContentAddressableStorage for EnvCursor<A> {
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        self.staging_cas_db.add(content)
    }

    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        self.fetch(address)
            .map(|maybe_content| maybe_content.is_some())
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let maybe_content = self.staging_cas_db.fetch(address)?;

        if maybe_content.is_some() {
            return Ok(maybe_content);
        }

        let maybe_content = self.cas_db.fetch(address)?;

        if let Some(content) = maybe_content {
            self.staging_cas_db.add(&content)?;
            Ok(Some(content))
        } else {
            Ok(None)
        }
    }

    fn get_id(&self) -> uuid::Uuid {
        self.cas_db.get_id()
    }
}

impl<A: Attribute + serde::de::DeserializeOwned> EntityAttributeValueStorage<A> for EnvCursor<A> {
    fn add_eavi(
        &self,
        eav: &EntityAttributeValueIndex<A>,
    ) -> PersistenceResult<Option<EntityAttributeValueIndex<A>>> {
        self.staging_eav_db.add_lmdb_eavi(eav).map_err(to_api_error)
    }

    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        let maybe_content = self.staging_eav_db.fetch_eavi(query)?;

        if !maybe_content.is_empty() {
            return Ok(maybe_content);
        }
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct EagerCursorProvider {
    cas_db: LmdbInstance,
    eav_db: LmdbInstance,
}
/*
impl<'env, A:Attribute> CursorProvider<A> for EagerCursorProvider {

    type Cursor = EnvCursor<'env>;
    fn create_cursor(&'env self) -> Self::Cursor {
        EnvCursor::new(self.cas_db.clone(), self.eav_db.clone())
    }
}
*/
/*
impl<'env, A:Attribute> CursorProvider<A> for EagerCursorProvider {

    type Cursor = EnvCursor<'env>;
    fn create_cursor<'a>(&self) -> EnvCursor<'a> {
        EnvCursor::new(self.cas_db.clone(), self.eav_db.clone())
    }
}
*/

/*
pub struct LmdbCursorProvider {
    primary_rkv: rkv::Rkv,
    staging_env_path_prefix: Path,
}

impl<A:Attribute> CursorProvider for LmdbCursorProvider<A> {
    type Cursor = EagerCursor<A>;

    fn create_cursor(&self) -> LmdbCursor {
        let Self {primary_rkv, staging_env_path_prefix } = Self;

    }
}
*/

/*
pub fn new_manager<A:Attribute, P: AsRef<Path> + Clone>(
        db_name: P1,
        eav_db_name: P2,
        cas_initial_map_bytes: Option<usize>,
        eav_initial_map_bytes: Option<usize>,
) -> DefaultCasEavManager<A, LmbdbStorage, LmdbEntityAttributeValueStorage<A>> {
    unimplemented!()
*/
