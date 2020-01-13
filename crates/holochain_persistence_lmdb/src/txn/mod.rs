use holochain_persistence_api::cas::{content::*, storage::*};
use holochain_persistence_api::eav::*;
//use holochain_persistence_api::txn::*;
use holochain_persistence_api::error::*;
use holochain_persistence_api::reporting::{ReportStorage, StorageReport};
use crate::common::LmdbInstance;
use crate::cas::lmdb::LmdbStorage;
use uuid::Uuid;
use lazycell::LazyCell;
use rkv::{Reader, Rkv, Writer};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};


#[allow(dead_code)]
#[derive(Clone)]
enum EnvLock<'a> {
    Write(Arc<RwLockWriteGuard<'a, Rkv>>, Arc<RwLock<Rkv>>),
    Read(Arc<RwLockReadGuard<'a, Rkv>>, Arc<RwLock<Rkv>>),
}

#[allow(dead_code)]
impl<'a> EnvLock<'a> {
    pub fn new(rkv: Arc<RwLock<Rkv>>) -> EnvLock<'a> {
        Self::Read(Arc::new(rkv.read().unwrap()), rkv)
    }

    pub fn promote(&mut self) -> &mut Self {
        match *self {
            Self::Read(read, rkv) => {
                let write = rkv.write().unwrap();
                drop(read);
                *self = Self::Write(Arc::new(write), rkv.clone());
            }
            Self::Write(_, _) => {}
        }
        self
    }

    pub fn read(&self) -> Arc<RwLockReadGuard<'a, Rkv>> {
        match self {
            Self::Read(read, _rkv) => read.clone(),
            Self::Write(_write, _rkv) => panic!("bad"),
        }
    }


    pub fn write(&mut self) -> Arc<RwLockWriteGuard<'a, Rkv>> {
        match self {
            Self::Read(_read, _rkv) => self.promote().write(),
            Self::Write(write, _) => write.clone()
        }
    }
}


#[allow(dead_code)]
#[derive(Clone)]
pub struct EagerEnvCursor<'env, A:Attribute> {
    env_reader: Arc<Reader<'env>>,
    env_lock: Arc<RwLockReadGuard<'env, Rkv>>,
    staging_env_lock: Arc<RwLockReadGuard<'env, Rkv>>,
    staging_env_reader: Arc<Reader<'env>>,
    staging_env_writer: Arc<Writer<'env>>,
    cas_db: LmdbStorage,
    eav_db: LmdbStorage,
    staging_cas_db: LmdbStorage,
    staging_eav_db: LmdbStorage,
    phantom: std::marker::PhantomData<A>
}

unsafe impl<'env, A:Attribute> Sync for EagerEnvCursor<'env, A>  {}
unsafe impl<'env, A:Attribute> Send for EagerEnvCursor<'env, A>  {}

impl<'env, A:Attribute> holochain_persistence_api::txn::Writer for EagerEnvCursor<'env, A> {


    fn commit(&self) -> PersistenceResult<()> {
        panic!("NYI")     
    }

    fn abort(&self) -> PersistenceResult<()> {
        self.staging_env_writer.abort();
        self.staging_env_writer.abort();
        self.env_reader.abort(); 
        Ok(())
    }
}

impl<'env, A:Attribute> std::fmt::Debug for EagerEnvCursor<'env, A> {
    
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}, {:?}, {:?}, {:?}",
            self.cas_db,
            self.eav_db,
            self.staging_cas_db,
            self.staging_eav_db)
    }
        
}

impl<'env, A:Attribute> ReportStorage for EagerEnvCursor<'env, A> {
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        Ok(StorageReport::new(0)) // TODO: implement this
    }
}


impl<'env, A:Attribute> EagerEnvCursor<'env, A> {

    pub fn new(cas_db : LmdbStorage, eav_db: LmdbStorage,
        staging_cas_db: LmdbStorage, staging_eav_db: LmdbStorage) -> Self {
  
        let env_lock = cas_db.lmdb.rkv.read().unwrap();
        let env_reader = env_lock.read().unwrap();
        let staging_env_lock = cas_db.lmdb.rkv.read().unwrap();
        let staging_env_reader = env_lock.read().unwrap();
        let staging_env_writer = env_lock.write().unwrap();
         Self {
            env_reader,
            env_lock,
            cas_db,
            eav_db,
            staging_env_reader,
            staging_env_lock,
            staging_env_writer,
            staging_cas_db,
            staging_eav_db,
            phantom: std::marker::PhantomData
        }
    }

}

impl<'env, A:Attribute> ContentAddressableStorage for EagerEnvCursor<'env, A> {

    fn add(&mut self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        self.staging_cas_db.lmdb_add(&mut self.staging_env_writer.clone(), content).
            map_err(|e| crate::error::PersistenceError(e))?
    }
    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
    
    }
    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        let result = self.staging_cas_db.lmdb_fetch(&self.staging_env_reader.clone(), address);
         
    }
    fn get_id(&self) -> Uuid {
        self.cas_db.id 
    }

}

/*
impl<'env, A:Attribute> Cursor<A> for EagerEnvCursor<'env,A> {
     
}
*/
#[derive(Clone)]
pub struct EagerCursorProvider {
    cas_db: LmdbInstance,
    eav_db: LmdbInstance
}
/*
impl<'env, A:Attribute> CursorProvider<A> for EagerCursorProvider {
 
    type Cursor = EagerEnvCursor<'env>;
    fn create_cursor(&'env self) -> Self::Cursor {
        EagerEnvCursor::new(self.cas_db.clone(), self.eav_db.clone())
    }
}
*/
/*
impl<'env, A:Attribute> CursorProvider<A> for EagerCursorProvider {
    
    type Cursor = EagerEnvCursor<'env>;
    fn create_cursor<'a>(&self) -> EagerEnvCursor<'a> {
        EagerEnvCursor::new(self.cas_db.clone(), self.eav_db.clone())
    }
}
*/
#[allow(dead_code)]
struct LazyEnvCursor<'env> {
    env_reader: LazyCell<Reader<'env>>,
    env_lock: LazyCell<EnvLock<'env>>,
    env_writer: LazyCell<Writer<'env>>,
    cas_db: LmdbInstance,
    eav_db: LmdbInstance,
}

#[allow(dead_code)]
impl<'env> LazyEnvCursor<'env> {
    fn env_lock<'a>(&self) -> &'a EnvLock<'env> {
        let Self {
            env_lock, cas_db, ..
        } = self;

        let ret = env_lock.borrow_with(|| EnvLock::new(cas_db.rkv));
        ret
    }

    pub fn writer(&mut self) -> &Writer<'env> {
        let Self {
            env_writer, env_lock, ..
        } = self;

        let writer = env_writer.borrow_with(|| {
            let mut env_write_lock = env_lock.write();
            let writer: Writer<'env> = env_write_lock.write().unwrap();
            writer
        });
        writer
    }
}

struct LmdbCursor<'prim_env, 'staging_env> {
    primary_cursor: LazyEnvCursor<'prim_env>,
    staging_cursor: LazyEnvCursor<'staging_env>,
}

impl<'prim_env, 'staging_env> LmdbCursor<'prim_env, 'staging_env> {
    fn primary_db_writer(&self) -> Writer<'prim_env> {
        panic!("NYI")
        /*
        let Self { primary_cursor, .. } = self;

        let ret = primary_cursor.env_writer.borrow_with(|| {
            let rkv = primary_cursor.cas_db.rkv.write().unwrap();
            let writer : Writer<'prim_env> = rkv.write().unwrap();
            writer
        });

        *ret*/
    }
}
/*

impl<'prim_env, 'staging_env> Cursor for LmdbCursor<'prim_env, 'staging_env> {

}


pub struct LmdbCursorProvider {
    primary_rkv: rkv::Rkv,
    staging_env_path_prefix: Path,
}

impl CursorProvider for LmdbCursorProvider {
    type Cursor = LmdbCursor;

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
