//use holochain_logging::prelude::*;
//use lmdb::Error as LmdbError;
use crate::error::{is_store_full_error, is_store_full_result};
use holochain_persistence_api::{
    cas::content::{Address, Content},
    hash::HashString,
};
use rkv::{
    DatabaseFlags, EnvironmentFlags, Manager, Reader, Rkv, SingleStore, StoreError, StoreOptions,
    Value, Writer,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

const DEFAULT_INITIAL_MAP_SIZE: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub struct LmdbInstance {
    store: SingleStore,
    /// A lock protected reference to the environment. This lock
    /// is a consequence of using the `rkv::Manager` api which returns
    /// guarded environments to protect users from creating multiple environments
    /// in the same process and path.
    rkv: Arc<RwLock<Rkv>>,
}

impl LmdbInstance {
    pub fn new<P: AsRef<Path> + Clone>(
        db_name: &str,
        path: P,
        initial_map_bytes: Option<usize>,
        flags: Option<EnvironmentFlags>,
    ) -> LmdbInstance {
        Self::new_all(&[db_name], path, initial_map_bytes, flags, None)
            .into_iter()
            .next()
            .map(|(_db_name, instance)| instance)
            .expect("Expected exactly one database instance")
    }

    /// Instantiates multiple lmdb database instances for a set of `db_names` at `path` and
    /// initial map size `initial_map_size`. Transactions will be synchronized over all of them.
    ///
    /// If `use_rkv_manager` is set to the default of `true`, the provided `rkv::Manager` singleton
    /// will ensure only one environment is created for the process per given `path`. If this is
    /// already enforced outside this api then set it to `false`.
    pub fn new_all<P: AsRef<Path> + Clone>(
        db_names: &[&str],
        path: P,
        initial_map_size: Option<usize>,
        flags: Option<EnvironmentFlags>,
        use_rkv_manager: Option<bool>,
    ) -> HashMap<String, LmdbInstance> {
        std::fs::create_dir_all(path.clone()).expect("Could not create file path for store");

        let rkv =
            if use_rkv_manager.unwrap_or_else(|| true) {
                Manager::singleton()
                    .write()
                    .unwrap()
                    .get_or_create(path.as_ref(), |path: &Path| {
                        let mut env_builder = Rkv::environment_builder();
                        env_builder
                            // max size of memory map, can be changed later
                            .set_map_size(initial_map_size.unwrap_or(DEFAULT_INITIAL_MAP_SIZE))
                            // max number of DBs in this environment
                            .set_max_dbs(db_names.len() as u32)
                            // These flags make writes waaaaay faster by async writing to disk rather than blocking
                            // There is some loss of data integrity guarantees that comes with this
                            .set_flags(flags.unwrap_or_else(|| {
                                EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC
                            }));
                        Rkv::from_env(path, env_builder)
                    })
                    .expect("Could not create the environment")
            } else {
                let mut env_builder = Rkv::environment_builder();
                env_builder
                    // max size of memory map, can be changed later
                    .set_map_size(initial_map_size.unwrap_or(DEFAULT_INITIAL_MAP_SIZE))
                    // max number of DBs in this environment
                    .set_max_dbs(db_names.len() as u32)
                    // These flags make writes waaaaay faster by async writing to disk rather than blocking
                    // There is some loss of data integrity guarantees that comes with this
                    .set_flags(flags.unwrap_or_else(|| {
                        EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC
                    }));
                Arc::new(RwLock::new(
                    Rkv::from_env(path.as_ref(), env_builder).unwrap(),
                ))
            };

        db_names
            .iter()
            .map(|db_name| {
                (
                    (*db_name).to_string(),
                    Self::create_database(db_name, rkv.clone()),
                )
            })
            .collect::<HashMap<_, _>>()
    }

    fn create_database(db_name: &str, rkv: Arc<RwLock<Rkv>>) -> LmdbInstance {
        let env = rkv
            .read()
            .expect("Could not get a read lock on the manager");

        // Then you can use the environment handle to get a handle to a datastore:
        let options = StoreOptions {
            create: true,
            flags: DatabaseFlags::empty(),
        };
        let store: SingleStore = env
            .open_single(db_name, options)
            .expect("Could not create store");

        LmdbInstance {
            store,
            rkv: rkv.clone(),
        }
    }

    /// Puts `value` into the store mapped by `key`.
    /// Fails with a `StoreError` if the memory map is already full.
    pub fn add<'env, K: AsRef<[u8]> + Clone>(
        &self,
        mut writer: &mut Writer<'env>,
        key: K,
        value: &Value,
    ) -> Result<(), StoreError> {
        self.store.put(&mut writer, key, value)
    }

    /// Puts `value` into the store mapped by `key`.
    /// Will automatically resize the memmory map if the map ends up full.
    pub fn resizable_add<K: AsRef<[u8]> + Clone>(
        &self,
        key: &K,
        value: &Value,
    ) -> Result<(), StoreError> {
        loop {
            let env_lock = self.rkv.write().unwrap();
            let mut writer = env_lock.write()?;
            let result = self.add(&mut writer, key, value);

            if is_store_full_result(&result) {
                drop(writer);
                let map_size = env_lock.info()?.map_size();
                env_lock.set_map_size(map_size * map_growth_factor())?;
                continue;
            }

            let result = writer.commit();

            if let Err(e) = &result {
                if is_store_full_error(e) {
                    let map_size = env_lock.info()?.map_size();
                    env_lock.set_map_size(map_size * map_growth_factor())?;
                    continue;
                }
            }
            return result;
        }
    }

    #[allow(dead_code)]
    pub fn info(&self) -> Result<rkv::Info, StoreError> {
        self.rkv.read().unwrap().info()
    }

    pub fn rkv(&self) -> &Arc<RwLock<rkv::Rkv>> {
        &self.rkv
    }

    pub fn store(&self) -> &SingleStore {
        &self.store
    }

    pub fn copy_all<'env, 'env2>(
        &self,
        source: &Reader<'env>,
        target: &Self,
        mut writer: &mut Writer<'env2>,
    ) -> Result<(), StoreError> {
        self.store().iter_start(source)?.try_fold((), |(), result| {
            if let Ok((address, Some(data))) = result {
                target.store().put(&mut writer, address, &data).map(|_| ())
            } else {
                result.map(|_| ())
            }
        })
    }
}

pub fn handle_cursor_tuple_result(
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

const MAP_GROWTH_FACTOR: usize = 8;

pub fn map_growth_factor() -> usize {
    MAP_GROWTH_FACTOR
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use holochain_persistence_api::cas::{content::AddressableContent, storage::CasBencher};
    use tempfile::tempdir;

    #[test]
    fn can_grow_map_on_write() {
        // make a db with a 1MB MMAP. This seems to be the lowest you an go (probably OS dependent)
        let inititial_mmap_size = 1024 * 1024;
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        let lmdb = LmdbInstance::new(
            "can_grow_map_on_write",
            dir.path(),
            Some(inititial_mmap_size),
            Default::default(),
        );

        // put data in there until the mmap size changes
        while lmdb.info().unwrap().map_size() == inititial_mmap_size {
            let content = CasBencher::random_addressable_content();
            lmdb.resizable_add(
                &content.address(),
                &Value::Json(&content.content().to_string()),
            )
            .unwrap();
        }

        assert_eq!(
            lmdb.info().unwrap().map_size(),
            inititial_mmap_size * map_growth_factor()
        );

        // Do it again for good measure
        while lmdb.info().unwrap().map_size() == map_growth_factor() * inititial_mmap_size {
            let content = CasBencher::random_addressable_content();
            lmdb.resizable_add(
                &content.address(),
                &Value::Json(&content.content().to_string()),
            )
            .unwrap();
        }

        assert_eq!(
            lmdb.info().unwrap().map_size(),
            inititial_mmap_size * map_growth_factor() * map_growth_factor(),
        );
    }

    #[test]
    fn can_write_entry_larger_than_map() {
        // can write a single entry that is much larger than the current mmap
        let inititial_mmap_size = 1024 * 1024;
        let dir = tempdir().expect("Could not create a tempdir for CAS testing");
        let lmdb = LmdbInstance::new(
            "can_grow_map_on_write",
            dir.path(),
            Some(inititial_mmap_size),
            Default::default(),
        );

        let data: Vec<u8> = std::iter::repeat(0)
            .take(10 * inititial_mmap_size)
            .collect();

        lmdb.resizable_add(&"a", &Value::Json(&String::from_utf8(data).unwrap()))
            .expect("could not write to lmdb");
    }
}
