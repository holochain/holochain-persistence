//use holochain_logging::prelude::*;
//use lmdb::Error as LmdbError;
use crate::error::{is_store_full_error, is_store_full_result};
use rkv::{
    DatabaseFlags, EnvironmentFlags, Manager, Rkv, SingleStore, StoreError, StoreOptions, Value,
    Writer,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

const DEFAULT_INITIAL_MAP_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub struct LmdbInstance {
    pub store: SingleStore,
    pub rkv: Arc<RwLock<Rkv>>,
}

impl LmdbInstance {
    pub fn new<P: AsRef<Path> + Clone>(
        db_name: &str,
        path: P,
        initial_map_bytes: Option<usize>,
    ) -> LmdbInstance {
        Self::new_all(&[db_name], path, initial_map_bytes)
            .into_iter()
            .next()
            .map(|(_db_name, instance)| instance)
            .expect("Expected exactly one database instance")
    }

    /// Instantiates multiple lmdb instances for a set of `db_names` at `path` and initial map size
    /// `initial_map_bytes`
    pub fn new_all<P: AsRef<Path> + Clone>(
        db_names: &[&str],
        path: P,
        initial_map_bytes: Option<usize>,
    ) -> HashMap<String, LmdbInstance> {
        std::fs::create_dir_all(path.clone()).expect("Could not create file path for store");

        let rkv = Manager::singleton()
            .write()
            .unwrap()
            .get_or_create(path.as_ref(), |path: &Path| {
                let mut env_builder = Rkv::environment_builder();
                env_builder
                    // max size of memory map, can be changed later
                    .set_map_size(initial_map_bytes.unwrap_or(DEFAULT_INITIAL_MAP_BYTES))
                    // max number of DBs in this environment
                    .set_max_dbs(db_names.len() as u32)
                    // These flags make writes waaaaay faster by async writing to disk rather than blocking
                    // There is some loss of data integrity guarantees that comes with this
                    .set_flags(EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC);
                Rkv::from_env(path, env_builder)
            })
            .expect("Could not create the environment");

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
            store: store,
            rkv: rkv.clone(),
        }
    }

    pub fn add<'env, K: AsRef<[u8]> + Clone>(
        &self,
        mut writer: &mut Writer<'env>,
        key: K,
        value: &Value,
    ) -> Result<(), StoreError> {
        self.store.put(&mut writer, key, value)
    }

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
                env_lock.set_map_size(map_size * 2)?;
                continue;
            }

            let result = writer.commit();

            if let Err(e) = &result {
                if is_store_full_error(e) {
                    let map_size = env_lock.info()?.map_size();
                    env_lock.set_map_size(map_size * 2)?;
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

        assert_eq!(lmdb.info().unwrap().map_size(), inititial_mmap_size * 2,);

        // Do it again for good measure
        while lmdb.info().unwrap().map_size() == 2 * inititial_mmap_size {
            let content = CasBencher::random_addressable_content();
            lmdb.resizable_add(
                &content.address(),
                &Value::Json(&content.content().to_string()),
            )
            .unwrap();
        }

        assert_eq!(lmdb.info().unwrap().map_size(), inititial_mmap_size * 4,);
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
        );

        let data: Vec<u8> = std::iter::repeat(0)
            .take(10 * inititial_mmap_size)
            .collect();

        lmdb.resizable_add(&"a", &Value::Json(&String::from_utf8(data).unwrap()))
            .expect("could not write to lmdb");
    }
}
