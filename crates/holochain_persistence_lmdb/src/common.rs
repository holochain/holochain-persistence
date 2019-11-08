use holochain_logging::prelude::*;
use lmdb::Error as LmdbError;
use rkv::{
    DatabaseFlags, EnvironmentFlags, Manager, Rkv, SingleStore, StoreError, StoreOptions, Value,
};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};

const DEFAULT_INITIAL_MAP_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct LmdbInstance {
    pub store: SingleStore,
    pub manager: Arc<RwLock<Rkv>>,
}

impl LmdbInstance {
    pub fn new<P: AsRef<Path> + Clone>(
        db_name: &str,
        path: P,
        initial_map_bytes: Option<usize>,
    ) -> LmdbInstance {
        let db_path = path.as_ref().join(db_name).with_extension("db");
        std::fs::create_dir_all(db_path.clone()).expect("Could not create file path for store");

        let manager = Manager::singleton()
            .write()
            .unwrap()
            .get_or_create(db_path.as_path(), |path: &Path| {
                let mut env_builder = Rkv::environment_builder();
                env_builder
                    // max size of memory map, can be changed later
                    .set_map_size(initial_map_bytes.unwrap_or(DEFAULT_INITIAL_MAP_BYTES))
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
            .open_single(db_name, options)
            .expect("Could not create store");

        LmdbInstance {
            store: store,
            manager: manager.clone(),
        }
    }

    pub fn add<K: AsRef<[u8]> + Clone>(&self, key: K, value: &Value) -> Result<(), StoreError> {
        let env = self.manager.read().unwrap();
        let mut writer = env.write()?;

        match self
            .store
            .put(&mut writer, key.clone(), value)
            .and_then(|_| writer.commit())
        {
            Err(StoreError::LmdbError(LmdbError::MapFull)) => {
                trace!("Insufficient space in MMAP, doubling and trying again");
                let map_size = env.info()?.map_size();
                env.set_map_size(map_size * 2)?;
                self.add(key.clone(), value)
            }
            r => r, // preserve any other errors
        }?;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn info(&self) -> Result<rkv::Info, StoreError> {
        self.manager.read().unwrap().info()
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
            lmdb.add(
                content.address(),
                &Value::Json(&content.content().to_string()),
            )
            .unwrap();
        }

        assert_eq!(lmdb.info().unwrap().map_size(), inititial_mmap_size * 2,);

        // Do it again for good measure
        while lmdb.info().unwrap().map_size() == 2 * inititial_mmap_size {
            let content = CasBencher::random_addressable_content();
            lmdb.add(
                content.address(),
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

        lmdb.add("a", &Value::Json(&String::from_utf8(data).unwrap()))
            .expect("could not write to lmdb");
    }
}
