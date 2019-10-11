use rkv::{
    DatabaseFlags, EnvironmentFlags, Manager, Rkv, SingleStore, StoreOptions,
};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};

const DEFAULT_INITIAL_MAP_BYTES: usize = 104857600;

#[derive(Clone)]
pub(crate) struct LmdbInstance {
	pub store: SingleStore,
	pub manager: Arc<RwLock<Rkv>>
}

impl LmdbInstance {
	pub fn new<P: AsRef<Path> + Clone>(db_name: &str, path: P, initial_map_bytes: Option<usize>) -> LmdbInstance {
		let db_path = path.as_ref().join(db_name).with_extension("db");
        std::fs::create_dir_all(db_path.clone())
            .expect("Could not create file path for store");

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
}