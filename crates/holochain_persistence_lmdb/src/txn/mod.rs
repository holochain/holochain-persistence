use holochain_persistence_api::cas::*;
use holochain_persistence_api::eav::*;
use holochain_persistence_api::txn::*;
use rkv::Writer;
use crate::common::LmdbInstance;


#[derive(Shrinkwrap)]
pub struct LmdbWriter<'txn>(rkv::Writer<'txn>);

impl<'txn, A:Attribute> WriterProvider for dyn CasEavManager<A, LmbdbStorage, LmdbEntityAttributeValueStorage<A>> {
    type Writer = LmdbWriter<'txn>;

    fn create_writer(&self) -> Writer {
        let writer = self.cas().rkv.env().write();
        writer
    }
}

pub fn new_manager<A:Attribute, P1: AsRef<Path> + Clone, P2: AsRef<Path> + Clone>(
        cas_db_path: P1,
        eav_db_path: P2,
        cas_initial_map_bytes: Option<usize>,
        eav_initial_map_bytes: Option<usize>,
) -> DefaultCasEavManager<A, LmbdbStorage, LmdbEntityAttributeValueStorage<A>> {

         let rkv = Manager::singleton()
            .write()
            .unwrap()
            .get_or_create(db_path.as_path(), |path: &Path| {
                let mut env_builder = Rkv::environment_builder();
                env_builder
                    // max size of memory map, can be changed later
                    .set_map_size(initial_map_bytes.unwrap_or(DEFAULT_INITIAL_MAP_BYTES))
                    // max number of DBs in this environment
                    .set_max_dbs(1)
                    // These flags make writes waaaaay faster by async writing to disk rather than blocking
                    // There is some loss of data integrity guarantees that comes with this
                    .set_flags(EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC);
                Rkv::from_env(path, env_builder)
            })
            .expect("Could not create the environment");

   
}

