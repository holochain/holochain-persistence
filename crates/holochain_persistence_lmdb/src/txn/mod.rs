use holochain_persistence_api::cas::*;
use holochain_persistence_api::eav::*;
use holochain_persistence_api::txn::*;
use rkv::Writer;
use crate::common::LmdbInstance;


pub fn new_manager<A:Attribute, P: AsRef<Path> + Clone>(
    Pat
        cas_db_name: P1,
        eav_db_name: P2,
        cas_initial_map_bytes: Option<usize>,
        eav_initial_map_bytes: Option<usize>,
) -> DefaultCasEavManager<A, LmbdbStorage, LmdbEntityAttributeValueStorage<A>> {

}

