use holochain_persistence_api::cas::*;
use holochain_persistence_api::eav::*;
use holochain_persistence_api::txn::*;
use rkv::Writer;
use crate::common::LmdbInstance;


struct LmdbCursor<'prim_env, 'staging_env> {
    // rw cursor for the staging db
    // a reference to the staging db itself?
    // a ro cursor over primary db
    // reference to primary db
    // lazy write cursor to primary db
    primary_db_writer: FnOnce<Writer<'prim_env>>,
    staging_db_writer: FnOnce<Writer<'staging_env>>
    primary_db_reader: FnOnce<Reader>,
    staging_db_reader: FnOnce<Reader>,
    staging_db: FnOnce<LmdbInstance>,
    primary_db: FnOnce<LmdbInstance>,
}

pub fn new_manager<A:Attribute, P: AsRef<Path> + Clone>(
        db_name: P1,
        eav_db_name: P2,
        cas_initial_map_bytes: Option<usize>,
        eav_initial_map_bytes: Option<usize>,
) -> DefaultCasEavManager<A, LmbdbStorage, LmdbEntityAttributeValueStorage<A>> {

}

