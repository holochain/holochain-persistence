use holochain_persistence_api::cas::*;
use holochain_persistence_api::eav::*;
use holochain_persistence_api::txn::*;
use rkv::Writer;
use crate::common::LmdbInstance;

struct LmdbTransaction {
    writer: rkv::Writer,
    staging_db : LmdbInstance
}
