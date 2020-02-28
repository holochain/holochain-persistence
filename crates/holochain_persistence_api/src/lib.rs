//! Holochain Persistence Api
//! This crate defines apis for a content addressable storage (cas) and a generic
//! entity attribute value index (eavi).
#![feature(try_trait)]
#![feature(never_type)]
#![warn(unused_extern_crates)]
#![feature(test)]
extern crate test;
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate shrinkwraprs;

extern crate chrono;
extern crate futures;
extern crate multihash;
extern crate regex;
extern crate rust_base58;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate holochain_json_derive;
extern crate holochain_json_api;
extern crate uuid;
pub mod cas;
pub mod eav;
pub mod error;
pub mod fixture;
pub mod has_uuid;
pub mod hash;
pub mod reporting;
pub mod txn;
pub mod univ_map;
#[macro_use]
extern crate objekt;

pub const GIT_HASH: &str = env!(
    "GIT_HASH",
    "failed to obtain git hash from build environment. Check build.rs"
);

// not docker build friendly
// https://circleci.com/gh/holochain/holochain-rust/10757
#[cfg(feature = "broken-tests")]
#[cfg(test)]
mod test_hash {
    use super::*;

    #[test]
    fn test_hash() {
        assert_eq!(GIT_HASH.chars().count(), 40);
        assert!(
            GIT_HASH.is_ascii(),
            "GIT HASH contains non-ascii characters"
        );
    }
}
