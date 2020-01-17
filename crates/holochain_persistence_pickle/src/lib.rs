//! CAS Implementations
//!
//! (CAS == Content Addressable Storage)
//!
//! This crate contains implementations for the CAS and EAV traits
//! which are defined but not implemented in the core_types crate.
#![warn(unused_extern_crates)]
#![feature(test)]
#[allow(unused_extern_crates)]
extern crate test;

pub mod cas;
pub mod eav;
pub mod txn;
