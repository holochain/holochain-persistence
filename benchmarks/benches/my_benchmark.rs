#[macro_use]
extern crate bencher;
extern crate holochain_persistence_api;
extern crate holochain_persistence_file;
extern crate holochain_persistence_mem;
extern crate holochain_persistence_pickle;
extern crate holochain_persistence_lmdb;
extern crate tempfile;

use self::tempfile::tempdir;
use bencher::Bencher;
use holochain_persistence_api::{
    cas::{content::Content, content::ExampleAddressableContent, storage::EavTestSuite},
    eav::eavi::ExampleAttribute,
};
use holochain_persistence_file::eav::file::EavFileStorage;
use holochain_persistence_mem::eav::memory::EavMemoryStorage;
use holochain_persistence_pickle::eav::pickle::EavPickleStorage;
use holochain_persistence_lmdb::eav::lmdb::EavLmdbStorage;

/*----------  Memory Storage  ----------*/


fn bench_memory_eav_one_to_many(b: &mut Bencher) {
    let eav_storage = EavMemoryStorage::new();
    b.iter(|| {
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavMemoryStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

fn bench_memory_eav_many_to_one(b: &mut Bencher) {
    let eav_storage = EavMemoryStorage::new();
    b.iter(|| {
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavMemoryStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

/*----------  File Storage  ----------*/


fn bench_file_eav_one_to_many(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavFileStorage::new(temp_path).unwrap();
    b.iter(|| {
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavFileStorage<ExampleAttribute>,
        >(eav_storage.clone(), &&ExampleAttribute::WithoutPayload)
    })
}

fn bench_file_eav_many_to_one(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavFileStorage::new(temp_path).unwrap();
    b.iter(|| {
        EavTestSuite::test_many_to_one::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavFileStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

/*----------  Pickle Storage  ----------*/


fn bench_pickle_eav_one_to_many(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavPickleStorage::new(temp_path);
    b.iter(|| {
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavPickleStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

fn bench_pickle_eav_many_to_one(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavPickleStorage::new(temp_path);
    b.iter(|| {
        EavTestSuite::test_many_to_one::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavPickleStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

/*----------  LMDB Storage  ----------*/

fn bench_lmdb_eav_one_to_many(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavLmdbStorage::new(temp_path);
    b.iter(|| {
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavLmdbStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

fn bench_lmdb_eav_many_to_one(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavLmdbStorage::new(temp_path);
    b.iter(|| {
        EavTestSuite::test_many_to_one::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavLmdbStorage<ExampleAttribute>,
        >(eav_storage.clone(), &ExampleAttribute::WithoutPayload)
    })
}

fn bench_lmdb_eav_round_trip(b: &mut Bencher) {
    let temp = tempdir().expect("test was supposed to create temp dir");
    let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
    let eav_storage = EavLmdbStorage::new(temp_path);

    b.iter(|| {
        EavTestSuite::test_round_trip(
            eav_storage.clone(),
            Content::from_json("feep"),
            ExampleAttribute::WithoutPayload,
            Content::from_json("foo"),
        )
    })
}

benchmark_group!(
    benches,
    bench_memory_eav_many_to_one,
    bench_memory_eav_one_to_many,
    bench_file_eav_one_to_many,
    bench_file_eav_many_to_one,
    bench_pickle_eav_many_to_one,
    bench_pickle_eav_one_to_many,
    bench_lmdb_eav_many_to_one,
    bench_lmdb_eav_one_to_many,
    bench_lmdb_eav_round_trip,
);
benchmark_main!(benches);
