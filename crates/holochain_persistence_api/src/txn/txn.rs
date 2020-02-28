use crate::{
    cas::storage::{ExampleContentAddressableStorage, ExampleLink},
    eav::ExampleEntityAttributeValueStorage,
    has_uuid::HasUuid,
    univ_map::*,
};
/// Transactional trait extensions to the CAS and EAV persistence
use crate::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::{
            AddContent, ContentAddressableStorage, EavTestSuite, FetchContent, StorageTestSuite,
        },
    },
    eav::storage::{AddEavi, FetchEavi},
    eav::*,
    error::*,
    reporting::{ReportStorage, StorageReport},
};
use std::{collections::BTreeSet, fmt::Debug, marker::PhantomData, sync::Arc};
use uuid::Uuid;

/// Defines a transactional writer, typically implemented over a cursor.
pub trait Writer {
    /// Commits the transaction. Returns a `PersistenceError` if the
    /// transaction does not succeed.
    fn commit(self) -> PersistenceResult<()>;
}

/// Defines a dynamic transactional writer. Useful for situations where
/// the concrete database is abstracted over as a trait object.
pub trait WriterDyn {
    /// Commits the transaction. Returns a `PersistenceError` if the
    /// transaction does not succeed.
    fn commit(self: Box<Self>) -> PersistenceResult<()>;
}

impl<W: Writer> WriterDyn for W {
    fn commit(self: Box<Self>) -> PersistenceResult<()> {
        (*self).commit()
    }
}

/// Cursor interface over both CAS and EAV databases.
pub trait Cursor<A: Attribute>: FetchContent + FetchEavi<A> + HasUuid {}

/// Writeable cursor interface over both CAS and EAV databases.
pub trait CursorRw<A: Attribute>: Cursor<A> + AddContent + AddEavi<A> {}

/// Dynamic cursor interface over both CAS and EAV databases. Provides transactional support
/// by providing a `WriterDyn` across both of them. Useful for situations where
/// the concrete database is abstracted over as a trait object.
pub trait CursorRwDyn<A: Attribute>: CursorRw<A> + WriterDyn {}

impl<A: Attribute, C: Cursor<A> + AddContent + AddEavi<A>> CursorRw<A> for C {}
impl<A: Attribute, C: CursorRw<A> + WriterDyn> CursorRwDyn<A> for C {}

clone_trait_object!(<A:Attribute> Cursor<A>);
clone_trait_object!(<A:Attribute> CursorRw<A>);

/// A write that does nothing, for testing or for
/// implementations that don't require the commit and abort functions
/// to do anything critical.
#[derive(Clone)]
pub struct NoopWriter;

impl NoopWriter {
    pub fn new() -> Self {
        NoopWriter
    }
}

impl Writer for NoopWriter {
    fn commit(self) -> PersistenceResult<()> {
        Ok(())
    }
}

/// A default cursor that does not execute anything explicitly within transactions.
#[derive(Clone, Debug)]
pub struct NonTransactionalCursor<
    A: Attribute,
    CAS: ContentAddressableStorage,
    EAV: EntityAttributeValueStorage<A>,
> {
    cas: CAS,
    eav: EAV,
    phantom: PhantomData<A>,
}
impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > NonTransactionalCursor<A, CAS, EAV>
{
    pub fn new(cas: CAS, eav: EAV) -> Self {
        Self {
            cas,
            eav,
            phantom: PhantomData,
        }
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > AddContent for NonTransactionalCursor<A, CAS, EAV>
{
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        self.cas.add(content)
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > FetchContent for NonTransactionalCursor<A, CAS, EAV>
{
    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        self.cas.fetch(address)
    }

    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        self.cas.contains(address)
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > HasUuid for NonTransactionalCursor<A, CAS, EAV>
{
    fn get_id(&self) -> Uuid {
        self.cas.get_id()
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > AddEavi<A> for NonTransactionalCursor<A, CAS, EAV>
{
    fn add_eavi(
        &self,
        eavi: &EntityAttributeValueIndex<A>,
    ) -> PersistenceResult<Option<EntityAttributeValueIndex<A>>> {
        self.eav.add_eavi(eavi)
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > FetchEavi<A> for NonTransactionalCursor<A, CAS, EAV>
{
    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        self.eav.fetch_eavi(query)
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > ReportStorage for NonTransactionalCursor<A, CAS, EAV>
{
    fn get_storage_report(&self) -> PersistenceResult<StorageReport> {
        self.cas.get_storage_report()
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > Cursor<A> for NonTransactionalCursor<A, CAS, EAV>
{
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > Writer for NonTransactionalCursor<A, CAS, EAV>
{
    fn commit(self) -> PersistenceResult<()> {
        NoopWriter {}.commit()
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > CursorProvider<A> for NonTransactionalCursor<A, CAS, EAV>
{
    type Cursor = Self;
    type CursorRw = Self;

    fn create_cursor(&self) -> PersistenceResult<Self::Cursor> {
        Ok(self.clone())
    }

    fn create_cursor_rw(&self) -> PersistenceResult<Self::CursorRw> {
        Ok(self.clone())
    }
}

/// Creates cursors over both EAV and CAS instances. May acquire read or write
/// resources to do so, depending on implementation.
///
/// Some cursors may cascade over temporary (aka "scratch") databases to improve
/// concurrency performance.
///
/// An advanced cursor might wrap other cursors and check external resources (such as peer data in a network)
/// in addition to the cursors it wraps.
pub trait CursorProvider<A: Attribute> {
    /// The type of a cursor for this cursor provider
    type Cursor: Cursor<A>;
    type CursorRw: CursorRw<A> + Writer;

    /// Creates a new read/write cursor. Use carefully as one instance
    /// of a cursor may block another, especially when cursors are mutating
    /// the primary store.
    fn create_cursor_rw(&self) -> PersistenceResult<Self::CursorRw>;

    /// Creates a new read cursor.
    fn create_cursor(&self) -> PersistenceResult<Self::Cursor>;
}

/// Creates cursors over both EAV and CAS instances. May acquire read or write
/// resources to do so, depending on implementation. A pure trait object
/// version, allowing users to not care about the particular Cursor, CAS, or
/// EAV implementation.
///
/// Some cursors may cascade over temporary (aka "scratch") databases to improve
/// concurrency performance.
///
/// An advanced cursor might wrap other cursors and check external resources (such as peer data in a network)
/// in addition to the cursors it wraps.
///
/// This provides a pure trait object style interface to a `CursorProvider`.
pub trait CursorProviderDyn<A: Attribute>: Send + Sync + Debug {
    /// Creates a new boxed cursor object. Use carefully as one instance of a cursor
    /// may block another, especially when cursors are mutating the primary store.
    fn create_cursor_rw(&self) -> PersistenceResult<Box<dyn CursorRwDyn<A>>>;

    fn create_cursor(&self) -> PersistenceResult<Box<dyn Cursor<A>>>;
}

impl<
        A: Attribute,
        C: Cursor<A> + 'static,
        CW: CursorRw<A> + 'static + Writer,
        CP: CursorProvider<A, Cursor = C, CursorRw = CW> + Send + Sync + Debug,
    > CursorProviderDyn<A> for CP
{
    fn create_cursor_rw(&self) -> PersistenceResult<Box<dyn CursorRwDyn<A>>> {
        let cp: &CP = self;
        let cursor = cp.create_cursor_rw()?;
        Ok(Box::new(cursor) as Box<dyn CursorRwDyn<A>>)
    }

    fn create_cursor(&self) -> PersistenceResult<Box<dyn Cursor<A>>> {
        let cp: &CP = self;
        let cursor = cp.create_cursor()?;
        Ok(Box::new(cursor) as Box<dyn Cursor<A>>)
    }
}

/// A high level api which brings together a CAS, EAV, and
/// Cursor over them. A cursor may start transactions over both
/// the stores or not, depending on implementation.
pub trait PersistenceManager<A: Attribute>: CursorProvider<A> {
    /// The type of Content Addressable Storage (CAS)
    type Cas: ContentAddressableStorage;
    /// The type of Entity Entity AttributeValue Storage (EAV)
    type Eav: EntityAttributeValueStorage<A>;

    /// Gets the CAS storage.
    fn cas(&self) -> Self::Cas;
    /// Gets the EAV storage
    fn eav(&self) -> Self::Eav;

    fn get_id(&self) -> Uuid;
}

/// Maps the type of a schema and a database reference to a particular
/// cursor (backed by both a CAS and EAV).
pub type CursorRwKey<A> = Key<String, Box<dyn CursorRw<A>>>;

/// A cursor that permits transactional semantics over multiple database
/// sources.
pub trait EnvCursor: Writer {
    /// Gets or creates a read/write cursor for a particular CAS/EAV pair
    /// as mapped by the given `key`. Produces a `PersistenceError` if
    /// the key is not associated with this environment.
    fn cursor_rw<A: Attribute + 'static + serde::de::DeserializeOwned>(
        &mut self,
        key: &CursorRwKey<A>,
    ) -> PersistenceResult<Box<dyn CursorRw<A>>>;
}

/// Represents a single source of databases. Transactions can be scoped
/// over all of them. To initiate a transaction invoke the `create_cursor`
/// trait method.
pub trait Environment {
    /// The type of a cursor for an entire environment.
    type EnvCursor: EnvCursor;

    /// Creates a new cursor that provides transactional support
    /// across mutiple databases within the environment.
    ///
    /// Scratch databases and other temporary resources may be created
    /// for some environment implementations.
    fn create_cursor(self: Arc<Self>) -> PersistenceResult<Self::EnvCursor>;
}

/// A degenerate environment that only returns erroneous `PersistenceResult`
/// instances.
pub struct DefaultEnvironment;

impl DefaultEnvironment {
    pub fn new() -> Self {
        Self {}
    }
}

impl EnvCursor for DefaultEnvironment {
    fn cursor_rw<A: Attribute + 'static + serde::de::DeserializeOwned>(
        &mut self,
        _key: &CursorRwKey<A>,
    ) -> PersistenceResult<Box<dyn CursorRw<A>>> {
        Err("cross transactional read/write cursors unsupported".into())
    }
}

impl Writer for DefaultEnvironment {
    fn commit(self) -> PersistenceResult<()> {
        Err("cross transactional cursors unsupported: commit failed".into())
    }
}

/// A high level api which brings together a CAS, EAV, and
/// Cursor over them. A cursor may start transactions over both
/// the stores or not, depending on implementation. Differs from
/// `PersistenceManager` trait removing the associated types and
/// using trait objects strictly.
pub trait PersistenceManagerDyn<A: Attribute>: CursorProviderDyn<A> {
    /// Gets the CAS storage.
    fn cas(&self) -> Arc<dyn ContentAddressableStorage>;
    /// Gets the EAV storage
    fn eav(&self) -> Arc<dyn EntityAttributeValueStorage<A>>;

    fn get_id(&self) -> Uuid;
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone + 'static,
        EAV: EntityAttributeValueStorage<A> + Clone + 'static,
        CP: CursorProvider<A> + 'static + Send + Sync + Debug,
    > PersistenceManagerDyn<A> for DefaultPersistenceManager<A, CAS, EAV, CP>
where
    CP::Cursor: 'static,
    CP::CursorRw: 'static,
{
    fn cas(&self) -> Arc<dyn ContentAddressableStorage> {
        Arc::new(self.cas.clone())
    }

    fn eav(&self) -> Arc<dyn EntityAttributeValueStorage<A>> {
        Arc::new(self.eav.clone())
    }

    fn get_id(&self) -> Uuid {
        PersistenceManager::get_id(self)
    }
}

/// Provides a simple, extensable version of a persistence manager. Intended
/// to be specialized for a particular database implementation easily.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DefaultPersistenceManager<
    A: Attribute,
    CAS: ContentAddressableStorage + Clone,
    EAV: EntityAttributeValueStorage<A> + Clone,
    CP: CursorProvider<A>,
> {
    cas: CAS,
    eav: EAV,
    cursor_provider: CP,
    phantom: PhantomData<A>,
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
        CP: CursorProvider<A>,
    > DefaultPersistenceManager<A, CAS, EAV, CP>
{
    pub fn new(cas: CAS, eav: EAV, cursor_provider: CP) -> Self {
        Self {
            cas: cas,
            eav: eav,
            cursor_provider,
            phantom: PhantomData,
        }
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > DefaultPersistenceManager<A, CAS, EAV, NonTransactionalCursor<A, CAS, EAV>>
{
    /// Create a new persistence manager with no transactional support. Cursors essentially
    /// behave as random accessors, much like if one uses the CAS or EAV directly.
    ///
    /// Useful for testing or implementations of databases which don't support transactions.
    pub fn new_non_transactional(cas: CAS, eav: EAV) -> Self {
        let cursor_provider = NonTransactionalCursor {
            cas: cas.clone(),
            eav: eav.clone(),
            phantom: PhantomData,
        };

        Self {
            cas: cas,
            eav: eav,
            cursor_provider,
            phantom: PhantomData,
        }
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
        CP: CursorProvider<A>,
    > PersistenceManager<A> for DefaultPersistenceManager<A, CAS, EAV, CP>
{
    type Cas = CAS;
    type Eav = EAV;
    fn eav(&self) -> Self::Eav {
        self.eav.clone()
    }

    fn cas(&self) -> Self::Cas {
        self.cas.clone()
    }

    fn get_id(&self) -> Uuid {
        self.cas.get_id()
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
        CP: CursorProvider<A>,
    > CursorProvider<A> for DefaultPersistenceManager<A, CAS, EAV, CP>
{
    type Cursor = CP::Cursor;
    type CursorRw = CP::CursorRw;

    fn create_cursor(&self) -> PersistenceResult<Self::Cursor> {
        self.cursor_provider.create_cursor()
    }

    fn create_cursor_rw(&self) -> PersistenceResult<Self::CursorRw> {
        self.cursor_provider.create_cursor_rw()
    }
}

pub type ExamplePersistenceManager<A> = DefaultPersistenceManager<
    A,
    ExampleContentAddressableStorage,
    ExampleEntityAttributeValueStorage<A>,
    NonTransactionalCursor<
        A,
        ExampleContentAddressableStorage,
        ExampleEntityAttributeValueStorage<A>,
    >,
>;

pub fn new_example_persistence_manager<A: Attribute + Default>(
) -> PersistenceResult<ExamplePersistenceManager<A>> {
    let cas = ExampleContentAddressableStorage::new()?;
    let eav = ExampleEntityAttributeValueStorage::new();
    let cursor_provider = NonTransactionalCursor::new(cas.clone(), eav.clone());

    Ok(DefaultPersistenceManager::new(cas, eav, cursor_provider))
}

/// Harness to test the transactional features of a persistence manager.
pub struct PersistenceManagerTestSuite<
    A: Attribute + Clone,
    CP: CursorProvider<A> + Clone + 'static,
    TCP: CursorProvider<ExampleLink> + Clone + 'static,
> {
    cursor_provider: CP,
    phantom: PhantomData<A>,
    tombstone_cursor_provider: TCP,
}

impl<
        A: Attribute + Clone,
        CP: CursorProvider<A> + Clone + 'static,
        TCP: CursorProvider<ExampleLink> + Clone + 'static,
    > PersistenceManagerTestSuite<A, CP, TCP>
where
    CP::Cursor: Clone + 'static,
    CP::CursorRw: Clone + 'static,
    TCP::Cursor: Clone,
    TCP::CursorRw: Clone,
{
    pub fn new(cursor_provider: CP, tombstone_cursor_provider: TCP) -> Self {
        Self {
            cursor_provider,
            tombstone_cursor_provider,
            phantom: PhantomData,
        }
    }

    /// Calls the given function `f` with a live cursor and commits the transaction.
    /// Asserts the commit succeeded.
    fn with_cursor_internal<AA: Attribute, CI: CursorProvider<AA>>(
        cursor_provider: &CI,
        context: &str,
        f: impl FnOnce(CI::CursorRw) -> (),
    ) where
        CI::CursorRw: Clone,
    {
        let cursor_result = cursor_provider.create_cursor_rw();
        assert!(
            cursor_result.is_ok(),
            format!(
                "{}: Failed to create cursor: {:?}",
                context,
                cursor_result.err()
            )
        );
        let cursor = cursor_result.unwrap();
        f(cursor.clone());
        let commit_result = cursor.commit();
        assert!(
            commit_result.is_ok(),
            format!(
                "{}: Failed to commit cursor: {:?}",
                context,
                commit_result.err()
            )
        );
    }

    pub fn with_cursor(&self, context: &str, f: impl FnOnce(CP::CursorRw) -> ()) {
        Self::with_cursor_internal(&self.cursor_provider, context, f)
    }

    pub fn cas_round_trip_test<Addressable, OtherAddressable>(
        &self,
        content: Content,
        other_content: Content,
    ) where
        Addressable: AddressableContent + Clone + PartialEq + Debug,
        OtherAddressable: AddressableContent + Clone + PartialEq + Debug,
    {
        self.with_cursor("txn_cas_round_trip_test", |cursor| {
            let cas_test_suite = StorageTestSuite::new(cursor);
            cas_test_suite.round_trip_test::<Addressable, OtherAddressable>(content, other_content);
        })
    }

    pub fn eav_test_round_trip(
        &self,
        entity_content: impl AddressableContent,
        attribute: A,
        value_content: impl AddressableContent,
    ) {
        self.with_cursor("txn_eav_test_round_trip", |cursor| {
            EavTestSuite::test_round_trip(cursor, entity_content, attribute, value_content)
        })
    }

    pub fn eav_test_one_to_many<Addressable>(&self, attribute: &A)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_one_to_many", |cursor| {
            EavTestSuite::test_one_to_many::<Addressable, A, CP::CursorRw>(cursor, attribute)
        })
    }

    pub fn eav_test_range<Addressable>(&self, attribute: &A)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_range", |cursor| {
            EavTestSuite::test_range::<Addressable, A, CP::CursorRw>(cursor, attribute)
        })
    }

    pub fn eav_test_multiple_attributes<Addressable>(&self, attributes: Vec<A>)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_multiple_attributes", |cursor| {
            EavTestSuite::test_multiple_attributes::<Addressable, A, CP::CursorRw>(
                cursor, attributes,
            )
        })
    }

    pub fn eav_test_many_to_one<Addressable>(&self, attribute: &A)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_many_to_one", |cursor| {
            EavTestSuite::test_many_to_one::<Addressable, A, CP::CursorRw>(cursor, attribute)
        })
    }

    pub fn eav_test_tombstone<Addressable>(&self)
    where
        Addressable: AddressableContent + Clone,
    {
        Self::with_cursor_internal(
            &self.tombstone_cursor_provider,
            "txn_eav_test_tombstone",
            |cursor| EavTestSuite::test_tombstone::<Addressable, _>(cursor),
        )
    }

    pub fn cas_eav_test_transaction_abort<Addressable, OtherAddressable>(
        &self,
        content: Content,
        attribute: A,
        transient_content: Content,
    ) where
        Addressable: AddressableContent + Clone + PartialEq + Debug,
        OtherAddressable: AddressableContent + Clone + PartialEq + Debug,
    {
        let addressable = Addressable::try_from_content(&content).unwrap();
        let eavi_query = EaviQuery::new(
            Some(content.address()).into(),
            Some(attribute.clone()).into(),
            Some(content.address()).into(),
            IndexFilter::LatestByAttribute,
            None,
        );
        self.with_cursor("cas_eav_transaction_abort_test_setup", |cursor| {
            cursor.add(&addressable).unwrap();
            let eavi = EntityAttributeValueIndex::new(
                &addressable.address(),
                &attribute,
                &addressable.address(),
            )
            .unwrap();
            cursor.add_eavi(&eavi).unwrap();
            assert!(cursor.contains(&addressable.address()).unwrap());
            assert_eq!(cursor.fetch_eavi(&eavi_query).unwrap().len(), 1);
        });

        self.with_cursor("cas_eav_transaction_abort_test_sanity_check", |cursor| {
            assert!(cursor.contains(&addressable.address()).unwrap());
            assert_eq!(cursor.fetch_eavi(&eavi_query).unwrap().len(), 1);
        });

        let cursor_result = self.cursor_provider.create_cursor_rw();
        assert!(
            cursor_result.is_ok(),
            format!(
                "Couldn't create cursor after setting up primary cas database: {:?}",
                cursor_result.err()
            )
        );

        let cursor = cursor_result.unwrap();
        let transient_addressable = Addressable::try_from_content(&transient_content).unwrap();
        assert!(!cursor.contains(&transient_addressable.address()).unwrap());

        let transient_eavi_query = EaviQuery::new(
            Some(transient_content.address()).into(),
            Some(attribute.clone()).into(),
            Some(transient_content.address()).into(),
            IndexFilter::LatestByAttribute,
            None,
        );
        assert!(cursor.fetch_eavi(&transient_eavi_query).unwrap().is_empty());

        let cursor_add_result = cursor.add(&transient_addressable);
        assert!(
            cursor_add_result.is_ok(),
            "Cursor add failed for cas {:?}: {:?}",
            transient_addressable,
            cursor_add_result.err()
        );

        let transient_eavi = EntityAttributeValueIndex::new(
            &transient_addressable.address(),
            &attribute,
            &transient_addressable.address(),
        )
        .unwrap();
        let cursor_add_result = cursor.add_eavi(&transient_eavi);
        assert!(
            cursor_add_result.is_ok(),
            "Cursor add failed for eavi {:?}: {:?}",
            transient_addressable,
            cursor_add_result.err()
        );
        assert_eq!(cursor.fetch_eavi(&transient_eavi_query).unwrap().len(), 1);

        drop(cursor);

        self.with_cursor("cas_eav_transaction_abort_test_purity_check", |cursor| {
            assert!(!cursor.contains(&transient_addressable.address()).unwrap());

            assert!(cursor.contains(&addressable.address()).unwrap());
            assert_eq!(cursor.fetch_eavi(&eavi_query).unwrap().len(), 1);
            assert!(cursor.fetch_eavi(&transient_eavi_query).unwrap().is_empty());
        });
    }
}
