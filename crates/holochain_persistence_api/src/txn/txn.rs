use crate::cas::storage::ExampleLink;
/// Transactional trait extensions to the CAS and EAV persistence
use crate::{
    cas::{
        content::{Address, AddressableContent, Content},
        storage::{ContentAddressableStorage, EavTestSuite, StorageTestSuite},
    },
    eav::*,
    error::*,
    reporting::{ReportStorage, StorageReport},
};
use std::{collections::BTreeSet, fmt::Debug, marker::PhantomData};
use uuid::Uuid;

/// Defines a transactional writer, typically implemented over a cursor.
pub trait Writer {
    /// Commits the transaction. Returns a `PersistenceError` if the
    /// transaction does not succeed.
    fn commit(self) -> PersistenceResult<()>;
}

/// Cursor interface over both CAS and EAV databases. Provides transactional support
/// by providing a `Writer` across both of them.
pub trait Cursor<A: Attribute>:
    Writer + ContentAddressableStorage + EntityAttributeValueStorage<A>
{
}

// TODO Should cursor's even be cloneable? SPIKE this
clone_trait_object!(<A:Attribute> Cursor<A>);

/// A write that does nothing, for testing or for
/// impementations that don't require the commit and abort functions
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
    > ContentAddressableStorage for NonTransactionalCursor<A, CAS, EAV>
{
    fn add(&self, content: &dyn AddressableContent) -> PersistenceResult<()> {
        self.cas.add(content)
    }

    fn fetch(&self, address: &Address) -> PersistenceResult<Option<Content>> {
        self.cas.fetch(address)
    }

    fn contains(&self, address: &Address) -> PersistenceResult<bool> {
        self.cas.contains(address)
    }

    fn get_id(&self) -> Uuid {
        self.cas.get_id()
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
    > EntityAttributeValueStorage<A> for NonTransactionalCursor<A, CAS, EAV>
{
    fn add_eavi(
        &self,
        eavi: &EntityAttributeValueIndex<A>,
    ) -> PersistenceResult<Option<EntityAttributeValueIndex<A>>> {
        self.eav.add_eavi(eavi)
    }

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

    fn create_cursor(&self) -> PersistenceResult<Self::Cursor> {
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

    /// Creates a new cursor. Use carefully as one instance of a cursor may block another,
    /// especially when cursors are mutating the primary store.
    fn create_cursor(&self) -> PersistenceResult<Self::Cursor>;
}

/// A high level api which brings together a CAS, EAV, and
/// Cursor over them both. A cursor may start transactions over both
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
}

/// Provides a simple, extensable version of a persistance manager. Intended
/// to be specialized for a particular database implementation easily.
#[derive(Clone)]
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
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage + Clone,
        EAV: EntityAttributeValueStorage<A> + Clone,
        CP: CursorProvider<A>,
    > CursorProvider<A> for DefaultPersistenceManager<A, CAS, EAV, CP>
{
    type Cursor = CP::Cursor;

    fn create_cursor(&self) -> PersistenceResult<Self::Cursor> {
        self.cursor_provider.create_cursor()
    }
}

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
    TCP::Cursor: Clone,
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
        f: impl FnOnce(CI::Cursor) -> (),
    ) where
        CI::Cursor: Clone,
    {
        let cursor_result = cursor_provider.create_cursor();
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

    pub fn with_cursor(&self, context: &str, f: impl FnOnce(CP::Cursor) -> ()) {
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
            EavTestSuite::test_one_to_many::<Addressable, A, CP::Cursor>(cursor, attribute)
        })
    }

    pub fn eav_test_range<Addressable>(&self, attribute: &A)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_range", |cursor| {
            EavTestSuite::test_range::<Addressable, A, CP::Cursor>(cursor, attribute)
        })
    }

    pub fn eav_test_multiple_attributes<Addressable>(&self, attributes: Vec<A>)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_multiple_attributes", |cursor| {
            EavTestSuite::test_multiple_attributes::<Addressable, A, CP::Cursor>(cursor, attributes)
        })
    }

    pub fn eav_test_many_to_one<Addressable>(&self, attribute: &A)
    where
        Addressable: AddressableContent + Clone,
    {
        self.with_cursor("txn_eav_test_many_to_one", |cursor| {
            EavTestSuite::test_many_to_one::<Addressable, A, CP::Cursor>(cursor, attribute)
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
        _other_content: Content,
        transient_content: Content,
    ) where
        Addressable: AddressableContent + Clone + PartialEq + Debug,
        OtherAddressable: AddressableContent + Clone + PartialEq + Debug,
    {
        let addressable = Addressable::try_from_content(&content).unwrap();
        self.with_cursor("cas_eav_transaction_abort_test_setup", |cursor| {
            cursor.add(&addressable).unwrap();
            assert!(cursor.contains(&addressable.address()).unwrap());
        });

        self.with_cursor("cas_eav_transaction_abort_test_sanity_check", |cursor| {
            assert!(cursor.contains(&addressable.address()).unwrap());
        });

        let cursor_result = self.cursor_provider.create_cursor();
        assert!(
            cursor_result.is_ok(),
            format!(
                "Couldn't create cursor after setting primary database: {:?}",
                cursor_result.err()
            )
        );

        let cursor = cursor_result.unwrap();
        let transient_addressable = Addressable::try_from_content(&transient_content).unwrap();
        assert!(!cursor.contains(&transient_addressable.address()).unwrap());

        let cursor_add_result = cursor.add(&transient_addressable);
        assert!(
            cursor_add_result.is_ok(),
            "Cursor add failed for {:?}: {:?}",
            transient_addressable,
            cursor_add_result.err()
        );

        assert!(cursor.contains(&transient_addressable.address()).unwrap());
        drop(cursor);

        self.with_cursor("cas_eav_transaction_abort_test_purity_check", |cursor| {
            assert!(!cursor.contains(&transient_addressable.address()).unwrap());

            assert!(cursor.contains(&addressable.address()).unwrap());
        });
    }
}
