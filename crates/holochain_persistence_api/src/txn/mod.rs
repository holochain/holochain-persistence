/// Transactional trait extensions to the CAS and EAV persistence
use crate::{
    cas::storage::ContentAddressableStorage,
    eav::{Attribute, EntityAttributeValueStorage},
    error::*,
};
use std::{
    marker::PhantomData,
    sync::{Arc, RwLock},
};

/// Defines a transactional writer, typically implemented over a cursor.
pub trait Writer: objekt::Clone {
    /// Commits the transaction. Returns a `PersistenceError` if the
    /// transaction does not succeed.
    fn commit(&self) -> PersistenceResult<()>;

    /// Aborts the transaction explicitly. Otherwise, called upon `drop`.
    fn abort(&self) -> PersistenceResult<()>;
}

clone_trait_object!(Writer);

/// Cursor interface over both CAS and EAV databases. Provides transactional support
/// by providing a `Writer` across both of them.
pub trait Cursor<A: Attribute>:
    ContentAddressableStorage + EntityAttributeValueStorage<A> + Writer
{
}

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
    fn commit(&self) -> PersistenceResult<()> {
        Ok(())
    }
    fn abort(&self) -> PersistenceResult<()> {
        Ok(())
    }
}

/// A high level api which brings together a CAS, EAV, and
/// Cursor over them both. A cursor may start transactions over both
/// the stores or not, depending on implementation.
pub trait PersistenceManager<A: Attribute> {
    /// The type of Content Addressable Storage (CAS)
    type Cas: ContentAddressableStorage;
    /// The type of Entity Entity AttributeValue Storage (EAV)
    type Eav: EntityAttributeValueStorage<A>;
    /// The type of the cursor provider
    type CursorProvider: CursorProvider<A>;

    /// Gets the CAS storage.
    // TODO decide on locking strategy here. Maybe RwLock is not needed
    fn cas(&self) -> Arc<RwLock<Self::Cas>>;
    /// Gets the EAV storage
    // TODO decide on locking strategy here. Maybe RwLock is not needed
    fn eav(&self) -> Arc<RwLock<Self::Eav>>;

    /// Gets the cursor provider.
    // TODO decide on locking strategy here. Maybe RwLock is needed
    fn cursor_provider(&self) -> Arc<Self::CursorProvider>;
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

/// Provides a simple, extensable version of a persistance manager. Intended
/// to be specialized for a particular database implementation easily.
pub struct DefaultPersistenceManager<
    A: Attribute,
    CAS: ContentAddressableStorage,
    EAV: EntityAttributeValueStorage<A>,
    CP: CursorProvider<A>,
> {
    cas: Arc<RwLock<CAS>>,
    eav: Arc<RwLock<EAV>>,
    cursor_provider: Arc<CP>,
    phantom: PhantomData<A>,
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage,
        EAV: EntityAttributeValueStorage<A>,
        CP: CursorProvider<A>,
    > DefaultPersistenceManager<A, CAS, EAV, CP>
{
    pub fn new(cas: CAS, eav: EAV, cursor_provider: CP) -> Self {
        Self {
            cas: Arc::new(RwLock::new(cas)),
            eav: Arc::new(RwLock::new(eav)),
            cursor_provider: Arc::new(cursor_provider),
            phantom: PhantomData,
        }
    }
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage,
        EAV: EntityAttributeValueStorage<A>,
        CP: CursorProvider<A>,
    > PersistenceManager<A> for DefaultPersistenceManager<A, CAS, EAV, CP>
{
    type Cas = CAS;
    type Eav = EAV;
    type CursorProvider = CP;

    fn eav(&self) -> Arc<RwLock<Self::Eav>> {
        self.eav.clone()
    }

    fn cas(&self) -> Arc<RwLock<Self::Cas>> {
        self.cas.clone()
    }

    fn cursor_provider(&self) -> Arc<Self::CursorProvider> {
        self.cursor_provider.clone()
    }
}
