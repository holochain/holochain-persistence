use crate::{
    cas::storage::ContentAddressableStorage,
    eav::{Attribute, EntityAttributeValueStorage},
    error::*,
};
use std::{marker::PhantomData, sync::Arc};

pub trait Writer: objekt::Clone {
    fn commit(&self) -> PersistenceResult<()>;
    fn abort(&self) -> PersistenceResult<()>;
}

clone_trait_object!(Writer);
pub trait Cursor<A: Attribute>:
    ContentAddressableStorage + EntityAttributeValueStorage<A> + Writer
{
}

clone_trait_object!(<A:Attribute> Cursor<A>);

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

pub trait CasEavManager<A: Attribute> {
    type Cas: ContentAddressableStorage;
    type Eav: EntityAttributeValueStorage<A>;
    type CursorProvider: CursorProvider<A>;
    fn cas(&self) -> Arc<Self::Cas>;
    fn eav(&self) -> Arc<Self::Eav>;
    fn cursor_provider(&self) -> Arc<Self::CursorProvider>;
}

pub trait CursorProvider<A: Attribute> {
    type Cursor: Cursor<A>;
    fn create_cursor(&self) -> PersistenceResult<Self::Cursor>;
}

pub struct DefaultCasEavManager<
    A: Attribute,
    CAS: ContentAddressableStorage,
    EAV: EntityAttributeValueStorage<A>,
    CP: CursorProvider<A>,
> {
    cas: Arc<CAS>,
    eav: Arc<EAV>,
    cursor_provider: Arc<CP>,
    phantom: PhantomData<A>,
}

impl<
        A: Attribute,
        CAS: ContentAddressableStorage,
        EAV: EntityAttributeValueStorage<A>,
        CP: CursorProvider<A>,
    > DefaultCasEavManager<A, CAS, EAV, CP>
{
    pub fn new(cas: CAS, eav: EAV, cursor_provider: CP) -> Self {
        Self {
            cas: Arc::new(cas),
            eav: Arc::new(eav),
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
    > CasEavManager<A> for DefaultCasEavManager<A, CAS, EAV, CP>
{
    type Cas = CAS;
    type Eav = EAV;
    type CursorProvider = CP;
    fn eav(&self) -> Arc<Self::Eav> {
        self.eav.clone()
    }
    fn cas(&self) -> Arc<Self::Cas> {
        self.cas.clone()
    }

    fn cursor_provider(&self) -> Arc<Self::CursorProvider> {
        self.cursor_provider.clone()
    }
}
