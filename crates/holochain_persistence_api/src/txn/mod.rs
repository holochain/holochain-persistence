use crate::{
    cas::storage::ContentAddressableStorage,
    eav::{Attribute, EntityAttributeValueStorage},
    error::*,
};
use std::sync::Arc;

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

    fn cas(&self) -> Arc<Self::Cas>;
    fn eav(&self) -> Arc<Self::Eav>;
}

trait CursorProvider<A: Attribute> {
    type Cursor: Cursor<A>;
    fn create_cursor(&self) -> PersistenceResult<Self::Cursor>;
}

pub struct DefaultCasEavManager<
    A: Attribute,
    CAS: ContentAddressableStorage,
    EAV: EntityAttributeValueStorage<A>,
> {
    cas: Arc<CAS>,
    eav: Arc<EAV>,
    phantom: std::marker::PhantomData<A>,
}

impl<A: Attribute, C: ContentAddressableStorage, E: EntityAttributeValueStorage<A>>
    DefaultCasEavManager<A, C, E>
{
    pub fn new(cas: C, eav: E) -> Self {
        Self {
            cas: Arc::new(cas),
            eav: Arc::new(eav),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<A: Attribute, CAS: ContentAddressableStorage, EAV: EntityAttributeValueStorage<A>>
    CasEavManager<A> for DefaultCasEavManager<A, CAS, EAV>
{
    type Cas = CAS;
    type Eav = EAV;

    fn eav(&self) -> Arc<Self::Eav> {
        self.eav.clone()
    }
    fn cas(&self) -> Arc<Self::Cas> {
        self.cas.clone()
    }
}
