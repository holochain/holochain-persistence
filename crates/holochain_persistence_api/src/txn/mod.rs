use crate::{
    cas::storage::ContentAddressableStorage,
    eav::{Attribute, EntityAttributeValueStorage},
    error::*,
};
use std::sync::Arc;

pub trait Writer {
    fn commit(&self) -> PersistenceResult<()>;
    fn abort(&self) -> PersistenceResult<()>;
}

pub struct AlwaysFailingWriter;

impl Writer for AlwaysFailingWriter {
    fn commit(&self) -> PersistenceResult<()> {
        Err(PersistenceError::ErrorGeneric("unimplemented".to_string()))
    }
    fn abort(&self) -> PersistenceResult<()> {
        Err(PersistenceError::ErrorGeneric("unimplemented".to_string()))
    }
}

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

pub trait WriterProvider {
    type Writer: Writer;
    fn create_writer(&self) -> Self::Writer;
}

pub trait CasEavManager<A: Attribute> {
    type Writer: Writer;
    type Cas: ContentAddressableStorage<Writer = Self::Writer>;
    type Eav: EntityAttributeValueStorage<A>;

    fn cas(&self) -> Arc<Self::Cas>;
    fn eav(&self) -> Arc<Self::Eav>;
}

pub struct DefaultCasEavManager<
    A: Attribute,
    C: ContentAddressableStorage,
    E: EntityAttributeValueStorage<A>,
> {
    cas: Arc<C>,
    eav: Arc<E>,
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

impl<
        W: Writer,
        A: Attribute,
        C: ContentAddressableStorage<Writer = W>,
        E: EntityAttributeValueStorage<A>,
    > CasEavManager<A> for DefaultCasEavManager<A, C, E>
{
    type Writer = W;
    type Cas = C;
    type Eav = E;

    fn eav(&self) -> Arc<Self::Eav> {
        self.eav.clone()
    }
    fn cas(&self) -> Arc<Self::Cas> {
        self.cas.clone()
    }
}
