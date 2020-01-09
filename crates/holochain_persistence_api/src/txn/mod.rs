use crate::cas::storage::*;
use crate::eav::*;
use crate::error::*;

pub trait Transaction<A:Attribute> : ContentAddressableStorage + EntityAttributeValueStorage<A> {

    fn commit() -> PersistenceResult<()>;
    fn abort() -> PersistenceResult<()>;
}
