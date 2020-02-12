/// Enforces an identifier for a trait object. Used by 
/// database instances to implement identity style equals semantics
use uuid::Uuid;

pub trait HasUuid {

    /// Gets the `Uuid` for this trait object.
    fn get_id(&self) -> Uuid;
}
