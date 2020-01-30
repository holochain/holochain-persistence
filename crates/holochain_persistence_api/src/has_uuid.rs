use uuid::Uuid;

pub trait HasUuid {
    fn get_id(&self) -> Uuid;
}
