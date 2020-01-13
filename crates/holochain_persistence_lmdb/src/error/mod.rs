use holochain_persistence_api::error::PersistenceError as BaseError;

#[derive(Shrinkwrap)]
pub struct PersistenceError(pub BaseError);

impl From<rkv::error::StoreError> for PersistenceError {
    fn from(e: rkv::error::StoreError) -> Self {
        Self(BaseError::ErrorGeneric(format!("{:?}", e)))
    }
}

impl Into<BaseError> for PersistenceError {
    fn into(self) -> BaseError {
        self.0
    }
}
