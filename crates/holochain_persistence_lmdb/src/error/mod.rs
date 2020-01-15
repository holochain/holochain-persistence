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

pub fn to_api_error(e: rkv::error::StoreError) -> BaseError {
    // Convert to lmdb persistence error
    let e: crate::error::PersistenceError = e.into();

    // Convert into api persistence error
    e.into()
}
