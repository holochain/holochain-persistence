use holochain_persistence_api::error::PersistenceError as BaseError;
use rkv::error::StoreError;

#[derive(Shrinkwrap)]
pub struct PersistenceError(pub BaseError);

impl From<StoreError> for PersistenceError {
    fn from(e: rkv::error::StoreError) -> Self {
        Self(BaseError::ErrorGeneric(format!("{:?}", e)))
    }
}

impl Into<BaseError> for PersistenceError {
    fn into(self) -> BaseError {
        self.0
    }
}

pub fn to_api_error(e: StoreError) -> BaseError {
    // Convert to lmdb persistence error
    let e: crate::error::PersistenceError = e.into();

    // Convert into api persistence error
    e.into()
}

pub fn is_store_full_result<T>(result: Result<T, StoreError>) -> bool {
    if let Err(e) = &result {
        is_store_full_error(e)   
    } else {
       false 
    }
}

pub fn is_store_full_error(e: &StoreError) -> bool {
    match e {
        rkv::error::StoreError::LmdbError(lmdb::Error::MapFull) => true,
        _ => false
    }
}
