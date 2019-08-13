use super::error::{PersistenceError, PersistenceResult};

pub trait ReportStorage {
    /// Return the number of bytes this storage implementation is using on the host system.
    /// The actual implementation is up to the author of the persistence implementation
    /// and may be disk usage or memory usage
    fn get_byte_count(&self) -> PersistenceResult<usize> {
        Err(PersistenceError::ErrorGeneric(
            "Not implemented for this storage type".into(),
        ))
    }
}
