//! [`BackendError`]: why a backend request failed.

use tonic::Status;
use zakura_chain::amount::{self};

/// Why a backend request failed. Each variant maps to one gRPC status code.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    /// The backend cannot answer now: the node is unreachable, a read failed, or this backend
    /// has no node handle for the request. Maps to `UNAVAILABLE`.
    #[error("{0}")]
    Unavailable(String),
    /// The node refused the request's content, for example a transaction that failed
    /// verification. Maps to `INVALID_ARGUMENT`.
    #[error("{0}")]
    Rejected(String),
    /// The backend answered with something it should not have. Maps to `INTERNAL`.
    #[error("{0}")]
    Unexpected(String),
}

/// A backend answer with an amount out of range is an answer that it should not have sent.
impl From<amount::Error> for BackendError {
    fn from(error: amount::Error) -> Self {
        Self::Unexpected(format!("backend sent an amount out of range: {error}"))
    }
}

impl From<BackendError> for Status {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::Unavailable(message) => Self::unavailable(message),
            BackendError::Rejected(message) => Self::invalid_argument(message),
            BackendError::Unexpected(message) => Self::internal(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_map_to_one_status_code_each() {
        assert_eq!(
            Status::from(BackendError::Unavailable("a".into())).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            Status::from(BackendError::Rejected("b".into())).code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            Status::from(BackendError::Unexpected("c".into())).code(),
            tonic::Code::Internal
        );
        assert_eq!(Status::from(BackendError::Rejected("b".into())).message(), "b");
    }
}
