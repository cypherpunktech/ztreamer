//! [`BackendError`]: why a backend request failed.

use tonic::Status;
use zakura_chain::amount::{self};

/// Why a backend request failed. Each variant maps to one gRPC status code.
///
/// The three `Node` variants are the failed calls to a remote node, by the state of the node.
/// A wallet client gets `UNAVAILABLE` for each of them, as for [`Self::Unavailable`]. A task of
/// the daemon uses the state to stop or to wait.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    /// The backend cannot answer now: a read or a task failed, or this backend has no node handle
    /// for the request. Maps to `UNAVAILABLE`.
    #[error("{0}")]
    Unavailable(String),
    /// The remote node did not answer, after the retries of the client. Maps to `UNAVAILABLE`.
    #[error("{0}")]
    NodeGone(String),
    /// The remote node refuses the call with an answer that does not change on a retry. Maps to
    /// `UNAVAILABLE`.
    #[error("{0}")]
    NodeRefuses(String),
    /// The remote node is in warm-up. A later call can succeed. Maps to `UNAVAILABLE`.
    #[error("{0}")]
    NodeSyncing(String),
    /// The node refused the request's content, for example a transaction that failed
    /// verification. Maps to `INVALID_ARGUMENT`.
    #[error("{0}")]
    Rejected(String),
    /// The backend answered with something it should not have. Maps to `INTERNAL`.
    #[error("{0}")]
    Unexpected(String),
}

impl BackendError {
    /// True for each error that maps to `UNAVAILABLE`: the backend or its node cannot answer
    /// now. The request has no fault.
    pub const fn is_unavailable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_) | Self::NodeGone(_) | Self::NodeRefuses(_) | Self::NodeSyncing(_)
        )
    }
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
            BackendError::Unavailable(message)
            | BackendError::NodeGone(message)
            | BackendError::NodeRefuses(message)
            | BackendError::NodeSyncing(message) => Self::unavailable(message),
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

    #[test]
    fn each_node_state_maps_to_unavailable() {
        let errors = [
            BackendError::Unavailable("a".into()),
            BackendError::NodeGone("b".into()),
            BackendError::NodeRefuses("c".into()),
            BackendError::NodeSyncing("d".into()),
        ];
        for error in errors {
            assert!(error.is_unavailable(), "{error}");
            let message = error.to_string();
            let status = Status::from(error);
            assert_eq!(status.code(), tonic::Code::Unavailable);
            assert_eq!(status.message(), message);
        }
        assert!(!BackendError::Rejected("e".into()).is_unavailable());
        assert!(!BackendError::Unexpected("f".into()).is_unavailable());
    }
}
