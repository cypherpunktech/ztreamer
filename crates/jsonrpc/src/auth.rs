//! Credentials for the node's JSON-RPC listener.

use std::{
    fmt,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::HeaderValue;

use crate::RpcError;

/// A credential. Its `Debug` output does not show the value, so a client or a configuration
/// can be logged.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a credential.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The credential, for building the `Authorization` header.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

/// How the client authenticates to the node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Auth {
    /// No `Authorization` header. Works against a listener with cookie auth disabled, which on
    /// Mainnet and Testnet serves only the restricted method set.
    #[default]
    None,
    /// zakura's cookie file, `.cookie` under the node's cache directory.
    ///
    /// The node rewrites it on every start. The client reads it when built and reads it again
    /// after a rejected call, so a node restart does not need a client restart.
    Cookie(PathBuf),
    /// Fixed user and password, sent as HTTP basic auth.
    Basic {
        /// User name. zakura ignores it and checks only the password.
        user: String,
        /// Password.
        password: Secret,
    },
}

impl Auth {
    /// Builds the `Authorization` header value, reading the cookie file if there is one.
    pub(crate) fn header(&self) -> Result<Option<HeaderValue>, RpcError> {
        match self {
            Self::None => Ok(None),
            Self::Cookie(path) => read_cookie(path).map(|cookie| Some(basic(cookie.expose()))),
            Self::Basic { user, password } => {
                if user.contains(':') {
                    return Err(RpcError::Config("basic auth user must not contain ':'".into()));
                }
                Ok(Some(basic(&format!("{user}:{}", password.expose()))))
            }
        }
    }

    pub(crate) const fn is_cookie(&self) -> bool {
        matches!(self, Self::Cookie(_))
    }
}

/// Reads a zakura cookie file and returns its `user:secret` line.
///
/// The file holds one line, `__cookie__:<secret>`. The node compares only the part after the
/// colon, so the whole line is what goes into the basic auth header.
fn read_cookie(path: &Path) -> Result<Secret, RpcError> {
    let contents = std::fs::read_to_string(path).map_err(|error| {
        RpcError::Config(format!("cannot read cookie file {}: {error}", path.display()))
    })?;
    let line = contents.trim();
    if line.is_empty() || !line.contains(':') {
        return Err(RpcError::Config(format!(
            "cookie file {} does not hold a user:secret line",
            path.display()
        )));
    }
    Ok(Secret::new(line))
}

/// Builds a basic auth header and marks it sensitive, so `http` keeps it out of `Debug` output.
fn basic(credentials: &str) -> HeaderValue {
    let encoded = STANDARD.encode(credentials.as_bytes());
    let mut value =
        HeaderValue::try_from(format!("Basic {encoded}")).expect("base64 is a valid header value");
    value.set_sensitive(true);
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_file_becomes_basic_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        std::fs::write(&path, "__cookie__:s3cret\n").unwrap();
        let header = Auth::Cookie(path).header().unwrap().unwrap();
        assert_eq!(
            header.to_str().unwrap(),
            format!("Basic {}", STANDARD.encode(b"__cookie__:s3cret"))
        );
    }

    #[test]
    fn cookie_file_must_exist_and_have_a_colon() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(matches!(Auth::Cookie(missing).header(), Err(RpcError::Config(_))));
        let bad = dir.path().join("bad");
        std::fs::write(&bad, "nocolon").unwrap();
        assert!(matches!(Auth::Cookie(bad).header(), Err(RpcError::Config(_))));
    }

    #[test]
    fn basic_auth_rejects_colon_in_user() {
        let auth = Auth::Basic { user: "a:b".into(), password: Secret::new("p") };
        assert!(matches!(auth.header(), Err(RpcError::Config(_))));
        assert!(Auth::None.header().unwrap().is_none());
    }

    #[test]
    fn secrets_stay_out_of_debug_output() {
        let auth = Auth::Basic { user: "ztreamer".into(), password: Secret::new("hunter2") };
        let debug = format!("{auth:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(debug.contains("[redacted]"), "{debug}");
        assert!(auth.header().unwrap().unwrap().is_sensitive());
        assert!(!format!("{:?}", read_cookie_from("__cookie__:s3cret")).contains("s3cret"));
    }

    fn read_cookie_from(line: &str) -> Secret {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        std::fs::write(&path, line).unwrap();
        read_cookie(&path).unwrap()
    }
}
