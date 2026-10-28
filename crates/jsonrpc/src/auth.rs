//! Credentials for the node's JSON-RPC listener.

use std::{
    fmt, io,
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
    /// Builds the `Authorization` header value and reads the cookie file if there is one. The
    /// read blocks the thread. Use this function only in sync code, such as [`RpcClient::new`].
    ///
    /// [`RpcClient::new`]: crate::RpcClient::new
    pub(crate) fn header_blocking(&self) -> Result<Option<HeaderValue>, RpcError> {
        match self {
            Self::None => Ok(None),
            Self::Cookie(path) => Ok(Some(CookieFile(path).read_blocking()?.basic_header())),
            Self::Basic { user, password } => Self::user_password_header(user, password).map(Some),
        }
    }

    /// Builds the `Authorization` header value and reads the cookie file if there is one. The
    /// read does not block the Tokio worker.
    pub(crate) async fn header(&self) -> Result<Option<HeaderValue>, RpcError> {
        match self {
            Self::None => Ok(None),
            Self::Cookie(path) => Ok(Some(CookieFile(path).read().await?.basic_header())),
            Self::Basic { user, password } => Self::user_password_header(user, password).map(Some),
        }
    }

    pub(crate) const fn is_cookie(&self) -> bool {
        matches!(self, Self::Cookie(_))
    }

    /// Builds the basic auth header of [`Self::Basic`].
    fn user_password_header(user: &str, password: &Secret) -> Result<HeaderValue, RpcError> {
        if user.contains(':') {
            return Err(RpcError::Config("basic auth user must not contain ':'".into()));
        }
        Ok(basic(&format!("{user}:{}", password.expose())))
    }
}

/// A zakura cookie file.
///
/// The file holds one line, `__cookie__:<secret>`. The node compares only the part after the
/// colon, so the whole line is what goes into the basic auth header.
struct CookieFile<'a>(&'a Path);

impl CookieFile<'_> {
    /// Reads the file and returns its `user:secret` line. The read blocks the thread.
    fn read_blocking(&self) -> Result<Secret, RpcError> {
        self.parse(std::fs::read_to_string(self.0))
    }

    /// Reads the file and returns its `user:secret` line. The read does not block the Tokio
    /// worker.
    async fn read(&self) -> Result<Secret, RpcError> {
        self.parse(tokio::fs::read_to_string(self.0).await)
    }

    /// Checks the result of a read and returns the `user:secret` line.
    fn parse(&self, contents: io::Result<String>) -> Result<Secret, RpcError> {
        let contents = contents.map_err(|error| {
            RpcError::Config(format!("cannot read cookie file {}: {error}", self.0.display()))
        })?;
        let line = contents.trim();
        if line.is_empty() || !line.contains(':') {
            return Err(RpcError::Config(format!(
                "cookie file {} does not hold a user:secret line",
                self.0.display()
            )));
        }
        Ok(Secret::new(line))
    }
}

impl Secret {
    /// Builds the basic auth header of a cookie line.
    fn basic_header(&self) -> HeaderValue {
        basic(self.expose())
    }
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
        let header = Auth::Cookie(path).header_blocking().unwrap().unwrap();
        assert_eq!(
            header.to_str().unwrap(),
            format!("Basic {}", STANDARD.encode(b"__cookie__:s3cret"))
        );
    }

    #[test]
    fn cookie_file_must_exist_and_have_a_colon() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(matches!(Auth::Cookie(missing).header_blocking(), Err(RpcError::Config(_))));
        let bad = dir.path().join("bad");
        std::fs::write(&bad, "nocolon").unwrap();
        assert!(matches!(Auth::Cookie(bad).header_blocking(), Err(RpcError::Config(_))));
    }

    #[tokio::test]
    async fn async_header_matches_blocking_header() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join(".cookie");
        std::fs::write(&good, "__cookie__:s3cret\n").unwrap();
        let bad = dir.path().join("bad");
        std::fs::write(&bad, "nocolon").unwrap();
        let cases = [
            Auth::None,
            Auth::Cookie(good),
            Auth::Cookie(bad),
            Auth::Cookie(dir.path().join("missing")),
            Auth::Basic { user: "ztreamer".into(), password: Secret::new("pw") },
            Auth::Basic { user: "a:b".into(), password: Secret::new("p") },
        ];
        for auth in cases {
            let blocking = auth.header_blocking().map_err(|error| error.to_string());
            let non_blocking = auth.header().await.map_err(|error| error.to_string());
            assert_eq!(blocking, non_blocking, "{auth:?}");
        }
    }

    #[test]
    fn basic_auth_rejects_colon_in_user() {
        let auth = Auth::Basic { user: "a:b".into(), password: Secret::new("p") };
        assert!(matches!(auth.header_blocking(), Err(RpcError::Config(_))));
        assert!(Auth::None.header_blocking().unwrap().is_none());
    }

    #[test]
    fn secrets_stay_out_of_debug_output() {
        let auth = Auth::Basic { user: "ztreamer".into(), password: Secret::new("hunter2") };
        let debug = format!("{auth:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(debug.contains("[redacted]"), "{debug}");
        assert!(auth.header_blocking().unwrap().unwrap().is_sensitive());
        assert!(!format!("{:?}", read_cookie_from("__cookie__:s3cret")).contains("s3cret"));
    }

    fn read_cookie_from(line: &str) -> Secret {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        std::fs::write(&path, line).unwrap();
        CookieFile(&path).read_blocking().unwrap()
    }
}
