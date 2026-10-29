//! The URL of a remote zakura node, with the credentials of its userinfo part as [`Auth`].

use std::{net::IpAddr, str::FromStr};

use http::{
    Uri,
    uri::{InvalidUri, InvalidUriParts},
};
use percent_encoding::percent_decode_str;
use ztreamer_jsonrpc::{Auth, Secret};

/// Why a node URL is not valid.
#[derive(Debug, thiserror::Error)]
pub enum NodeUrlError {
    /// The URL, or its part without the userinfo, is not a valid URI.
    #[error(transparent)]
    Uri(#[from] InvalidUri),
    /// The URL without its userinfo part does not make a valid URI.
    #[error(transparent)]
    UriParts(#[from] InvalidUriParts),
    /// The userinfo part has no `:` between the user and the password.
    #[error("the userinfo part of the URL needs user:password")]
    NoPassword,
    /// The percent-decoded user or password is not UTF-8.
    #[error("the userinfo part of the URL is not UTF-8")]
    NotUtf8,
}

/// The URL of a remote zakura node. The credentials of its userinfo part
/// (`https://user:password@host:8232`) go into [`Auth::Basic`]. Thus the URL that ztreamerd keeps
/// and logs has no credentials, and the `Debug` output does not show the password.
///
/// ```
/// use ztreamer_remote::{Auth, NodeUrl, Secret};
///
/// let url: NodeUrl = "https://user:hunter2@node.example:8232".parse().unwrap();
/// assert_eq!(url.uri(), "https://node.example:8232/");
/// let basic = Auth::Basic { user: "user".into(), password: Secret::new("hunter2") };
/// assert_eq!(url.credentials(), Some(&basic));
/// ```
#[derive(Clone, Debug)]
pub struct NodeUrl {
    /// The URL without its userinfo part.
    uri: Uri,
    /// [`Auth::Basic`] from the userinfo part, if the URL has one.
    credentials: Option<Auth>,
}

impl NodeUrl {
    /// The URL without its userinfo part.
    pub const fn uri(&self) -> &Uri {
        &self.uri
    }

    /// [`Auth::Basic`] from the userinfo part, if the URL has one.
    pub const fn credentials(&self) -> Option<&Auth> {
        self.credentials.as_ref()
    }

    /// True when the host is `localhost` or a loopback address.
    pub fn is_loopback(&self) -> bool {
        self.uri.host().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
    }
}

impl FromStr for NodeUrl {
    type Err = NodeUrlError;

    /// Parses the URL and moves `user:password` from the userinfo part into [`Auth::Basic`]. It
    /// percent-decodes the user and the password.
    fn from_str(url: &str) -> Result<Self, NodeUrlError> {
        let uri: Uri = url.parse()?;
        let Some((userinfo, host)) = uri
            .authority()
            .and_then(|authority| authority.as_str().rsplit_once('@'))
            .map(|(userinfo, host)| (userinfo.to_owned(), host.to_owned()))
        else {
            return Ok(Self { uri, credentials: None });
        };
        let (user, password) = userinfo.split_once(':').ok_or(NodeUrlError::NoPassword)?;
        let decode = |part: &str| {
            percent_decode_str(part)
                .decode_utf8()
                .map(String::from)
                .map_err(|_| NodeUrlError::NotUtf8)
        };
        let credentials =
            Auth::Basic { user: decode(user)?, password: Secret::new(decode(password)?) };
        let mut parts = uri.into_parts();
        parts.authority = Some(host.parse()?);
        Ok(Self { uri: Uri::from_parts(parts)?, credentials: Some(credentials) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_in_the_url_become_basic_auth() {
        let url: NodeUrl = "https://us%40er:hunter:2@node.example:8232/".parse().unwrap();
        assert_eq!(url.uri, "https://node.example:8232/");
        let expected = Auth::Basic { user: "us@er".into(), password: Secret::new("hunter:2") };
        assert_eq!(url.credentials, Some(expected));
        assert!(!format!("{url:?}").contains("hunter"));
        assert!(!url.is_loopback());

        let url: NodeUrl = "http://[::1]:8232".parse().unwrap();
        assert_eq!(url.credentials, None);
        assert!(url.is_loopback());
        assert!("http://user@127.0.0.1:8232".parse::<NodeUrl>().is_err());
    }
}
