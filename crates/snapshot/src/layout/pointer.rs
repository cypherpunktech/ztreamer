//! `latest.json`: the signed pointer to the current manifest.

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::ManifestPath;

/// The pointer to the current manifest of a snapshot, in `latest.json`.
///
/// ```
/// use ed25519_dalek::SigningKey;
/// use ztreamer_snapshot::layout::{ManifestPath, ManifestPointer};
///
/// let key = SigningKey::from_bytes(&[7; 32]);
/// let manifest: ManifestPath = "manifests/2026-10-26T000000Z.json".parse().unwrap();
/// let pointer = ManifestPointer { manifest, sealed_through: 999 };
/// let json = pointer.to_signed_json(&key);
/// let read = ManifestPointer::from_signed_json(&json, &key.verifying_key()).unwrap();
/// assert_eq!(read, pointer);
/// ```
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestPointer {
    /// The path of the manifest.
    pub manifest: ManifestPath,
    /// The last height of the last range in the manifest.
    pub sealed_through: u32,
}

impl ManifestPointer {
    /// The path of the pointer, relative to the snapshot directory.
    pub const PATH: &str = "latest.json";

    /// The bytes of `latest.json`, signed with `key`.
    pub fn to_signed_json(&self, key: &SigningKey) -> Vec<u8> {
        let latest = serde_json::to_string(self).expect("a ManifestPointer always serializes");
        let signature = key.sign(latest.as_bytes()).to_bytes();
        let latest = RawValue::from_string(latest).expect("serde_json wrote valid JSON");
        serde_json::to_vec(&SignedPointer { latest: &latest, signature })
            .expect("a SignedPointer always serializes")
    }

    /// Reads the bytes of `latest.json` and verifies their signature with `key`, the key that
    /// the reader trusts.
    pub fn from_signed_json(bytes: &[u8], key: &VerifyingKey) -> Result<Self, PointerError> {
        let file: SignedPointer<'_> = serde_json::from_slice(bytes)?;
        key.verify_strict(file.latest.get().as_bytes(), &Signature::from_bytes(&file.signature))
            .map_err(|_| PointerError::Signature)?;
        Ok(serde_json::from_str(file.latest.get())?)
    }
}

/// The JSON of `latest.json`: the pointer and the signature of its exact bytes.
#[derive(Deserialize, Serialize)]
struct SignedPointer<'a> {
    #[serde(borrow)]
    latest: &'a RawValue,
    #[serde(with = "crate::json::hex_bytes")]
    signature: [u8; 64],
}

/// Why a reader refused `latest.json`.
#[derive(Debug, thiserror::Error)]
pub enum PointerError {
    /// The file does not parse. A file without a signature does not parse.
    #[error("latest.json is not valid")]
    Json(#[from] serde_json::Error),
    /// The signature does not verify with the trusted key.
    #[error("signature of latest.json does not verify")]
    Signature,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pointer() -> ManifestPointer {
        let manifest = "manifests/2026-10-26T000000Z.json".parse().unwrap();
        ManifestPointer { manifest, sealed_through: 2_999 }
    }

    #[test]
    fn a_signed_pointer_verifies_with_the_trusted_key_only() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = pointer().to_signed_json(&key);

        let latest = r#"{"manifest":"manifests/2026-10-26T000000Z.json","sealed_through":2999}"#;
        let signature = hex::encode(key.sign(latest.as_bytes()).to_bytes());
        let expected = format!(r#"{{"latest":{latest},"signature":"{signature}"}}"#);
        assert_eq!(String::from_utf8(bytes.clone()).unwrap(), expected);
        let read = ManifestPointer::from_signed_json(&bytes, &key.verifying_key()).unwrap();
        assert_eq!(read, pointer());
        let other = SigningKey::from_bytes(&[8; 32]).verifying_key();
        let error = ManifestPointer::from_signed_json(&bytes, &other).unwrap_err();
        assert!(matches!(error, PointerError::Signature), "{error:?}");
        let changed = String::from_utf8(bytes).unwrap().replace("2999", "3999");
        let error = ManifestPointer::from_signed_json(changed.as_bytes(), &key.verifying_key())
            .unwrap_err();
        assert!(matches!(error, PointerError::Signature), "{error:?}");
    }

    #[test]
    fn an_unsigned_pointer_is_refused() {
        let key = SigningKey::from_bytes(&[7; 32]).verifying_key();
        let unsigned = serde_json::json!({ "latest": pointer() }).to_string();

        let error = ManifestPointer::from_signed_json(unsigned.as_bytes(), &key).unwrap_err();

        assert!(matches!(error, PointerError::Json(_)), "{error:?}");
    }
}
