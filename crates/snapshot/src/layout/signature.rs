//! `<file>.sig`: the detached signature of a file.

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};

/// A detached signature. The file `<file>.sig` holds the Ed25519 signature of the bytes of
/// `<file>`: 64 bytes in hex.
///
/// ```
/// use ed25519_dalek::SigningKey;
/// use ztreamer_snapshot::layout::SignatureFile;
///
/// let key = SigningKey::from_bytes(&[7; 32]);
/// let file = SignatureFile::sign(b"manifest", &key).to_hex();
/// let signature = SignatureFile::try_from(file.as_bytes()).unwrap();
/// assert!(signature.verify(b"manifest", &key.verifying_key()).is_ok());
/// assert_eq!(SignatureFile::path_of("manifests/a.json"), "manifests/a.json.sig");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignatureFile(Signature);

impl SignatureFile {
    /// The end of the path of a signature file.
    pub const SUFFIX: &str = ".sig";

    /// The signature of `bytes` with `key`.
    pub fn sign(bytes: &[u8], key: &SigningKey) -> Self {
        Self(key.sign(bytes))
    }

    /// The path of the signature file for the file at `path`: `path`, then [`Self::SUFFIX`].
    pub fn path_of(path: &str) -> String {
        format!("{path}{}", Self::SUFFIX)
    }

    /// The contents of the signature file: the signature in hex.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0.to_bytes())
    }

    /// Verifies that this signature signs `bytes` with `key`.
    pub fn verify(&self, bytes: &[u8], key: &VerifyingKey) -> Result<(), SignatureFileError> {
        key.verify_strict(bytes, &self.0).map_err(|_| SignatureFileError::Mismatch)
    }
}

impl TryFrom<&[u8]> for SignatureFile {
    type Error = SignatureFileError;

    /// Decodes the contents of a signature file. The hex can have ASCII whitespace before and
    /// after it.
    fn try_from(file: &[u8]) -> Result<Self, Self::Error> {
        let bytes = hex::decode(file.trim_ascii()).map_err(|_| SignatureFileError::Encoding)?;
        Signature::from_slice(&bytes).map(Self).map_err(|_| SignatureFileError::Encoding)
    }
}

/// Why a reader refused a [`SignatureFile`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SignatureFileError {
    /// The file is not 64 bytes in hex.
    #[error("the signature file is not 64 bytes in hex")]
    Encoding,
    /// The signature does not verify with the key of the reader.
    #[error("the signature does not verify")]
    Mismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signature_file_verifies_its_bytes_with_its_key_only() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let file = format!("{}\n", SignatureFile::sign(b"manifest", &key).to_hex());
        let signature = SignatureFile::try_from(file.as_bytes()).unwrap();

        assert_eq!(signature.verify(b"manifest", &key.verifying_key()), Ok(()));
        let other = SigningKey::from_bytes(&[8; 32]).verifying_key();
        assert_eq!(signature.verify(b"manifest", &other), Err(SignatureFileError::Mismatch));
        let error = signature.verify(b"other bytes", &key.verifying_key());
        assert_eq!(error, Err(SignatureFileError::Mismatch));
        for file in [&file[..126], "zz"] {
            let error = SignatureFile::try_from(file.as_bytes());
            assert_eq!(error, Err(SignatureFileError::Encoding));
        }
    }
}
