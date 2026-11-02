//! The layout of a snapshot directory: what `ztreamerd export-snapshot` writes and a replica
//! reads.
//!
//! ```text
//! latest.json                         ManifestPointer to the current manifest, signed
//! manifests/<time>.json               one entry per sealed range, at ManifestPath; never replaced
//! manifests/<time>.json.sig           SignatureFile of the manifest bytes
//! ranges/<start>-<digest prefix>.bin  range blob, at RangePath; never replaced
//! ```
//!
//! `<time>` is UTC, for example `2026-10-26T143005Z`.
//! `ztreamer_replicator::snapshot_writer::SnapshotWriter` writes the directory.
//!
//! `latest.json` is the only file that an export replaces. It holds its own signature. Thus a
//! reader never sees a new pointer with an old signature:
//!
//! ```json
//! {"latest":{"manifest":"manifests/2026-10-26T143005Z.json","sealed_through":1785999},
//!   "signature":"<hex>"}
//! ```
//!
//! The signature covers the exact bytes of the `latest` object. A reader verifies it with a key
//! that it trusts. The file does not hold the key: a source that can change the file can also
//! change a key in it.

mod path;
mod pointer;
mod signature;

pub use path::{ManifestPath, ManifestPathError, RangePath};
pub use pointer::{ManifestPointer, PointerError};
pub use signature::{SignatureFile, SignatureFileError};

/// The Ed25519 public key that signs the snapshots that a replica accepts.
///
/// This is the production key. The publishing host made its secret seed, and the seed does
/// not leave the host. A replica refuses a manifest or a `latest.json` that this key does not
/// verify. A peer source has no signature.
pub const SNAPSHOT_PUBKEY: [u8; 32] = [
    0xf8, 0x92, 0x06, 0x28, 0x05, 0xbc, 0xd4, 0xa6, 0xc8, 0x8f, 0x7f, 0x32, 0x88, 0xd2, 0xdd, 0xe2,
    0x48, 0x7d, 0x67, 0xf8, 0x77, 0x5b, 0x26, 0x32, 0xe8, 0x56, 0x0c, 0xbd, 0x48, 0xe3, 0xce, 0xa8,
];

#[cfg(test)]
mod tests {
    use ed25519_zebra::VerificationKey;

    use super::*;

    #[test]
    fn the_snapshot_key_is_a_valid_key() {
        VerificationKey::try_from(SNAPSHOT_PUBKEY).unwrap();
    }
}
