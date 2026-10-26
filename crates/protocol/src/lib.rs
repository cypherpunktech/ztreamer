//! Lightwallet protobuf types, the Replicator service, and Ztreamer's Zakura p2p protocol
//! definitions.

// Public items are not fully documented yet. Remove this once every `pub` item has a doc comment.
#![allow(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod encoded;
pub mod p2p;
pub use encoded::EncodedCompactBlock;

/// Server bindings that can send a previously encoded CompactBlock.
#[allow(missing_docs, clippy::missing_const_for_fn)] // Generated code.
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/wire/cash.z.wallet.sdk.rpc.rs"));
}

/// Compact messages whose byte fields share one owned decode buffer.
#[allow(missing_docs, clippy::missing_const_for_fn)] // Generated code.
pub mod compact_bytes {
    include!(concat!(env!("OUT_DIR"), "/compact_bytes/cash.z.wallet.sdk.rpc.rs"));
}

/// Generated Lightwallet protocol types and service descriptors.
#[allow(missing_docs, clippy::missing_const_for_fn)] // Generated code.
pub mod proto {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");

    /// Serialized file descriptor set for gRPC reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("lightwalletd_descriptor");
}

/// Generated Replicator service types: a ztreamer serves its sealed ranges to a replica.
#[allow(missing_docs, clippy::missing_const_for_fn)] // Generated code.
pub mod replicate {
    tonic::include_proto!("ztreamer.replicate");
}
