//! Lightwallet protobuf types and Ztreamer's Zakura p2p protocol definitions.

mod encoded;
pub mod p2p;
pub use encoded::EncodedCompactBlock;

/// Server bindings that can send a previously encoded CompactBlock.
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/wire/cash.z.wallet.sdk.rpc.rs"));
}

/// Compact messages whose byte fields share one owned decode buffer.
pub mod compact_bytes {
    include!(concat!(env!("OUT_DIR"), "/compact_bytes/cash.z.wallet.sdk.rpc.rs"));
}

/// Generated Lightwallet protocol types and service descriptors.
pub mod proto {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("lightwalletd_descriptor");
}
