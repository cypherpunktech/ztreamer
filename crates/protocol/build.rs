//! Compiles the Lightwallet protobuf definitions and the file descriptor set.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("lightwalletd_descriptor.bin"),
        )
        .compile_protos(
            &["proto/lightwalletd/compact_formats.proto", "proto/lightwalletd/service.proto"],
            &["proto/lightwalletd"],
        )?;
    let wire_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("wire");
    std::fs::create_dir_all(&wire_dir)?;
    // Reuse the ordinary request/response messages, substituting only the compact
    // block response. The RPC paths and protobuf schema remain identical.
    tonic_prost_build::configure()
        .build_client(false)
        .out_dir(wire_dir)
        .extern_path(".cash.z.wallet.sdk.rpc", "crate::proto")
        .extern_path(".cash.z.wallet.sdk.rpc.CompactBlock", "crate::EncodedCompactBlock")
        .compile_protos(&["proto/lightwalletd/service.proto"], &["proto/lightwalletd"])?;
    let bytes_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("compact_bytes");
    std::fs::create_dir_all(&bytes_dir)?;
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .out_dir(bytes_dir)
        .bytes(".")
        .compile_protos(&["proto/lightwalletd/compact_formats.proto"], &["proto/lightwalletd"])?;
    Ok(())
}
