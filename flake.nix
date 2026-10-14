{
  description = "ztreamer — Zcash indexer and CompactTxStreamer";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      eachSystem = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      zakuraHash = "sha256-1GNbFnmRl1fALD8wo9BWVjCHqsKx5UvQ1hLJfzK5k7c=";
    in
    {
      packages = eachSystem (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = "ztreamerd";
          version = "0.0.1";
          src = pkgs.lib.cleanSource ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = pkgs.lib.genAttrs [
              "zakura-1.7.0-rc0"
              "zakura-chain-10.0.0-rc0"
              "zakura-consensus-11.0.0-rc0"
              "zakura-header-chain-4.1.0-rc0"
              "zakura-jsonl-trace-1.2.0"
              "zakura-mmr-tree-0.0.1"
              "zakura-network-10.0.0-rc0"
              "zakura-node-services-5.0.0-rc0"
              "zakura-quic-0.1.0"
              "zakura-rpc-13.0.0-rc0"
              "zakura-script-5.0.0-rc0"
              "zakura-state-11.0.0-rc0"
              "zakura-test-3.0.0-rc0"
              "zakura-tower-batch-control-1.4.0-rc0"
              "zakura-tower-fallback-1.3.0-rc0"
            ] (_: zakuraHash);
          };
          nativeBuildInputs = with pkgs; [
            protobuf
            pkg-config
            cmake
            rustPlatform.bindgenHook
          ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          doCheck = false;
          meta.mainProgram = "ztreamerd";
        };
      });

      devShells = eachSystem (pkgs: {
        default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            rustc
            cargo
            rustfmt
            clippy
            git
            protobuf
            pkg-config
            cmake
            rustPlatform.bindgenHook
          ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
      });
    };
}
