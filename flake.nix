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
    in
    {
      packages = eachSystem (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = "ztreamerd";
          version = "0.1.0";
          src = pkgs.lib.cleanSource ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
            # Fetch each git dependency at the rev that `Cargo.lock` pins. The
            # zakura crates then need no hash list that goes stale on each bump.
            allowBuiltinFetchGit = true;
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
