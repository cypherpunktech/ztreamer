# This file builds ztreamerd and puts it in a small image:
#
#     docker build -t ztreamer .
#
# The build runs on all x86-64 and aarch64 CPUs. To make a build for the CPU type of the build
# host only, add: --build-arg RUSTFLAGS="-C target-cpu=native"

ARG RUST_VERSION=1.99

FROM docker.io/library/rust:${RUST_VERSION}-bookworm AS build

# protoc compiles the protobuf definitions. libclang generates the RocksDB bindings of zakura.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends libclang-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

ARG PROFILE=maxperf
ARG RUSTFLAGS=""
ENV RUSTFLAGS=${RUSTFLAGS}
# The cache mounts keep the dependencies and the build outputs between builds. The binary goes
# outside the cache. The runtime stage copies it from there.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --locked --profile "${PROFILE}" --package ztreamerd \
    && install -D "target/${PROFILE}/ztreamerd" /out/usr/local/bin/ztreamerd \
    && install -d /out/var/lib/ztreamer

# distroless/cc has glibc, libstdc++ for RocksDB, and the CA certificates for https snapshots. It
# has no shell and no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /out/usr/local/bin/ztreamerd /usr/local/bin/ztreamerd
COPY --from=build --chown=65532:65532 /out/var/lib/ztreamer /var/lib/ztreamer

# This directory holds the index (ztreamer-index). In the embedded mode, it also holds the zakura
# state (.cache/zakura).
ENV HOME=/var/lib/ztreamer
WORKDIR /var/lib/ztreamer
VOLUME /var/lib/ztreamer

# gRPC, Prometheus metrics, and the zakura P2P listener of the embedded mode.
EXPOSE 9067 9999 8233

ENTRYPOINT ["/usr/local/bin/ztreamerd"]
# The arguments of `docker run` replace this list. Keep the listen flags. Without them, ztreamerd
# binds to loopback, and only the container can connect.
CMD ["--grpc-listen", "0.0.0.0:9067", "--metrics-listen", "0.0.0.0:9999"]
