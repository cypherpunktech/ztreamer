# Builds ztreamerd and packs it into a small image:
#
#     docker build -t ztreamer .
#
# The build is portable to every x86-64 and aarch64 CPU. For a build that runs only on CPUs like
# the build host's: --build-arg RUSTFLAGS="-C target-cpu=native".

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
# outside the cache, where the runtime stage can copy it.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --locked --profile "${PROFILE}" --package ztreamerd \
    && install -D "target/${PROFILE}/ztreamerd" /out/usr/local/bin/ztreamerd \
    && install -d /out/var/lib/ztreamer

# distroless/cc has glibc, libstdc++ (RocksDB) and the CA certificates (https snapshots). It has
# no shell and no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /out/usr/local/bin/ztreamerd /usr/local/bin/ztreamerd
COPY --from=build --chown=65532:65532 /out/var/lib/ztreamer /var/lib/ztreamer

# The index (ztreamer-index) and, in the embedded mode, the zakura state (.cache/zakura) are here.
ENV HOME=/var/lib/ztreamer
WORKDIR /var/lib/ztreamer
VOLUME /var/lib/ztreamer

# gRPC, Prometheus metrics, and the zakura P2P listener of the embedded mode.
EXPOSE 9067 9999 8233

ENTRYPOINT ["/usr/local/bin/ztreamerd"]
# The arguments of `docker run` replace this list. Keep the listen flags: the defaults bind to
# loopback, which only the container reaches.
CMD ["--grpc-listen", "0.0.0.0:9067", "--metrics-listen", "0.0.0.0:9999"]
