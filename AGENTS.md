# ztreamer

Zcash indexer and light-wallet server with an embedded or remote Zakura node.
Cargo workspace: `bin/ztreamerd` plus
`crates/{backend,blocking,chain,import,index,jsonrpc,node,p2p,protocol,remote,replicator,service,snapshot,sync}`.
Building needs `protoc` on the path.

## Commands

- `just fmt` - format (nightly rustfmt)
- `just clippy` - lint, warnings are errors
- `just doc` - rustdoc, warnings are errors
- `just check` - fmt-check + clippy + doc, what CI runs before tests
- `just test` - test the workspace
- `cargo t -p <crate>` - test one crate

Never use `cargo check` or `cargo build` to validate; use clippy.

### Pre-push checks (enforced by Claude hook)

`.claude/settings.json` runs `.claude/hooks/pre-push.sh` before every
`git push`. The push is blocked if any check fails:

- `cargo +nightly fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`

## Lints

Lints live in `[workspace.lints]` in the root `Cargo.toml`; every crate
sets `[lints] workspace = true`. The one per-crate attribute is
`#![cfg_attr(not(test), warn(unused_crate_dependencies))]` at each crate
root, because dev-dependencies would trip it under `cfg(test)`. Do not add
other per-crate `#![warn]` blocks.
Thresholds live in `clippy.toml`. Format rules live in `rustfmt.toml`.

`missing_docs` is temporarily allowed at the crate root of `indexer`,
`protocol`, and `service`. Document public items as you touch them, and
remove the allow once a crate is clean.

Generated protobuf modules in `crates/protocol/src/lib.rs` carry a targeted
`#[allow]`. Do not widen it.

## Style

- Functional combinators over imperative control flow. Avoid nesting.
- `let Some(x) = opt else { return; };` for early returns.
- No glob imports outside `mod tests`. Group imports from the same crate.
- Private by default, `pub(crate)` for internal, `pub` for API only.
  Never `pub(super)`.
- `thiserror` in library crates, `eyre` in binaries, never `anyhow`.
- Builders for structs with more than four fields or several fields of the
  same type.
- Small, focused functions and types. No `TODO`s for core logic.
- A source file holds at most about 500 lines of non-test code. Split a larger file along
  the seams of its types.
- An inherent `impl Type` block is in the file that defines `Type`, also in tests. Never
  split the methods of one type over several files: a type that needs this has too many
  jobs, so split the type. A trait impl follows the orphan rule.
- Order the items in a file: module docs, `mod` declarations, `use` and `pub use`,
  constants, traits, types (each struct, enum or type alias followed by its impls), free
  functions, then `#[cfg(test)] mod tests`. A trait comes before every type and impl in its
  file, also the types that its signature uses: Rust needs no forward declaration.
- Doc every public item. Include a usage example where it helps.
  Traits get an implementation guide.
- A module with submodules or with a separate test file is `foo/mod.rs`, never `foo.rs` next to
  a `foo/` directory. Clippy `self_named_module_files` enforces it.

### Tracing

- Instrument work items, not long-lived tasks. `skip(self)` on methods.
- Levels: `trace` rare and verbose, `debug` sparingly, `info` default,
  `warn` potential issues, `error` prevents operation.
- Propagate spans across task boundaries with `Instrument`.

### Async

- Tokio. No blocking in async functions.
- Long-lived tasks return a spawnable future instead of running directly.

### Tests

- Tests panic. Never return `Result` from a test. `unwrap()` directly.
- Unit tests in `mod tests` at the bottom of the file. A test module over about 200 lines
  goes to its own file: the module is then `foo/mod.rs`, it ends with
  `#[cfg(test)] mod tests;`, and the tests are in `foo/tests.rs`. Integration tests in
  `tests/`. Benchmarks in `benches/`. Shared fixtures in the `test_utils` modules of
  `ztreamer_chain`, `ztreamer_index`, `ztreamer_remote` and `ztreamer_service`, behind the
  `test-utils` feature of each crate. `ztreamer_import::test_utils` is for the
  tests of that crate only.
- A module of shared test fixtures is named `test_utils`, also in a binary or inside a
  module. Never `test_support`.
