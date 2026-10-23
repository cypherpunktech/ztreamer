# ztreamer

Zcash indexer and light-wallet server with an embedded Zakura node. Cargo
workspace: `bin/ztreamerd` plus `crates/{indexer,node,protocol,service}`.
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
- Doc every public item. Include a usage example where it helps.
  Traits get an implementation guide.

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
- Unit tests in `mod tests` at the bottom of the file. Integration tests in
  `tests/`. Benchmarks in `benches/`. Shared fixtures in `ztreamer_indexer::test_utils`,
  behind the `test-utils` feature.
