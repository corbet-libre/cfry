# cfry — agent instructions

Work in a lane, never in ~/agents/scratch/ws. From the repo dir:
`cargo fmt --all --check`,
`cargo clippy --locked --all-targets --all-features -- -D warnings`,
`cargo test --locked --all-features`,
`cargo check --locked --target wasm32-unknown-unknown --lib`,
`CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner cargo test
--locked --target wasm32-unknown-unknown --test it`.
Coverage gate needs 100% of every LLVM-emitted source line + branch in one
execution (.github/test-source-coverage.py, check-source-coverage.py).
publish=false; no registry publication.

Invariants (rules: docs/CONTRACT.md):
- Partial input never becomes a message; truncation at EOF is Protocol failure
  (tests/it/streams.rs:130,155).
- Payload bound checked before allocation; oversize refuses with Limit and emits
  nothing (tests/it/streams.rs:169).
- Cancellation/uncertain partial I/O closes the stream, never retries an effect
  (tests/it/streams.rs:184,337).
- Local send completion is not a receipt (tests/it/messages.rs:39).
- Command queue bounded by outstanding-operation permits; cancelled commands keep
  theirs until the driver consumes them (tests/it/messages.rs:189,265).
- Live substreams obey yamux capacity: 64 streams, 16 MiB receive credit (:224).
- Complete-message send keeps the session alive through yamux half-close so the
  queued payload drains (:142,331,403).

Pointers: docs/CONTRACT.md, tests/it/streams.rs, tests/it/messages.rs,
tests/support/frame_unit.rs.
