# Codec vectors (browser JS boundary)

`browser/codec-contract.mjs` checks the JS boundary of the actual Wasm
`FrameCodec` from `./pkg/cfry.js`: f64 constructor validation, subarray
encode, push ownership, empty/NUL/high-byte frames, truncated `finish()`,
and limit-overflow close. It holds no mock codec and no framing copy.

Caller initializes the real Wasm module first using the `browser/pkg/`
build's own initializer, then calls `runCodecContract()`.

Selection: split/coalesced/partial and allocator bounds stay in the Rust
vectors ([streams](../tests/streams.rs),
[frame_unit](../tests/support/frame_unit.rs)); this module covers only the
JS numeric/array/copy/error edge. Sources: [browser.rs](../src/browser.rs),
[frame.rs](../src/frame.rs), [streams.mjs](./streams.mjs),
[contract](../docs/CONTRACT.md).

Configured vs executed: this change is statically reviewed only
(`git diff --check`); nothing was run locally. Real browser execution
belongs to the owner: CI runs the wasm-bindgen vectors but does not build
`browser/pkg/` or execute `browser/*.mjs`, so that wiring is still open.
No CI edits are made here.
