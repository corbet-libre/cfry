# Ferry browser integration contract (`cfry`)

Scope supplement to [the contract](CONTRACT.md) and [the readme](../README.md).
Consumers: `ctrn`/`cmsg`. No new protocol; no crypto, circuits, keys, or network selection.

## What is real here versus what Tor still must prove

- Maintained in this repo: yamux multiplexing plus `tokio-util`
  length-delimited framing over caller-supplied in-memory byte streams.
  Configured native and Wasm vectors exercise that implementation over
  `tokio::io::duplex` (see [tests/messages.rs](../tests/messages.rs),
  [tests/streams.rs](../tests/streams.rs)); they state no passing result here.
- Tor transport qualification is separate. Real onion native/browser
  integration is gated independently by the Tor backend (`ctrn`); configured
  gates below state no result, and nothing here claims that integration works.

## Entrypoints consumed by `ctrn`/`cmsg`

- [`Peer::new`](../src/message.rs) takes `io: BoxStream, role: Role`,
  `clock: Arc<dyn Clock>`, `limits: MessageLimits` and returns
  `(Peer, BoxFuture<'static, ()>)`. No runtime is assumed; the caller must pump
  the driver future: `tokio::spawn` natively,
  `wasm_bindgen_futures::spawn_local` in the browser (pattern in use in
  [tests/messages.rs](../tests/messages.rs)).
- [`Session::new`](../src/session.rs) takes `io: BoxStream, role: Role` and
  returns `(Session, BoxFuture<'static, ()>)` with `open`/`accept`/`close` and a
  one-shot terminal [`SessionEvent::Closed`](../src/session.rs); later
  `next_event` returns `None`.
- Wasm binding surface is [`BrowserFrameCodec`](../src/browser.rs), exported as
  JS `FrameCodec` with `new(max_frame_bytes)`, `encode(payload)`,
  `push(chunk)`, `finish()`, `close()`; it exists only on `wasm32`
  (see [src/lib.rs](../src/lib.rs)). No `Peer`/`Session` Wasm export exists.
- Externally driven framing without I/O is [`FrameCodec`](../src/frame.rs)
  with the same `new`/`encode`/`push`/`finish`/`close` shape; the browser
  adapter [`browser/streams.mjs`](../browser/streams.mjs) constructs
  `FramedStream` with `{ maximumBytes, timeoutMs, clock }` (`clock` defaults
  to `performance.now`) over an injected `io` port of
  `read(maximum, deadline)`, `write(bytes, deadline)`, `close()`. The provider
  must cancel pending I/O when closed; deadlines are whole-frame budgets from
  the injected monotonic clock.

## Configured bounds (`ctrn`/`cmsg` inputs)

- Frames: [`MAX_FRAME_BYTES = 1 MiB`](../src/frame.rs); `FrameCodec` bound is
  `1..=MAX_FRAME_BYTES`; one `push` chunk is at most `max + 4` bytes.
- Messages: [`MessageLimits`](../src/message.rs) requires
  `1 <= max_payload <= MAX_FRAME_BYTES - 16` and `1 <= timeout_ms <= 60000`;
  wire frame bound is `max_payload + 16`; `validate()` checks without I/O.
- Streams: [`MAX_SUBSTREAMS = 64`](../src/session.rs), inbound accept queue 16
  (excess reset), yamux receive window `64 * DEFAULT_CREDIT` (16 MiB total).
  [`Limits.max_frame_bytes`](../src/streams.rs) is `1..=MAX_FRAME_BYTES`,
  enforced by [`frame::codec`](../src/frame.rs);
  [`Limits.timeout_ms`](../src/streams.rs) is `1..=60000`, enforced by
  [`FramedStream::open`](../src/streams.rs).

## Message vs call/reply, one-use reply, lifetime

- Full detail already covered in [the contract](CONTRACT.md); this doc only
  binds the browser-relevant surface:
  [`app_message`](../src/message.rs) (one-way plus yamux FIN exchange),
  [`app_call`](../src/message.rs) (one request, one bounded opaque reply, never
  retried), [`next`](../src/message.rs) yielding
  [`Received::Message / Received::Call`](../src/message.rs).
- [`Reply::send`](../src/message.rs) is exactly-once on the original stream;
  `Reply` is nonserializable, keeps its `Session` alive after the listener
  `Peer` is dropped, and drop/cancel cannot replay it. An extra reply frame to
  a one-way message fails with `Protocol` (vector in
  [tests/messages.rs](../tests/messages.rs)).
- Stream ending, per [src/streams.rs](../src/streams.rs) and
  [src/message.rs](../src/message.rs): `finish`/`finish_writing` run the
  maintained yamux FIN exchange before releasing the stream; `cancel` drops
  buffered bytes and reports `Failed` without retry. Local send completion
  never implies receipt or durable acceptance.

## Cancellation, backpressure, uncertain sends

- Per [src/streams.rs](../src/streams.rs): each operation takes the owned I/O
  and reports `Failed` while in flight; dropping that future drops the I/O, so
  an uncertain partial send or read can never be retried on the stream.
  `cancel()` drops buffered bytes and reports `Failed`; `finish()` on success
  reports `Closed`. Clock regression or expiry reports `Failed`; malformed,
  truncated or oversized input never becomes a message
  (see [the contract](CONTRACT.md)). The
  `graceful_close_waits_for_real_byte_backpressure_to_drain` vector in
  [tests/messages.rs](../tests/messages.rs) configures a close that waits for
  byte backpressure to drain; it states no result here.

## `cfg` and dependency differences

- `BoxFuture` is `Send` natively, `!Send` local future on `wasm32`;
  `MaybeSend`/`MaybeSync` bind `Send`/`Sync` natively and nothing on `wasm32`
  (see [src/lib.rs](../src/lib.rs)). Browser drivers must use `spawn_local`.
- `wasm-bindgen`/`js-sys` are `wasm32`-only; `wasm-bindgen-futures` is a
  `wasm32`-only dev-dependency; native tests use `tokio` with
  `rt-multi-thread`, `macros`, `time`, `test-util` (see
  [Cargo.toml](../Cargo.toml)).

## Configured gates (not executed here) and known selector gaps

- Configured in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml),
  none executed here: `cargo fmt --all --check`,
  `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`,
  `cargo test --locked --workspace --all-features`,
  `cargo check --locked --target wasm32-unknown-unknown --lib`, Wasm vectors
  via `cargo test --locked --target wasm32-unknown-unknown --test streams
  --test messages` under `wasm-bindgen-test-runner`, and nightly `cargo
  llvm-cov` 100% line/branch gates over non-test sources (native measurement
  only; Wasm vectors run separately).
- Known browser selector gaps (CI unchanged): the Wasm gate selects only
  `--test streams --test messages`, so
  [`tests/support/frame_unit.rs`](../tests/support/frame_unit.rs) (`#[test]`
  only, wired via `src/frame.rs`) has no Wasm execution; the paused-clock
  `idle_peer_cannot_hold_the_listener_forever` vector is
  `#[cfg(not(target_arch = "wasm32"))]` native-only; and
  [`browser/streams.mjs`](../browser/streams.mjs) has no JS runner wired in CI.
