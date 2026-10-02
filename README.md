# Ferry (`cfry`)

Ferry turns Tor byte streams into complete bounded messages. Its home is
`cmsh → ctrn → cfry`. See [the contract](docs/CONTRACT.md).

## Scope

Purpose

Turns Tor byte streams into complete, size-bounded messages.

Owns

Framing, partial reads, backpressure, and stream lifecycle, built on maintained length-delimited framing and yamux. Byte I/O and a monotonic clock go in; bounded messages and one-use replies come out.

Never

Interprets profiles, messages, receipts, records, or authority; selects a network; keeps payloads; retries an effect after cancellation or uncertain partial I/O. Veilid speaks messages natively and does not need this library.

States

Opening to Open to Draining to Closed. Malformed input, deadline, cancellation, or network error closes the affected stream as failed. Partial frames never become messages.

Test obligations

Split, coalesced, and partial reads reproduce frames on native and Wasm with identical vectors. Oversized lengths are rejected before allocation. Half-close and cancellation release resources. Slow peers stay within memory bounds of 64 streams and 16 MiB credit. The same suite runs against a controlled alternative byte adapter. Full line and branch coverage with real round trips and no mocks of this library's logic, injected delay, duplication, loss, cancellation, clock regression, and corruption, bounded parsing, queues, and work, errors that omit identifiers, plaintext, and secrets, reuse of maintained third-party code, and no own cryptography.

## Validation

Native and executed Wasm vectors, strict line and branch coverage, and the
Tor backend's independent real private-network and browser integrations.
A configured gate is not a passing result. No registry publication.

## Reuse

- `tokio-util::codec::LengthDelimitedCodec`: bounded length framing.
- `yamux`: maintained stream multiplexing and flow control.
- `futures-io`: runtime-independent I/O and cancellation.
- The existing Mesh framing/lifecycle and browser adapter moved here with their
  executed native/Wasm vectors; no crypto or network-selection algorithm added.
