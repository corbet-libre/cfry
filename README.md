# Ferry (`cfry`)

Ferry turns Tor byte streams into complete bounded messages. Its home is
`cmsh → ctrn → cfry`. See [the contract](docs/CONTRACT.md).

## Scope

**Purpose and home.** Turn Tor byte streams into complete, size-bounded messages
under `cmsh → ctrn → cfry`. Veilid speaks messages natively and does not need Ferry.

**Owns.** Framing, partial reads, backpressure and stream lifecycle using maintained
length-delimited codecs and yamux. Byte I/O and a monotonic clock enter; bounded
messages and one-use replies leave.

**Never.** Interpret application content, authority or record policy; select a
network; retain application payloads; retry an effect after cancellation or
uncertain partial I/O.

**States and invariants.** Opening → Open → Draining → Closed. Malformed input,
deadline, cancellation or I/O failure closes the affected stream. Partial frames
never become messages; local completion never claims recipient acceptance.

**Test obligations.** Native and actual Wasm split/coalesced/partial frames, bounds
before allocation, half-close/cancellation cleanup and controlled alternative byte
I/O. Slow peers stay within 64 streams and 16 MiB receive credit. Tor independently
proves real native/browser onion integration. Both reachable line and branch
coverage target 100% without production exclusions.

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
