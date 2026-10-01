# Ferry (`cfry`)

Ferry turns Tor byte streams into complete bounded messages. Its home is
`cmsh → ctrn → cfry`. See [the contract](docs/CONTRACT.md).

## Scope

Own framing, partial reads, backpressure and stream lifecycle below the Tor
backend. Reuse maintained codecs and stream multiplexing. Never interpret
profiles, messages, delivery receipts, record policy or cryptographic authority;
never select a network or retain application payloads.

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
