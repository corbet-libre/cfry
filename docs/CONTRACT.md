# Ferry contract

The Tor backend supplies owned reliable byte I/O and an injected monotonic clock.
Ferry bounds each complete message, operation deadline, pending operation and
inbound queue. Partial input never becomes an application message. Cancellation
or uncertain partial I/O closes the affected stream and never retries an effect.
Local send completion is not a recipient receipt or durable acceptance.

Ferry executes transport mechanics only. Mesh selects networks; the application
owner authenticates and interprets payloads. No Tor framing is required from a
native message backend such as Veilid.

## Internal invariants

The maintained tokio-util encoder can refuse an excessive length or length
adjustment overflow. Ferry checks the payload bound first and always configures
zero adjustment; encoding then asserts that invariant. The source is
[tokio-util LengthDelimitedCodec](https://docs.rs/tokio-util/latest/src/tokio_util/codec/length_delimited.rs.html).
No production source region is excluded from coverage. The session command
queue is bounded by outstanding-operation permits; it does not duplicate that
same bound as an unreachable second refusal branch. Live substream capacity
remains enforced by maintained yamux and is tested separately.

Yamux receive credit is bounded to the 64-stream limit times its maintained default per-stream credit (16 MiB total). The adapter does not retain the upstream 1 GiB default connection window.


Queued commands own their capacity permit even after callers cancel. A cancelled
reply is discarded only when the driver consumes its command; cancellation cannot
free capacity while leaving unbounded commands behind.

Ferry's outbound-open path uses the maintained future adapter to assert yamux's
synchronous result under this configuration. Its 64-stream limit is below the
upstream 256-unacknowledged-stream wait condition; error Cleanup always returns
Ready after closing/draining stream receivers. Ferry enters poll_close only in
its terminal path and never opens another stream afterward. Thus no deferred-open
queue state is needed for Pending. The assertions make upstream drift fail visibly;
no production coverage exclusion is used. Sources: maintained
[Connection](https://docs.rs/yamux/latest/src/yamux/connection.rs.html) and
[Cleanup](https://github.com/paritytech/yamux/blob/master/yamux/src/connection/cleanup.rs).


Coverage gates every LLVM-emitted source line and branch in the same-execution
LCOV report. The JSON export and annotated source remain available, including
instantiation diagnostics. LLVM's JSON summary selects generic instantiations
in ways that can report a missed branch even when another instantiation covered
the same source branch; this gate explicitly measures source coverage, not every
generic instantiation. No line or branch is excluded and both targets remain 100%.
Native measurement does not substitute for the separately executed Wasm vectors.


Complete-message send/reply keeps the byte session alive through the existing
yamux FIN exchange. A stream flush queues yamux commands; closing the byte peer
immediately could make a delayed receiver fail on its initial control write
before reading the queued payload. This uses maintained yamux half-close, adds no
receipt format and never claims application processing or durable delivery. A
malformed extra response to a one-way message fails; timeout/cancellation closes
without replay. The controlled delayed-runtime vector reproduces the original
failure and runs natively and in Wasm. Real Tor independently gates integration.
