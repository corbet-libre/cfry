import { FramedStream } from './streams.mjs';

// JS-only constructor/clock option boundaries for the actual FramedStream.
// The caller initializes the real Wasm module first; this module never builds
// a stand-in codec or stream and never reimplements option validation.

const CLOSED = 'cfry:StreamClosed';
const MAX = 1024 * 1024;
const WATCH_MS = 5000;

const fail = (label) => {
  throw new Error(`cfry:Contract:${label}`);
};
const same = (got, want) =>
  got instanceof Uint8Array && got.length === want.length &&
  got.every((value, index) => value === want[index]);

// Finite test-only watchdog so a hung round trip fails in the owner harness.
// Watchdog faults carry cfry:Contract:Watchdog and never pose as StreamClosed.
async function watched(promise, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`cfry:Contract:Watchdog:${label}`)), WATCH_MS);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function expectCtorRejects(options, label) {
  const io = { read: async () => new Uint8Array(0), close: () => {}, write: async () => {} };
  try {
    const stream = new FramedStream(io, options);
    stream.cancel();
  } catch (error) {
    if (error instanceof Error && error.message === CLOSED) return;
    throw error;
  }
  fail(label);
}

// Real WebStreams byte port: writes flow through a real WritableStream sink
// and reads arrive from a real ReadableStream reader. Small payloads only.
// close cancels pending I/O before releasing either owner lock.
function loopPort() {
  let push = null;
  const source = new ReadableStream({ start(c) { push = c; } });
  const reader = source.getReader();
  const sink = new WritableStream({ write() {} });
  const writer = sink.getWriter();
  const io = {
    async read() {
      const next = await reader.read();
      if (next.done) return new Uint8Array(0);
      const bytes = next.value instanceof Uint8Array ? next.value : Uint8Array.from(next.value);
      return bytes.slice();
    },
    async write(bytes) {
      const copy = new Uint8Array(bytes);
      await writer.write(copy);
      push.enqueue(copy);
    },
    close() {
      reader.cancel().catch(() => {});
      writer.abort().catch(() => {});
      try { reader.releaseLock(); } catch { /* port owns details */ }
      try { writer.releaseLock(); } catch { /* port owns details */ }
    },
  };
  const cleanup = async () => {
    try { await reader.cancel(); } catch { /* port owns details */ }
    try { await writer.abort(); } catch { /* port owns details */ }
    try { reader.releaseLock(); } catch { /* port owns details */ }
    try { writer.releaseLock(); } catch { /* port owns details */ }
  };
  return { io, cleanup };
}

async function validBoundary(maximumBytes, timeoutMs, label) {
  const { io, cleanup } = loopPort();
  let stream = null;
  try {
    // Frozen injected clock: option bounds only, never deadline behaviour.
    const tick = 0;
    stream = new FramedStream(io, { maximumBytes, timeoutMs, clock: () => tick });
    if (stream.state !== 'Open') fail(`${label}:not-open`);
    const payload = new Uint8Array([7]);
    await watched(stream.sendFrame(payload), `${label}:send`);
    if (stream.state !== 'Open') fail(`${label}:send-closed`);
    const got = await watched(stream.nextFrame(), `${label}:recv`);
    if (!same(got, payload)) fail(`${label}:payload`);
    if (stream.state !== 'Open') fail(`${label}:recv-closed`);
  } finally {
    try { if (stream) stream.cancel(); } catch { /* contract owns state */ }
    await watched(cleanup(), `${label}:cleanup`);
  }
}

async function clockRejected(makeClock, drive, label) {
  const { io, cleanup } = loopPort();
  let stream = null;
  try {
    stream = new FramedStream(io, { maximumBytes: 64, timeoutMs: 1000, clock: makeClock() });
    await watched(drive(stream), label);
    fail(label);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith('cfry:Contract:Watchdog')) throw error;
    if (!(error instanceof Error) || error.message !== CLOSED) throw error;
    if (!stream || stream.state !== 'Failed') fail(`${label}:state`);
  } finally {
    try { if (stream) stream.cancel(); } catch { /* contract owns state */ }
    await watched(cleanup(), `${label}:cleanup`);
  }
}

export async function runStreamOptionsContract() {
  let cases = 0;
  for (const value of [NaN, Infinity, 1.5, 2 ** 53, 0, -1]) {
    expectCtorRejects({ maximumBytes: value, timeoutMs: 1000 }, 'option-maximum');
    cases += 1;
  }
  for (const value of [NaN, Infinity, 2.5, 2 ** 53, 0, -1]) {
    expectCtorRejects({ maximumBytes: 64, timeoutMs: value }, 'option-timeout');
    cases += 1;
  }
  await watched(validBoundary(1, 60000, 'edge-min-max'), 'edge-min-max:watch');
  cases += 1;
  await watched(validBoundary(MAX, 1, 'edge-max-min'), 'edge-max-min:watch');
  cases += 1;
  await watched(
    clockRejected(() => () => NaN, (stream) => stream.sendFrame(new Uint8Array([1])), 'clock-initial'),
    'clock-initial:watch',
  );
  cases += 1;
  let tick = 0;
  await watched(
    clockRejected(
      () => () => tick,
      (stream) => { tick = NaN; return stream.nextFrame(); },
      'clock-next',
    ),
    'clock-next:watch',
  );
  cases += 1;
  if (cases !== 16) fail('option-count');
  return { cases };
}
