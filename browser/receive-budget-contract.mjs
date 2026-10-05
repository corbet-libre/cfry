import { FramedStream } from './streams.mjs';
import { FrameCodec } from './pkg/cfry.js';

// Whole-frame receive budgets over the actual FramedStream, real Wasm framing,
// and a real WebStreams byte port. Caller initializes Wasm. Budgets only;
// EOF/coalescing/copy/concurrency live in receive-lifecycle.
const MAXIMUM = 16;
const TIMEOUT_MS = 100;
const BOUND_MS = 2000;
const CLOSED = 'cfry:StreamClosed';
const fail = (label) => { throw new Error(`cfry:Contract:${label}`); };
const same = (got, want) =>
  got instanceof Uint8Array && got.length === want.length &&
  got.every((value, index) => value === want[index]);
const isWatchdog = (e) => e instanceof Error && e.message.startsWith('cfry:Contract:Watchdog');
// Finite test-only bound; watchdog faults never pose as StreamClosed.
async function watched(promise, label) {
  promise.catch(() => {});
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`cfry:Contract:Watchdog:${label}`)), BOUND_MS);
    })]);
  } finally { clearTimeout(timer); }
}
async function expectClosed(promise, label) {
  try { await watched(promise, label); } catch (error) {
    if (isWatchdog(error)) throw error;
    if (error instanceof Error && error.message === CLOSED) return;
    throw error;
  }
  fail(label);
}
// Real WebStreams port: reads from a real reader, close cancels pending reads.
// Adapter/watchdog faults recorded separately, never masked as StreamClosed.
function openPort(tick, onRead) {
  let controller;
  const source = new ReadableStream({ start(c) { controller = c; } });
  const reader = source.getReader();
  const calls = { read: 0, close: 0, remainings: [], fault: null };
  const pending = new Set();
  const io = {
    async read(maximum, remaining) {
      calls.read += 1;
      calls.remainings.push(remaining);
      const task = reader.read();
      pending.add(task);
      task.then(() => pending.delete(task), () => pending.delete(task));
      try {
        const next = await watched(task, 'port-read');
        if (onRead) onRead(calls.read);
        if (next.done) return new Uint8Array(0);
        return next.value instanceof Uint8Array ? next.value.slice() : Uint8Array.from(next.value);
      } catch (error) {
        if (isWatchdog(error)) calls.fault = error.message;
        throw error;
      }
    },
    close() {
      calls.close += 1;
      try { reader.cancel().catch(() => {}); } catch { /* port owns details */ }
    },
  };
  const cleanup = async () => {
    await watched(reader.cancel().catch(() => {}), 'port-cancel');
    for (const task of [...pending]) {
      try { await watched(task, 'port-settle'); } catch (error) { if (isWatchdog(error)) throw error; }
    }
    try { reader.releaseLock(); } catch { /* port owns details */ }
  };
  return { io, controller, reader, calls, cleanup };
}
const opts = (tick) => ({ maximumBytes: MAXIMUM, timeoutMs: TIMEOUT_MS, clock: () => tick.t });
const done = async (stream, port, label) => {
  try { stream.cancel(); } catch { /* terminal already */ }
  await watched(port.cleanup(), label);
};
export async function runReceiveBudgetContract() {
  const codec = new FrameCodec(MAXIMUM);
  let wireA, wireB, wireC;
  try {
    wireA = codec.encode(new Uint8Array([1, 2, 3])).slice();
    wireB = codec.encode(new Uint8Array([4, 5])).slice();
    wireC = codec.encode(new Uint8Array([6, 7, 8, 9])).slice();
  } finally { try { codec.free(); } catch { /* wasm owns teardown */ } }
  let cases = 0;
  {
    const tick = { t: 0 };
    const port = openPort(tick);
    const stream = new FramedStream(port.io, opts(tick));
    try {
      port.controller.enqueue(wireA.slice(0, 2));
      port.controller.enqueue(wireA.slice(2, 5));
      port.controller.enqueue(wireA.slice(5));
      port.controller.close();
      if (!same(await watched(stream.nextFrame(), 'frag'), [1, 2, 3])) fail('frag-payload');
      if (port.calls.fault || port.calls.read !== 3) fail('frag-io');
      if (stream.state !== 'Open') fail('frag-open');
      if (await watched(stream.nextFrame(), 'frag-eof') !== null) fail('frag-eof');
      if (stream.state !== 'Closed') fail('frag-closed');
      cases += 1;
    } finally { await done(stream, port, 'frag-cleanup'); }
  }
  {
    const tick = { t: 0 };
    const port = openPort(tick);
    const stream = new FramedStream(port.io, opts(tick));
    try {
      const pending = stream.nextFrame();
      pending.catch(() => {});
      await watched(new Promise((r) => setTimeout(r, 0)), 'deadline-arm');
      tick.t = TIMEOUT_MS;
      port.controller.enqueue(wireB.slice());
      port.controller.close();
      await expectClosed(pending, 'deadline-exact');
      if (port.calls.fault || port.calls.read !== 1) fail('deadline-io');
      if (stream.state !== 'Failed' || port.calls.close !== 1) fail('deadline-state');
      cases += 1;
    } finally { await done(stream, port, 'deadline-cleanup'); }
  }
  {
    const tick = { t: 0 };
    const port = openPort(tick, (n) => { tick.t = n === 1 ? 60 : TIMEOUT_MS; });
    const stream = new FramedStream(port.io, opts(tick));
    try {
      port.controller.enqueue(wireC.slice(0, 3));
      port.controller.enqueue(wireC.slice(3));
      port.controller.close();
      await expectClosed(watched(stream.nextFrame(), 'progress'), 'progress-budget');
      if (port.calls.fault || port.calls.read !== 2) fail('progress-io');
      if (port.calls.remainings[1] !== 40) fail('progress-remaining');
      if (stream.state !== 'Failed') fail('progress-state');
      cases += 1;
    } finally { await done(stream, port, 'progress-cleanup'); }
  }
  {
    const tick = { t: 10 };
    const port = openPort(tick, () => { tick.t = 9; });
    const stream = new FramedStream(port.io, opts(tick));
    try {
      port.controller.enqueue(wireB.slice());
      port.controller.close();
      await expectClosed(watched(stream.nextFrame(), 'regress'), 'regress-clock');
      if (port.calls.fault || port.calls.read !== 1) fail('regress-io');
      if (stream.state !== 'Failed') fail('regress-state');
      cases += 1;
    } finally { await done(stream, port, 'regress-cleanup'); }
  }
  {
    const tick = { t: 0 };
    const port = openPort(tick);
    const stream = new FramedStream(port.io, opts(tick));
    try {
      port.controller.enqueue(new Uint8Array(MAXIMUM + 4 + 1));
      port.controller.close();
      await expectClosed(watched(stream.nextFrame(), 'oversize'), 'oversize-chunk');
      if (port.calls.fault || port.calls.read !== 1) fail('oversize-io');
      if (stream.state !== 'Failed') fail('oversize-state');
      cases += 1;
    } finally { await done(stream, port, 'oversize-cleanup'); }
  }
  {
    const tick = { t: 0 };
    const port = openPort(tick);
    const stream = new FramedStream(port.io, opts(tick));
    try {
      port.controller.enqueue(new Uint8Array([0, 0, 0, 32, 1, 2]));
      port.controller.close();
      await expectClosed(watched(stream.nextFrame(), 'terminal-first'), 'terminal-first');
      if (stream.state !== 'Failed') fail('terminal-state');
      const reads = port.calls.read;
      const closes = port.calls.close;
      await expectClosed(watched(stream.nextFrame(), 'terminal-second'), 'terminal-second');
      if (port.calls.read !== reads || port.calls.close !== closes) fail('terminal-io');
      if (port.calls.fault) fail('terminal-fault');
      if (!(await watched(port.reader.read(), 'terminal-cancelled')).done) fail('terminal-pending');
      cases += 1;
    } finally { await done(stream, port, 'terminal-cleanup'); }
  }
  if (cases !== 6) fail('budget-count');
  return { cases };
}
