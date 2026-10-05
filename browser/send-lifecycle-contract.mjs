import { FramedStream } from './streams.mjs';
const fail = (label) => { throw new Error(label); };
const expect = (cond, label) => { if (!cond) fail(label); };
const deferred = () => { let resolve; let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  promise.catch(() => {}); return { promise, resolve, reject }; };
const toBoundedU8 = (value, max, label) => {
  const u8 = value instanceof Uint8Array ? value : new Uint8Array(0);
  if (u8.length > max) fail(label);
  return new Uint8Array(u8);
};
const isSettled = async (p) => {
  let done = false;
  p.then(() => { done = true; }, () => { done = true; });
  await new Promise((r) => setTimeout(r, 0));
  return done;
};
// Production bounded refusals are exactly cfry:StreamClosed; any other
// rejection rethrows. The outer watchdog bounds never-settling promises.
const expectClosed = (p, label) => watchdog((async () => {
  try { await p; } catch (e) {
    if (e instanceof Error && e.message === 'cfry:StreamClosed') return; throw e;
  } fail(label); })(), 'synthetic-watchdog-refusal', 5000);
const watchdog = (p, label, ms = 2000) => {
  p.catch(() => {});
  let t;
  const limit = new Promise((_, rej) => { t = setTimeout(() => rej(new Error(label)), ms); });
  return Promise.race([p, limit]).finally(() => clearTimeout(t));
};
const payload = (n, s) => Uint8Array.from({ length: n }, (_, i) => (i + s) & 255);
// Real TransformStream echo plus WritableStream sink; read/write start
// together with handlers attached, bounded completion, bytes verified.
const realEcho = async (copy, max, sinkWriter) => {
  const ts = new TransformStream();
  const w = ts.writable.getWriter();
  const r = ts.readable.getReader();
  try {
    const writing = w.write(copy); writing.catch(() => {});
    const reading = r.read(); reading.catch(() => {});
    await watchdog(writing, 'synthetic-watchdog-echo-write');
    const next = await watchdog(reading, 'synthetic-watchdog-echo-read');
    await watchdog(w.close(), 'synthetic-watchdog-echo-close');
    if (next.done || !(next.value instanceof Uint8Array)) fail('synthetic-echo-empty');
    const got = toBoundedU8(next.value, max, 'synthetic-echo-bound');
    expect(got.length === copy.length, 'synthetic-echo-length');
    for (let i = 0; i < got.length; i += 1) expect(got[i] === copy[i], 'synthetic-echo-bytes');
    await watchdog(sinkWriter.write(got), 'synthetic-watchdog-sink');
    return got;
  } finally {
    r.cancel().catch(() => {});
    w.abort().catch(() => {});
    try { r.releaseLock(); w.releaseLock(); } catch { /* test-only cleanup */ }
  }
};
// Adapter faults are recorded out of band: FramedStream normalizes every I/O
// error to cfry:StreamClosed, so a hung adapter must not pass as a refusal.
const parkedWrite = (calls, gate, sinkWriter, faults) => async (bytes) => {
  try {
    calls.write += 1;
    const copy = toBoundedU8(bytes, 260, 'synthetic-write-bound');
    await watchdog(gate.promise, 'synthetic-watchdog-gate');
    await realEcho(copy, 260, sinkWriter);
    calls.done += 1;
  } catch (e) { if (!(e instanceof Error && e.message === 'synthetic-testio-closed')) faults.push(e); throw e; }
};
const drop = async (stream, gate, reader, writer, sends, faults) => {
  stream.cancel();
  try { gate.reject(new Error('synthetic-testio-closed')); } catch { /* test-only */ }
  if (reader) { reader.cancel().catch(() => {}); try { reader.releaseLock(); } catch { /* test-only */ } }
  if (writer) { writer.abort().catch(() => {}); try { writer.releaseLock(); } catch { /* test-only */ } }
  for (const s of sends || []) if (s) await watchdog(s.catch(() => {}), 'synthetic-watchdog-settle', 5000).catch((e) => faults.push(e));
};
const testClose = (gate) => () => { try { gate.reject(new Error('synthetic-testio-closed')); } catch { /* test-only */ } };
// Deadline/regression refusal: the wire write genuinely completes after a real
// control read, then the injected clock advance forces the refusal. Faults and
// the completed write are asserted after cleanup, never masked by it.
const refusalCase = async ({ tick0, tick1, seed, seed2, tag }) => {
  let tick = tick0;
  const gate = deferred();
  const calls = { write: 0, done: 0 };
  const faults = [];
  const sinkWriter = new WritableStream({ write() {} }).getWriter();
  let pushControl = null;
  const control = new ReadableStream({ start(c) { pushControl = c; } });
  const controlReader = control.getReader();
  const io = { read: async () => new Uint8Array(0), close: testClose(gate), write: parkedWrite(calls, gate, sinkWriter, faults) };
  const stream = new FramedStream(io, { maximumBytes: 256, timeoutMs: 100, clock: () => tick });
  let send;
  try {
    const controlRead = controlReader.read(); controlRead.catch(() => {});
    send = stream.sendFrame(payload(24, seed));
    send.catch(() => {});
    expect(!(await isSettled(send)), `synthetic-${tag}-not-pending`);
    tick = tick1;
    pushControl.enqueue(new Uint8Array([2]));
    await watchdog(controlRead, 'synthetic-watchdog-control');
    gate.resolve();
    await expectClosed(send, `synthetic-${tag}-admitted`);
    expect(stream.state === 'Failed', `synthetic-${tag}-not-failed`);
    const before = calls.write;
    await expectClosed(stream.sendFrame(payload(8, seed2)), `synthetic-${tag}-retry-admitted`);
    expect(calls.write === before, `synthetic-${tag}-retried-io`);
  } finally {
    await drop(stream, gate, controlReader, sinkWriter, [send], faults);
  }
  expect(faults.length === 0, 'synthetic-harness-fault');
  expect(calls.done === 1, 'synthetic-wire-write-missing');
};
export async function runSendLifecycleContract() {
  let cases = 0;
  {
    let tick = 0;
    const gate = deferred();
    const transported = [];
    const calls = { write: 0 };
    const faults = [];
    const collected = [];
    const sink = new WritableStream({ write(c) { collected.push(new Uint8Array(c)); } });
    const sinkWriter = sink.getWriter();
    let pushControl = null;
    const control = new ReadableStream({ start(c) { pushControl = c; } });
    const controlReader = control.getReader();
    const msg = payload(32, 7); const other = payload(16, 3);
    const io = {
      read: async () => new Uint8Array(0),
      close: testClose(gate),
      write: async (bytes) => {
        calls.write += 1;
        const copy = toBoundedU8(bytes, 260, 'synthetic-write-bound');
        await watchdog(gate.promise, 'synthetic-watchdog-gate');
        transported.push(await realEcho(copy, 260, sinkWriter));
      },
    };
    const stream = new FramedStream(io, { maximumBytes: 256, timeoutMs: 100, clock: () => tick });
    let first; let overlap;
    try {
      const controlRead = controlReader.read(); controlRead.catch(() => {});
      first = stream.sendFrame(msg);
      first.catch(() => {});
      expect(!(await isSettled(first)), 'synthetic-backpressure-not-pending');
      overlap = stream.sendFrame(other);
      overlap.catch(() => {});
      await expectClosed(overlap, 'synthetic-overlap-admitted');
      expect(calls.write === 1, 'synthetic-overlap-retried-io');
      pushControl.enqueue(new Uint8Array([1]));
      const ctrl = await watchdog(controlRead, 'synthetic-watchdog-control');
      toBoundedU8(ctrl.value, 8, 'synthetic-control-bound');
      gate.resolve();
      await watchdog(first, 'synthetic-watchdog-send');
      expect(stream.state === 'Open', 'synthetic-send-not-open');
      expect(transported.length === 1, 'synthetic-payload-missing');
      const enc = transported[0];
      expect(enc.length === msg.length + 4, 'synthetic-payload-length');
      for (let i = 0; i < msg.length; i += 1) expect(enc[4 + i] === msg[i], 'synthetic-payload-bytes');
      expect(collected.length === 1 && collected[0].length === enc.length, 'synthetic-sink-drop');
      cases += 3;
    } finally {
      await drop(stream, gate, controlReader, sinkWriter, [first, overlap], faults);
    }
    expect(faults.length === 0, 'synthetic-settle-fault');
  }
  await refusalCase({ tick0: 0, tick1: 100, seed: 11, seed2: 1, tag: 'deadline' });
  cases += 1;
  await refusalCase({ tick0: 50, tick1: 49, seed: 21, seed2: 2, tag: 'regression' });
  cases += 1;
  {
    let tick = 0;
    const gate = deferred();
    const faults = [];
    const io = {
      read: async () => new Uint8Array(0),
      close: testClose(gate),
      write: async () => {
        try { await watchdog(gate.promise, 'synthetic-watchdog-gate'); }
        catch (e) { if (!(e instanceof Error && e.message === 'synthetic-testio-closed')) faults.push(e); throw e; }
      },
    };
    const stream = new FramedStream(io, { maximumBytes: 256, timeoutMs: 100, clock: () => tick });
    let send;
    try {
      send = stream.sendFrame(payload(16, 5));
      send.catch(() => {});
      expect(!(await isSettled(send)), 'synthetic-finishwrite-not-pending');
      let threw = false;
      try { stream.finish(); } catch { threw = true; }
      expect(threw, 'synthetic-finishwrite-admitted');
      expect(stream.state === 'Failed', 'synthetic-finishwrite-not-failed');
      await expectClosed(send, 'synthetic-finishwrite-send-survived');
      cases += 1;
    } finally {
      await drop(stream, gate, null, null, [send], faults);
    }
    expect(faults.length === 0, 'synthetic-harness-fault');
  }
  {
    const tick = 0;
    const io = { read: async () => new Uint8Array(0), close: () => {}, write: async () => {} };
    const stream = new FramedStream(io, { maximumBytes: 256, timeoutMs: 100, clock: () => tick });
    try {
      stream.finish();
      expect(stream.state === 'Closed', 'synthetic-finish-not-closed');
      cases += 1;
    } finally {
      stream.cancel();
    }
  }
  return cases;
}
