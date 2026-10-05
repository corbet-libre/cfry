import { FramedStream } from './streams.mjs';
import { FrameCodec } from './pkg/cfry.js';

const MAXIMUM = 64;
const TIMEOUT_MS = 1000;
const BOUND_MS = 5000;
const CLOSED_LABEL = 'cfry:StreamClosed';
const TIMEOUT_PREFIX = 'cfry:Contract:Timeout:';

const same = (got, want) =>
  got instanceof Uint8Array && got.length === want.length &&
  got.every((value, index) => value === want[index]);
const fail = (label) => {
  throw new Error(`cfry:Contract:${label}`);
};

async function bound(promise, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${TIMEOUT_PREFIX}${label}`)), BOUND_MS);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function sourceFrom(chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

function byteIo(source) {
  const reader = source.getReader();
  const reads = new Set();
  const unlock = () => {
    try {
      reader.releaseLock();
    } catch { /* external stream owns details */ }
  };
  const io = {
    async read(maximum) {
      const task = reader.read();
      reads.add(task);
      let next;
      try {
        next = await task;
      } finally {
        reads.delete(task);
      }
      if (next.done) return new Uint8Array(0);
      const bytes = next.value instanceof Uint8Array ? next.value : Uint8Array.from(next.value);
      if (bytes.length > maximum) fail('ChunkLimit');
      return bytes;
    },
    async write() {},
    close() {
      try {
        reader.cancel().then(unlock, unlock);
      } catch {
        unlock();
      }
    },
  };
  const cancel = async () => {
    try {
      await reader.cancel();
    } catch { /* already closing */ }
    for (const task of [...reads]) {
      try {
        await task;
      } catch { /* pending read settles closed */ }
    }
    unlock();
  };
  return { io, cancel };
}

async function runCase(makeSource, body) {
  const made = makeSource();
  const handle = byteIo(made.source);
  const stream = new FramedStream(handle.io, { maximumBytes: MAXIMUM, timeoutMs: TIMEOUT_MS });
  const outstanding = new Set();
  const watch = (task) => {
    outstanding.add(task);
    task.then(() => outstanding.delete(task), () => outstanding.delete(task));
    return task;
  };
  const next = (step) => bound(watch(stream.nextFrame()), step);
  try {
    await body({ stream, next, watch, controller: made.controller });
  } finally {
    try {
      stream.cancel();
    } catch { /* terminal already */ }
    try {
      await bound(handle.cancel(), 'cleanup');
    } catch (error) {
      if (error instanceof Error && error.message.startsWith(TIMEOUT_PREFIX)) throw error;
    }
    for (const task of [...outstanding]) {
      try {
        await bound(task, 'settle');
      } catch (error) {
        if (error instanceof Error && error.message === CLOSED_LABEL) continue;
        throw error;
      }
    }
  }
}

export async function runReceiveLifecycleContract() {
  const codec = new FrameCodec(MAXIMUM);
  let encA;
  let encB;
  let encC;
  try {
    encA = codec.encode(new Uint8Array([1, 2, 3])).slice();
    encB = codec.encode(new Uint8Array([4, 5])).slice();
    encC = codec.encode(new Uint8Array([9])).slice();
  } finally {
    try {
      codec.free();
    } catch { /* wasm owns teardown */ }
  }
  const both = new Uint8Array(encA.length + encB.length);
  both.set(encA, 0);
  both.set(encB, encA.length);
  const pair = new Uint8Array(encB.length + encC.length);
  pair.set(encB, 0);
  pair.set(encC, encB.length);

  await runCase(() => ({ source: sourceFrom([encA.slice()]) }), async ({ next, stream }) => {
    const first = await next('single');
    if (!same(first, [1, 2, 3])) fail('SinglePayload');
    if (await next('single-eof') !== null) fail('SingleEof');
    if (stream.state !== 'Closed') fail('SingleClosed');
  });

  await runCase(() => ({ source: sourceFrom([both.slice()]) }), async ({ next, stream }) => {
    const first = await next('coalesced-first');
    const second = await next('coalesced-second');
    if (!same(first, [1, 2, 3]) || !same(second, [4, 5])) fail('CoalescedPayloads');
    if (await next('coalesced-eof') !== null) fail('CoalescedEof');
    if (stream.state !== 'Closed') fail('CoalescedClosed');
  });

  for (const [label, bytes] of [['partial-header', encA.slice(0, 2)], ['partial-payload', encA.slice(0, 5)]]) {
    await runCase(() => ({ source: sourceFrom([bytes]) }), async ({ next, stream }) => {
      try {
        await next(label);
      } catch (error) {
        if (error instanceof Error && error.message === CLOSED_LABEL && stream.state === 'Failed') return;
        throw error;
      }
      fail(label);
    });
  }

  await runCase(() => ({ source: sourceFrom([pair.slice()]) }), async ({ next }) => {
    const first = await next('buffered-first');
    first.fill(0);
    if (!same(await next('buffered-second'), [9])) fail('BufferedExact');
    if (await next('buffered-eof') !== null) fail('BufferedEof');
  });

  await runCase(
    () => {
      let controller;
      const source = new ReadableStream({ start(c) { controller = c; } });
      return { source, controller };
    },
    async ({ stream, watch, controller }) => {
      const pending = watch(stream.nextFrame());
      const second = watch(stream.nextFrame());
      try {
        await bound(second, 'concurrent-refusal');
        fail('ConcurrentAccepted');
      } catch (error) {
        if (!(error instanceof Error) || error.message !== CLOSED_LABEL) throw error;
      }
      if (stream.state !== 'Open') fail('ConcurrentOpen');
      controller.enqueue(encC.slice());
      controller.close();
      if (!same(await bound(pending, 'concurrent'), [9])) fail('ConcurrentPayload');
      if (await bound(watch(stream.nextFrame()), 'concurrent-eof') !== null) fail('ConcurrentEof');
      if (stream.state !== 'Closed') fail('ConcurrentClosed');
    },
  );

  return { cases: 6 };
}
