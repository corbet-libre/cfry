import { FrameCodec } from './pkg/cfry.js';

// JS numeric/array/copy/error boundary contract for the actual Wasm FrameCodec.
// The caller initializes the real Wasm module first; this module never builds
// a stand-in codec and never reimplements framing.

const MAX = 1024 * 1024;

function check(out, label, ok) {
  out.push(label);
  if (!ok) throw new Error(`codec-contract failed: ${label}`);
}

function refusedText(fn) {
  let value;
  try {
    value = fn();
  } catch (error) {
    const text = error && error.message ? error.message : error;
    return String(text);
  }
  if (value instanceof FrameCodec) value.free();
  return null;
}

function checkRefused(out, label, fn, tag) {
  check(out, label, refusedText(fn) === tag);
}

function same(a, b) {
  if (!(a instanceof Uint8Array) || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) if (a[i] !== b[i]) return false;
  return true;
}

function withCodec(maximum, fn) {
  const codec = new FrameCodec(maximum);
  try {
    return fn(codec);
  } finally {
    codec.free();
  }
}

function badConstructors(out) {
  const bad = [
    ['ctor:NaN', NaN],
    ['ctor:+Inf', Infinity],
    ['ctor:-Inf', -Infinity],
    ['ctor:zero', 0],
    ['ctor:negzero', -0],
    ['ctor:frac', 1.5],
    ['ctor:over-max', MAX + 1],
  ];
  for (const [label, value] of bad) {
    checkRefused(out, label, () => new FrameCodec(value), 'cfry:Limit'));
  }
  // Exact bounds stay usable with no large allocation (empty frame only).
  for (const [label, value] of [['ctor:one', 1], ['ctor:max', MAX]]) {
    const codec = new FrameCodec(value);
    try {
      check(out, label, same(codec.encode(new Uint8Array(0)), new Uint8Array([0, 0, 0, 0])));
    } finally {
      codec.free();
    }
  }
}

function encodeCopiesInput(out) {
  withCodec(64, (codec) => {
    const backing = new Uint8Array([7, 7, 104, 105, 0, 255, 7]);
    const view = backing.subarray(2, 6);
    check(out, 'encode:subarray-view', view.byteOffset === 2 && view.length === 4);
    const wire = codec.encode(view);
    const exact = new Uint8Array([0, 0, 0, 4, 104, 105, 0, 255]);
    check(out, 'encode:exact-wire', same(wire, exact));
    backing.fill(9);
    check(out, 'encode:input-mutation-kept', same(wire, exact));
  });
}

function pushSplitOwnsPrefix(out) {
  withCodec(64, (codec) => {
    const wire = codec.encode(new Uint8Array([1, 2, 3]));
    const prefix = wire.slice(0, 3);
    const first = codec.push(prefix);
    check(out, 'push:prefix-incomplete', Array.isArray(first) && first.length === 0);
    prefix.fill(9);
    const frames = codec.push(wire.slice(3));
    check(out, 'push:suffix-exact', frames.length === 1 && same(frames[0], new Uint8Array([1, 2, 3])));
    check(out, 'push:frame-u8', Array.isArray(frames) && frames[0] instanceof Uint8Array);
    const held = frames[0];
    const more = codec.push(codec.encode(new Uint8Array([4])));
    check(out, 'push:frame-detached', more.length === 1 && same(held, new Uint8Array([1, 2, 3])));
  });
}

function binaryPayloads(out) {
  withCodec(64, (codec) => {
    const empty = codec.push(codec.encode(new Uint8Array(0)));
    check(out, 'frame:empty', empty.length === 1 && empty[0] instanceof Uint8Array && empty[0].length === 0);
    const payload = new Uint8Array([0, 255, 0, 128, 1]);
    const frames = codec.push(codec.encode(payload));
    check(out, 'frame:nul-high', frames.length === 1 && same(frames[0], payload));
  });
}

function truncatedThenClosed(out) {
  withCodec(64, (codec) => {
    const pending = codec.push(new Uint8Array([0, 0, 0, 4, 1]));
    check(out, 'finish:partial-empty', Array.isArray(pending) && pending.length === 0);
    checkRefused(out, 'finish:protocol', () => codec.finish(), 'cfry:Protocol');
    checkRefused(out, 'after-finish:encode-closed', () => codec.encode(new Uint8Array([1])), 'cfry:Closed');
    checkRefused(out, 'after-finish:push-closed', () => codec.push(new Uint8Array([1])), 'cfry:Closed');
  });
}

function overflowThenClosed(out) {
  withCodec(8, (codec) => {
    checkRefused(out, 'limit:encode', () => codec.encode(new Uint8Array(9)), 'cfry:Limit');
    checkRefused(out, 'limit:encode-closed', () => codec.push(new Uint8Array([1])), 'cfry:Closed');
  });
  withCodec(8, (codec) => {
    checkRefused(out, 'limit:push', () => codec.push(new Uint8Array(13)), 'cfry:Limit');
    checkRefused(out, 'limit:push-closed', () => codec.encode(new Uint8Array([1])), 'cfry:Closed');
  });
}

export function runCodecContract() {
  const labels = [];
  badConstructors(labels);
  encodeCopiesInput(labels);
  pushSplitOwnsPrefix(labels);
  binaryPayloads(labels);
  truncatedThenClosed(labels);
  overflowThenClosed(labels);
  return { labels, count: labels.length };
}
