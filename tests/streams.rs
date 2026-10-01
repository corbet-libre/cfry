//! Identical native and wasm framing lifecycle vectors.
use cfry::{
    BoxFuture,
    frame::FrameCodec,
    streams::{Clock, FramedStream, Limits, State},
};
use futures::{
    FutureExt,
    io::{AsyncRead, AsyncWrite},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

#[derive(Default)]
struct Time {
    tick: AtomicU64,
    wake: Mutex<Option<Waker>>,
}
impl Time {
    fn set(&self, tick: u64) {
        self.tick.store(tick, Ordering::SeqCst);
        if let Some(w) = self.wake.lock().unwrap().take() {
            w.wake();
        }
    }
}
impl Clock for Time {
    fn now(&self) -> u64 {
        self.tick.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, end: u64) -> BoxFuture<'_, ()> {
        Box::pin(futures::future::poll_fn(move |cx| {
            *self.wake.lock().unwrap() = Some(cx.waker().clone());
            if self.now() >= end {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
    }
}
struct Wire {
    input: Vec<u8>,
    pos: usize,
    split: usize,
    output: Arc<Mutex<Vec<u8>>>,
    dropped: Arc<AtomicBool>,
    stall: bool,
}
impl Drop for Wire {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl AsyncRead for Wire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.stall {
            return Poll::Pending;
        }
        let n = self.split.min(buf.len()).min(self.input.len() - self.pos);
        buf[..n].copy_from_slice(&self.input[self.pos..self.pos + n]);
        self.pos += n;
        Poll::Ready(Ok(n))
    }
}
impl AsyncWrite for Wire {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.stall {
            return Poll::Pending;
        }
        let n = self.split.min(buf.len());
        self.output.lock().unwrap().extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn wire(input: Vec<u8>, split: usize, stall: bool) -> Wire {
    Wire {
        input,
        pos: 0,
        split,
        output: Arc::default(),
        dropped: Arc::default(),
        stall,
    }
}
fn stream(wire: Wire, clock: Arc<Time>) -> FramedStream<Wire> {
    FramedStream::open(
        wire,
        Limits {
            max_frame_bytes: 64,
            timeout_ms: 10,
        },
        clock,
    )
    .unwrap()
}
fn ready<T>(f: impl Future<Output = T>) -> T {
    f.now_or_never().expect("ready controlled I/O")
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn identical_vectors_split_coalesced_and_partial_io() {
    let expected = [vec![], b"hello".to_vec(), vec![255; 64], vec![0; 1]];
    let mut codec = FrameCodec::new(64).unwrap();
    let bytes: Vec<_> = expected
        .iter()
        .flat_map(|p| codec.encode(p).unwrap())
        .collect();
    for split in 1..=68 {
        let io = wire(bytes.clone(), split, false);
        let output = io.output.clone();
        let dropped = io.dropped.clone();
        let mut s = stream(io, Arc::default());
        for p in &expected {
            ready(s.send_frame(p)).unwrap();
            assert_eq!(ready(s.next_frame()).unwrap(), Some(p.clone()));
        }
        assert_eq!(*output.lock().unwrap(), bytes);
        assert_eq!(ready(s.next_frame()).unwrap(), None);
        assert_eq!(s.state(), State::Closed);
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn truncated_headers_and_payloads_fail_in_both_codecs() {
    let full = [0, 0, 0, 3, 1, 2, 3];
    for n in 1..full.len() {
        let mut codec = FrameCodec::new(64).unwrap();
        codec.push(&full[..n]).unwrap();
        assert!(codec.finish().is_err(), "prefix {n}");
        let mut s = stream(wire(full[..n].to_vec(), 2, false), Arc::default());
        assert!(ready(s.next_frame()).is_err(), "prefix {n}");
        assert_eq!(s.state(), State::Failed);
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn oversized_length_and_send_refuse_before_payload_work() {
    let io = wire(u32::MAX.to_be_bytes().to_vec(), 4, false);
    let dropped = io.dropped.clone();
    let mut s = stream(io, Arc::default());
    assert!(ready(s.next_frame()).is_err());
    assert!(dropped.load(Ordering::SeqCst));
    let io = wire(vec![], 1, false);
    let output = io.output.clone();
    let mut s = stream(io, Arc::default());
    assert!(ready(s.send_frame(&[0; 65])).is_err());
    assert!(output.lock().unwrap().is_empty());
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn cancellation_releases_slow_reader_and_writer() {
    for writing in [false, true] {
        let io = wire(vec![], 1, true);
        let dropped = io.dropped.clone();
        let mut s = stream(io, Arc::default());
        if writing {
            assert!(s.send_frame(b"data").now_or_never().is_none());
        } else {
            assert!(s.next_frame().now_or_never().is_none());
        }
        assert_eq!(s.state(), State::Failed);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(ready(s.send_frame(b"retry")).is_err());
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn exact_deadline_clock_regression_and_finish() {
    let clock = Arc::new(Time::default());
    let mut s = stream(wire(vec![], 1, true), clock.clone());
    let mut future = Box::pin(s.next_frame());
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    clock.set(10);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Err(_))));
    drop(future);
    assert_eq!(s.state(), State::Failed);
    let mut s = stream(wire(vec![], 1, false), clock.clone());
    clock.set(9);
    assert!(ready(s.send_frame(b"x")).is_err());
    let io = wire(vec![], 1, false);
    let dropped = io.dropped.clone();
    let mut s = stream(io, clock);
    ready(s.finish()).unwrap();
    assert_eq!(s.state(), State::Closed);
    assert!(dropped.load(Ordering::SeqCst));
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn invalid_limits_explicit_cancel_and_error_redaction() {
    for timeout_ms in [0, 60001] {
        assert!(
            FramedStream::open(
                wire(vec![], 1, false),
                Limits {
                    max_frame_bytes: 16,
                    timeout_ms
                },
                Arc::new(Time::default())
            )
            .is_err()
        );
    }
    let io = wire(vec![], 1, false);
    let dropped = io.dropped.clone();
    let mut s = stream(io, Arc::default());
    s.cancel();
    assert_eq!(s.state(), State::Failed);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(ready(s.next_frame()).is_err());
    assert!(ready(s.finish()).is_err());
    for (kind, io_kind) in [
        (cfry::ErrorKind::Timeout, std::io::ErrorKind::TimedOut),
        (cfry::ErrorKind::Closed, std::io::ErrorKind::BrokenPipe),
        (cfry::ErrorKind::Limit, std::io::ErrorKind::OutOfMemory),
        (cfry::ErrorKind::Protocol, std::io::ErrorKind::InvalidData),
    ] {
        let e = cfry::Error::new(kind, "coarse error");
        assert_eq!(e.context(), "coarse error");
        assert!(e.to_string().ends_with("coarse error"));
        let io: std::io::Error = e.into();
        assert_eq!(io.kind(), io_kind);
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn eof_can_return_a_final_frame_and_then_clean_end() {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;
    let mut codec = cfry::frame::codec(64).unwrap();
    let mut bytes = BytesMut::from(&b"\0\0\0\x01z"[..]);
    assert_eq!(
        codec.decode_eof(&mut bytes).unwrap().unwrap().as_ref(),
        b"z"
    );
    assert!(codec.decode_eof(&mut bytes).unwrap().is_none());
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn clock_changes_during_io_and_tick_overflow_fail_closed() {
    struct Changing {
        clock: Arc<Time>,
        end: u64,
    }
    impl AsyncRead for Changing {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            self.clock.set(self.end);
            Poll::Ready(Ok(0))
        }
    }
    impl AsyncWrite for Changing {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(b.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    for (start, end) in [(5, 4), (5, 15), (u64::MAX - 1, u64::MAX - 1)] {
        let clock = Arc::new(Time::default());
        clock.set(start);
        let mut s = FramedStream::open(
            Changing {
                clock: clock.clone(),
                end,
            },
            Limits {
                max_frame_bytes: 64,
                timeout_ms: 10,
            },
            clock,
        )
        .unwrap();
        assert!(ready(s.next_frame()).is_err());
        assert_eq!(s.state(), State::Failed);
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn closed_push_codec_refuses_a_second_finish() {
    let mut codec = FrameCodec::new(64).unwrap();
    codec.finish().unwrap();
    assert!(codec.finish().is_err());
}
