//! Bounded messages and calls over actual yamux on backpressured byte I/O.
use cfry::{BoxFuture, MessageLimits, Peer, Received, Role, SessionEvent, streams::Clock};
use std::sync::Arc;
use tokio_util::compat::TokioAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
struct Time;
impl Clock for Time {
    fn now(&self) -> u64 {
        0
    }
    fn sleep_until(&self, _: u64) -> BoxFuture<'_, ()> {
        Box::pin(futures::future::pending())
    }
}
fn spawn(driver: BoxFuture<'static, ()>) {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::spawn(driver);
    }
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(driver);
}
fn pair() -> (Peer, Peer) {
    let (a, b) = tokio::io::duplex(128);
    let clock: Arc<dyn Clock> = Arc::new(Time);
    let limits = MessageLimits {
        max_payload: 1024,
        timeout_ms: 1000,
    };
    let (a, ad) = Peer::new(Box::new(a.compat()), Role::Dialer, clock.clone(), limits).unwrap();
    let (b, bd) = Peer::new(Box::new(b.compat()), Role::Listener, clock, limits).unwrap();
    spawn(ad);
    spawn(bd);
    (a, b)
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn real_multiplexed_messages_calls_and_one_use_reply_survive_peer_drop() {
    MessageLimits {
        max_payload: 1024,
        timeout_ms: 1000,
    }
    .validate()
    .unwrap();
    let (a, b) = pair();
    let receive = async move {
        match b.next().await.unwrap() {
            Received::Message(p) => assert_eq!(p, b"one-way"),
            _ => panic!("message expected"),
        }
        let Received::Call { payload, reply } = b.next().await.unwrap() else {
            panic!("call expected")
        };
        assert_eq!(payload, vec![42; 1024]);
        drop(b);
        reply.send(&[7; 1024]).await.unwrap();
    };
    let send = async {
        a.app_message(b"one-way").await.unwrap();
        assert_eq!(a.app_call(&[42; 1024]).await.unwrap(), vec![7; 1024]);
    };
    futures::join!(send, receive);
    assert_eq!(a.next_event().await, Some(SessionEvent::Closed));
    assert_eq!(a.next_event().await, None);
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn refusal_does_not_send_and_dropped_reply_fails_the_call() {
    let (a, b) = pair();
    assert_eq!(
        a.app_message(&[1; 1025]).await.unwrap_err().kind(),
        cfry::ErrorKind::Limit
    );
    assert_eq!(
        a.app_call(&[1; 1025]).await.unwrap_err().kind(),
        cfry::ErrorKind::Limit
    );
    let task = async move {
        let Received::Call { reply, .. } = b.next().await.unwrap() else {
            panic!()
        };
        drop(reply);
    };
    futures::join!(
        async {
            assert!(a.app_call(b"cancelled").await.is_err());
        },
        task
    );
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn reply_bounds_and_clean_close_are_explicit() {
    let (a, b) = pair();
    let task = async move {
        let Received::Call { reply, .. } = b.next().await.unwrap() else {
            panic!()
        };
        assert_eq!(
            reply.send(&[0; 1025]).await.unwrap_err().kind(),
            cfry::ErrorKind::Limit
        );
    };
    futures::join!(
        async {
            assert!(a.app_call(b"").await.is_err());
        },
        task
    );
    let (a, b) = pair();
    a.close().await.unwrap();
    assert!(b.next().await.is_err());
}
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn invalid_budgets_refuse_before_driver_creation() {
    for (maximum, timeout) in [
        (0, 1),
        (cfry::frame::MAX_FRAME_BYTES, 1),
        (1, 0),
        (1, 60001),
    ] {
        let (io, _) = tokio::io::duplex(1);
        assert!(
            Peer::new(
                Box::new(io.compat()),
                Role::Dialer,
                Arc::new(Time),
                MessageLimits {
                    max_payload: maximum,
                    timeout_ms: timeout
                }
            )
            .is_err()
        );
    }
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn one_way_sender_can_drop_after_local_flush() {
    let (a, b) = pair();
    let send = async move {
        a.app_message(b"one-shot").await.unwrap();
        drop(a);
    };
    let receive = async move {
        let Received::Message(payload) = b.next().await.unwrap() else {
            panic!("message expected")
        };
        assert_eq!(payload, b"one-shot");
    };
    futures::join!(send, receive);
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(start_paused = true)]
async fn idle_peer_cannot_hold_the_listener_forever() {
    struct Deadline(tokio::time::Instant);
    impl Clock for Deadline {
        fn now(&self) -> u64 {
            self.0.elapsed().as_millis() as u64
        }
        fn sleep_until(&self, t: u64) -> BoxFuture<'_, ()> {
            Box::pin(tokio::time::sleep_until(
                self.0 + std::time::Duration::from_millis(t),
            ))
        }
    }
    let (a, b) = tokio::io::duplex(32);
    let (peer, driver) = Peer::new(
        Box::new(a.compat()),
        Role::Listener,
        Arc::new(Deadline(tokio::time::Instant::now())),
        MessageLimits {
            max_payload: 32,
            timeout_ms: 100,
        },
    )
    .unwrap();
    spawn(driver);
    assert!(matches!(peer.next().await,Err(e) if e.kind()==cfry::ErrorKind::Timeout));
    drop(b);
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn pending_commands_are_bounded_and_driver_cancellation_closes_once() {
    use futures::FutureExt;
    let (a, _b) = tokio::io::duplex(64);
    let (session, driver) = cfry::Session::new(Box::new(a.compat()), Role::Dialer);
    let mut opens = Vec::new();
    for _ in 0..cfry::MAX_SUBSTREAMS {
        let mut pending = Box::pin(session.open());
        assert!(pending.as_mut().now_or_never().is_none());
        opens.push(pending);
    }
    assert_eq!(
        session.open().await.unwrap_err().kind(),
        cfry::ErrorKind::Limit
    );
    assert_eq!(
        session.close().await.unwrap_err().kind(),
        cfry::ErrorKind::Limit
    );
    drop(opens);
    drop(driver);
    assert_eq!(
        session.open().await.unwrap_err().kind(),
        cfry::ErrorKind::Closed
    );
    assert_eq!(
        session.close().await.unwrap_err().kind(),
        cfry::ErrorKind::Closed
    );
    assert!(session.accept().await.is_err());
    assert_eq!(session.next_event().await, Some(SessionEvent::Closed));
    assert_eq!(session.next_event().await, None);
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn live_substreams_obey_yamux_capacity() {
    // Keep the byte peer alive but unread, so no remote resets can release the
    // streams while we test yamux's local concurrent-stream bound.
    let (a, _b) = tokio::io::duplex(65536);
    let (session, driver) = cfry::Session::new(Box::new(a.compat()), Role::Dialer);
    spawn(driver);
    let mut opened = Vec::new();
    for _ in 0..cfry::MAX_SUBSTREAMS {
        let io = session.open().await.unwrap();
        assert_eq!(format!("{io:?}"), "Substream");
        opened.push(io);
    }
    // Queue both requests before the driver runs: failing the first must close
    // the second with an explicit error and release its command permit.
    let (first, second) = futures::join!(session.open(), session.open());
    assert!(first.is_err());
    assert!(second.is_err());
    assert_eq!(session.next_event().await, Some(SessionEvent::Closed));
    drop(opened);
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn already_received_one_way_payload_survives_sender_session_close() {
    let (a, b) = pair();
    a.app_message(b"complete-before-close").await.unwrap();
    a.close().await.unwrap();
    drop(a);
    let Received::Message(payload) = b.next().await.unwrap() else {
        panic!("complete one-way payload expected")
    };
    assert_eq!(payload, b"complete-before-close");
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn cancelled_queued_commands_keep_their_capacity_until_consumed() {
    use futures::FutureExt;
    let (a, _b) = tokio::io::duplex(65536);
    let (session, mut driver) = cfry::Session::new(Box::new(a.compat()), Role::Dialer);
    for _ in 0..cfry::MAX_SUBSTREAMS * 4 {
        // A cancelled send either remains within channel capacity or is refused
        // by the command-owned permit bound. The driver has not run yet.
        if let Some(result) = session.open().now_or_never() {
            assert_eq!(result.unwrap_err().kind(), cfry::ErrorKind::Limit);
        }
    }
    assert!(driver.as_mut().now_or_never().is_none());
    let mut opening = Box::pin(session.open());
    assert!(opening.as_mut().now_or_never().is_none());
    assert!(driver.as_mut().now_or_never().is_none());
    drop(opening.await.unwrap());
    spawn(driver);
    session.close().await.unwrap();
}
