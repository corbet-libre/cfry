//! Bidirectional binary messages and calls over real yamux [`Peer`]s.
use cfry::{BoxFuture, MessageLimits, Peer, Received, Role, streams::Clock};
use std::sync::Arc;
use tokio_util::compat::TokioAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

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

fn ascending_all_bytes() -> Vec<u8> {
    (0..256u16).map(|i| i as u8).collect()
}

fn descending_all_bytes() -> Vec<u8> {
    (0..256u16).map(|i| 255 - i as u8).collect()
}

fn bound_pattern(xor: u8) -> Vec<u8> {
    (0..1024).map(|i| ((i % 256) as u8) ^ xor).collect()
}

fn call_pattern(mul: u8, add: u8) -> Vec<u8> {
    (0..48u16)
        .map(|i| (i as u8).wrapping_mul(mul).wrapping_add(add))
        .collect()
}

fn reply_pattern(mul: u8, add: u8) -> Vec<u8> {
    (0..32u16)
        .map(|i| (i as u8).wrapping_mul(mul).wrapping_add(add))
        .collect()
}

#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
async fn duplex_binary_messages_and_calls_roundtrip() {
    let empty: Vec<u8> = Vec::new();
    let a_all = ascending_all_bytes();
    let b_all = descending_all_bytes();
    let a_bound = bound_pattern(0xA5);
    let b_bound = bound_pattern(0x5A);
    let call_a = call_pattern(7, 0xA0);
    let call_b = call_pattern(5, 0xB0);
    let reply_a = reply_pattern(11, 0xC0);
    let reply_b = reply_pattern(13, 0xD0);

    let (a, b) = pair();
    let (a_send, a_recv) = (a.clone(), a);
    let (b_send, b_recv) = (b.clone(), b);

    let send_a = {
        let empty = empty.clone();
        let a_all = a_all.clone();
        let a_bound = a_bound.clone();
        let call_a = call_a.clone();
        let reply_a = reply_a.clone();
        let peer = a_send.clone();
        async move {
            peer.app_message(&empty).await.unwrap();
            peer.app_message(&a_all).await.unwrap();
            peer.app_message(&a_bound).await.unwrap();
            assert_eq!(peer.app_call(&call_a).await.unwrap(), reply_a);
        }
    };
    let send_b = {
        let empty = empty.clone();
        let b_all = b_all.clone();
        let b_bound = b_bound.clone();
        let call_b = call_b.clone();
        let reply_b = reply_b.clone();
        let peer = b_send.clone();
        async move {
            peer.app_message(&empty).await.unwrap();
            peer.app_message(&b_all).await.unwrap();
            peer.app_message(&b_bound).await.unwrap();
            assert_eq!(peer.app_call(&call_b).await.unwrap(), reply_b);
        }
    };
    let recv_a = {
        let empty = empty.clone();
        let b_all = b_all.clone();
        let b_bound = b_bound.clone();
        let call_b = call_b.clone();
        let reply_b = reply_b.clone();
        async move {
            let mut messages = Vec::new();
            let mut calls = 0;
            for _ in 0..4 {
                match a_recv.next().await.unwrap() {
                    Received::Message(payload) => messages.push(payload),
                    Received::Call { payload, reply } => {
                        assert_eq!(payload, call_b);
                        calls += 1;
                        reply.send(&reply_b).await.unwrap();
                    }
                }
            }
            let mut expected = vec![empty, b_all, b_bound];
            messages.sort();
            expected.sort();
            assert_eq!(messages, expected);
            assert_eq!(calls, 1);
        }
    };
    let recv_b = {
        let empty = empty.clone();
        let a_all = a_all.clone();
        let a_bound = a_bound.clone();
        let call_a = call_a.clone();
        let reply_a = reply_a.clone();
        async move {
            let mut messages = Vec::new();
            let mut calls = 0;
            for _ in 0..4 {
                match b_recv.next().await.unwrap() {
                    Received::Message(payload) => messages.push(payload),
                    Received::Call { payload, reply } => {
                        assert_eq!(payload, call_a);
                        calls += 1;
                        reply.send(&reply_a).await.unwrap();
                    }
                }
            }
            let mut expected = vec![empty, a_all, a_bound];
            messages.sort();
            expected.sort();
            assert_eq!(messages, expected);
            assert_eq!(calls, 1);
        }
    };

    futures::join!(send_a, send_b, recv_a, recv_b);

    a_send.close().await.unwrap();
    assert!(b_send.next().await.is_err());
    drop(a_send);
    drop(b_send);
}
