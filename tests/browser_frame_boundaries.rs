//! Frame boundary vectors for the Tor browser byte path.
//!
//! Split, coalesced, truncated, and complete-plus-truncated vectors over the
//! public `cfry::frame::FrameCodec`. Configured for identical native/browser
//! harnesses; no executed browser success is claimed here.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

use cfry::ErrorKind;
use cfry::frame::FrameCodec;

const MAX_PAYLOAD: usize = 512;
const SMALL: &[u8] = b"ferry";

fn encode(payload: &[u8]) -> Vec<u8> {
    FrameCodec::new(MAX_PAYLOAD)
        .unwrap()
        .encode(payload)
        .unwrap()
}

fn assert_closed(codec: &mut FrameCodec) {
    assert_eq!(codec.encode(SMALL).unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(codec.push(SMALL).unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(codec.finish().unwrap_err().kind(), ErrorKind::Protocol);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn every_split_boundary_of_a_small_frame_reproduces_it() {
    let wire = encode(SMALL);
    assert_eq!(wire.len(), SMALL.len() + 4);
    for split in 1..wire.len() {
        let mut codec = FrameCodec::new(MAX_PAYLOAD).unwrap();
        assert!(
            codec.push(&wire[..split]).unwrap().is_empty(),
            "split {split}"
        );
        assert_eq!(
            codec.push(&wire[split..]).unwrap(),
            vec![SMALL.to_vec()],
            "split {split}"
        );
        assert!(codec.finish().is_ok(), "split {split}");
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn coalesced_empty_binary_and_boundary_frames_in_legal_chunks() {
    let binary: Vec<u8> = (0u8..=255).collect();
    let boundary = vec![0xAB; MAX_PAYLOAD];
    let expected = vec![Vec::new(), binary.clone(), boundary.clone()];
    let wire: Vec<u8> = expected.iter().flat_map(|p| encode(p)).collect();
    for chunk in [1usize, 7, 100, MAX_PAYLOAD + 4] {
        assert!(chunk <= MAX_PAYLOAD + 4, "chunk {chunk}");
        let mut codec = FrameCodec::new(MAX_PAYLOAD).unwrap();
        let mut frames = Vec::new();
        for part in wire.chunks(chunk) {
            frames.extend(codec.push(part).unwrap());
        }
        assert_eq!(frames, expected, "chunk {chunk}");
        assert!(codec.finish().is_ok(), "chunk {chunk}");
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn every_truncation_is_protocol_with_clean_controls() {
    let wire = encode(SMALL);
    for end in 0..=wire.len() {
        let mut codec = FrameCodec::new(MAX_PAYLOAD).unwrap();
        let got = codec.push(&wire[..end]).unwrap();
        if end == wire.len() {
            assert_eq!(got, vec![SMALL.to_vec()], "complete frame");
            assert!(codec.finish().is_ok(), "complete frame");
            continue;
        }
        if end == 0 {
            assert!(got.is_empty(), "zero-byte stream");
            assert!(codec.finish().is_ok(), "zero-byte stream");
            continue;
        }
        assert!(got.is_empty(), "truncation {end}");
        assert_eq!(
            codec.finish().unwrap_err().kind(),
            ErrorKind::Protocol,
            "truncation {end}"
        );
        assert_closed(&mut codec);
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn complete_frame_followed_by_truncated_next_is_protocol() {
    let first = encode(b"ok");
    let second = encode(b"second-payload");
    for end in 1..second.len() {
        let mut codec = FrameCodec::new(MAX_PAYLOAD).unwrap();
        assert_eq!(codec.push(&first).unwrap(), vec![b"ok".to_vec()]);
        assert!(
            codec.push(&second[..end]).unwrap().is_empty(),
            "prefix {end}"
        );
        assert_eq!(
            codec.finish().unwrap_err().kind(),
            ErrorKind::Protocol,
            "prefix {end}"
        );
        assert_closed(&mut codec);
    }
    let mut codec = FrameCodec::new(MAX_PAYLOAD).unwrap();
    assert_eq!(codec.push(&first).unwrap(), vec![b"ok".to_vec()]);
    assert_eq!(
        codec.push(&second).unwrap(),
        vec![b"second-payload".to_vec()]
    );
    assert!(codec.finish().is_ok());
}
