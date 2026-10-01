//! Browser bindings for Ferry framing.
use crate::frame::{FrameCodec, MAX_FRAME_BYTES};
use js_sys::{Array, Uint8Array};
use wasm_bindgen::prelude::*;

fn failure(error: crate::Error) -> JsValue {
    JsValue::from_str(&format!("cfry:{:?}", error.kind()))
}

/// Push-based length-delimited framing (u32 big-endian length prefix).
#[wasm_bindgen(js_name = FrameCodec)]
pub struct BrowserFrameCodec {
    codec: FrameCodec,
}

#[wasm_bindgen(js_class = FrameCodec)]
impl BrowserFrameCodec {
    /// A codec bounded to `max_frame_bytes` (1 ..= 1 MiB) per frame.
    #[wasm_bindgen(constructor)]
    pub fn new(max_frame_bytes: f64) -> Result<BrowserFrameCodec, JsValue> {
        if !max_frame_bytes.is_finite()
            || max_frame_bytes.fract() != 0.0
            || !(1.0..=MAX_FRAME_BYTES as f64).contains(&max_frame_bytes)
        {
            return Err(JsValue::from_str("cfry:Limit"));
        }
        Ok(Self {
            codec: FrameCodec::new(max_frame_bytes as usize).map_err(failure)?,
        })
    }

    /// Encode one frame.
    pub fn encode(&mut self, payload: &[u8]) -> Result<Vec<u8>, JsValue> {
        self.codec.encode(payload).map_err(failure)
    }

    /// Feed received bytes; returns the completed frames.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Array, JsValue> {
        let frames = Array::new();
        for frame in self.codec.push(chunk).map_err(failure)? {
            frames.push(&Uint8Array::from(frame.as_slice()));
        }
        Ok(frames)
    }

    /// End of stream; valid only between complete frames.
    pub fn finish(&mut self) -> Result<(), JsValue> {
        self.codec.finish().map_err(failure)
    }

    /// Discard state and refuse further use.
    pub fn close(&mut self) {
        self.codec.close();
    }
}
