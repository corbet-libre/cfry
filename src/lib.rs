//! Ferry: bounded message framing and stream lifecycle below the Tor backend.
#![forbid(unsafe_code)]
#[cfg(target_arch = "wasm32")]
pub mod browser;
mod error;
mod message;
pub use message::{MessageLimits, Peer, Received, Reply};
pub mod frame;
mod session;
pub mod streams;
pub use error::{Error, ErrorKind};
use futures::io::{AsyncRead, AsyncWrite};
pub use session::{MAX_SUBSTREAMS, Role, Session, SessionEvent, Substream};
use std::{future::Future, pin::Pin};
/// Runtime future, Send on native and local in the browser.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// Runtime future, Send on native and local in the browser.
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
/// `Send` on native targets; no bound on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}
/// `Send` on native targets; no bound on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native targets; no bound on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}
/// `Sync` on native targets; no bound on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}

/// A reliable, ordered, bidirectional byte stream with flow control.
///
/// Closing the write half (`poll_close`) signals end of stream to the peer.
/// Payload bytes are opaque: backends never interpret them.
pub trait ByteStream: AsyncRead + AsyncWrite + Unpin + MaybeSend {}
impl<T: AsyncRead + AsyncWrite + Unpin + MaybeSend + ?Sized> ByteStream for T {}

/// An owned byte stream from any backend.
pub type BoxStream = Box<dyn ByteStream>;
