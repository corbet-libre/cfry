//! One-way messages and request/reply calls over bounded Ferry substreams.
use crate::streams::{Clock, FramedStream, Limits};
use crate::{BoxFuture, BoxStream, Error, ErrorKind, Role, Session, SessionEvent, Substream};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Transport limits, independent of application message meaning.
#[derive(Clone, Copy, Debug)]
pub struct MessageLimits {
    /// Largest opaque payload or reply. Includes neither framing nor request tag.
    pub max_payload: usize,
    /// Deadline for each complete I/O operation, in milliseconds.
    pub timeout_ms: u64,
}
impl MessageLimits {
    /// Check the transport bounds without creating a session or starting I/O.
    pub fn validate(self) -> Result<(), Error> {
        self.framing().map(|_| ())
    }
    fn framing(self) -> Result<Limits, Error> {
        if self.max_payload == 0
            || self.max_payload > crate::frame::MAX_FRAME_BYTES - 16
            || self.timeout_ms == 0
            || self.timeout_ms > 60_000
        {
            return Err(LIMIT);
        }
        Ok(Limits {
            max_frame_bytes: self.max_payload + 16,
            timeout_ms: self.timeout_ms,
        })
    }
    fn check(self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() > self.max_payload {
            Err(LIMIT)
        } else {
            Ok(())
        }
    }
}
const LIMIT: Error = Error::new(ErrorKind::Limit, "message size or deadline out of bounds");
const PROTOCOL: Error = Error::new(ErrorKind::Protocol, "invalid message envelope");

#[derive(Serialize, Deserialize)]
enum Request<'a> {
    Message(#[serde(borrow)] &'a [u8]),
    Call(#[serde(borrow)] &'a [u8]),
}

/// A Tor-internal message session. The caller must drive the returned future.
#[derive(Clone)]
pub struct Peer {
    session: Session,
    clock: Arc<dyn Clock>,
    limits: MessageLimits,
}
impl Peer {
    /// Wrap byte I/O with maintained yamux and length framing.
    pub fn new(
        io: BoxStream,
        role: Role,
        clock: Arc<dyn Clock>,
        limits: MessageLimits,
    ) -> Result<(Self, BoxFuture<'static, ()>), Error> {
        limits.framing()?;
        let (session, driver) = Session::new(io, role);
        Ok((
            Self {
                session,
                clock,
                limits,
            },
            driver,
        ))
    }
    fn frame(&self, io: Substream) -> Result<FramedStream<Substream>, Error> {
        FramedStream::open(io, self.limits.framing()?, self.clock.clone())
    }
    async fn begin(&self, request: Request<'_>) -> Result<FramedStream<Substream>, Error> {
        let payload = match request {
            Request::Message(bytes) | Request::Call(bytes) => bytes,
        };
        self.limits.check(payload)?;
        let bytes = postcard::to_allocvec(&request).map_err(|_| PROTOCOL)?;
        let mut last = self.clock.now();
        let io = crate::streams::within(
            self.clock.as_ref(),
            &mut last,
            self.limits.timeout_ms,
            self.session.open(),
        )
        .await?;
        let mut stream = self.frame(io)?;
        stream.send_frame(&bytes).await?;
        Ok(stream)
    }
    /// Submit one message and finish its transport half-close exchange.
    /// Completion never proves application processing or durable acceptance.
    pub async fn app_message(&self, payload: &[u8]) -> Result<(), Error> {
        let stream = self.begin(Request::Message(payload)).await?;
        drain_sent(stream).await
    }
    /// Submit one request and return one bounded opaque reply. Never retries.
    pub async fn app_call(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let mut stream = self.begin(Request::Call(payload)).await?;
        let reply = stream.next_frame().await?.ok_or(PROTOCOL)?;
        self.limits.check(&reply)?;
        // The complete reply resolves the call. A best-effort FIN releases the
        // sender; cleanup failure cannot discard already received valid bytes.
        let _ = stream.finish().await;
        Ok(reply)
    }
    /// Receive a complete message or request; payload authorization is the caller's job.
    pub async fn next(&self) -> Result<Received, Error> {
        let mut last = self.clock.now();
        let io = crate::streams::within(
            self.clock.as_ref(),
            &mut last,
            self.limits.timeout_ms,
            self.session.accept(),
        )
        .await?;
        let mut stream = self.frame(io)?;
        let bytes = stream.next_frame().await?.ok_or(PROTOCOL)?;
        let request: Request<'_> = postcard::from_bytes(&bytes).map_err(|_| PROTOCOL)?;
        match request {
            Request::Message(payload) => {
                self.limits.check(payload)?;
                // Existing yamux FIN, not an application ACK: release the sender
                // only after consuming its complete frame. Keep valid received
                // bytes even if best-effort cleanup fails.
                let _ = stream.finish().await;
                Ok(Received::Message(payload.to_vec()))
            }
            Request::Call(payload) => {
                self.limits.check(payload)?;
                Ok(Received::Call {
                    payload: payload.to_vec(),
                    reply: Reply {
                        stream: Box::new(stream),
                        limits: self.limits,
                        _session: self.session.clone(),
                    },
                })
            }
        }
    }
    /// One terminal session event across all clones, never a delivery receipt.
    pub async fn next_event(&self) -> Option<SessionEvent> {
        self.session.next_event().await
    }
    /// Close this session and every stream it owns.
    pub async fn close(&self) -> Result<(), Error> {
        self.session.close().await
    }
}

/// One complete inbound transport payload.
pub enum Received {
    /// One-way message; it carries no implicit receipt.
    Message(Vec<u8>),
    /// Request with its nonserializable, one-use reply capability.
    Call {
        /// Opaque request bytes.
        payload: Vec<u8>,
        /// Reply on this call's original stream only.
        reply: Reply,
    },
}
/// One-use reply capability tied to the original call and network.
pub struct Reply {
    stream: Box<FramedStream<Substream>>,
    limits: MessageLimits,
    // Keep the driver alive even after the listener's temporary Peer is dropped.
    _session: Session,
}
impl Reply {
    /// Send exactly one bounded reply. Dropping/cancelling cannot replay it.
    pub async fn send(mut self, payload: &[u8]) -> Result<(), Error> {
        self.limits.check(payload)?;
        self.stream.send_frame(payload).await?;
        drain_sent(*self.stream).await
    }
}

// A yamux Stream flush only queues stream commands. Dropping the whole byte
// connection immediately can make its delayed peer fail before reading them.
// Reuse yamux's FIN in both directions before releasing the original session.
async fn drain_sent(mut stream: FramedStream<Substream>) -> Result<(), Error> {
    stream.finish_writing().await?;
    if stream.next_frame().await?.is_some() {
        return Err(PROTOCOL);
    }
    Ok(())
}
