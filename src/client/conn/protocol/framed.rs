//! Codec-basd clients which support multiplexed and pipelined protocols
//!
//! This protocol uses a [`Framed`] [sink][futures::Sink] and
//! [stream][futures::Stream] to implement the driver for
//! a [`tower::Service`]. Codecs can handle multiplexing or
//! not (i.e. be pipelined), and usually they can be driven
//! over any underlying IO system. HTTP/1 and HTTP/2 could both
//! be implemented as Codecs, though `hyper` does not take
//! this approach.
//!
//! # Integrating Codecs with [tower::Service]
//!
//! Codecs are a useful abstraction, but they don't necessarily fit
//! well with the conventional model for tower Servers and Clients.
//!
//! This module provides a [FramedProtocol] and [FramedConnection]
//! which cooperate with the other protocol and connection abstractions
//! in this library to build clients. The [FramedProtocol] can
//! be used with the client builder to create a tower-based Client
//! which appropriately sets up the protocol, and sends requests via
//! a transport on that protocol.
//!
//! # Multiplexing and tags
//!
//! The framed protocol here is designed for multiplexed protocols,
//! where responses can be recieved in any order, not necessarily
//! the order in which they were sent.
//!
//! As such, requests and responses must implement the [`Tagged`]
//! trait, and return an identifier that can be used to match a
//! request to a response.
//!
//! # Driving connections
//!
//! By default, connections only make progress processing requests and
//! responses when at least one response future is polled. No request is sent
//! until its response future is polled at least once. In cases where
//! this might not happen (e.g. fire-and-forget requests, or requests sent,
//! and then the task proceeds to do other work before polling for a response)
//! it is important to have some task which is polling for responses and driving
//! the connection forward. This can be done by getting a [ConnectionDriver]
//! via the [`driver`](FramedConnection::driver) method, and then spawning that driver onto a runtime.

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug};
use std::future::{Ready, ready};
use std::hash::Hash;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker, ready};

use futures::{Sink, SinkExt, Stream, StreamExt as _, task::AtomicWaker, task::noop_waker};
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{trace, warn};

use crate::client::conn::Connection;
use crate::info::HasConnectionInfo;

use super::Protocol;

/// A trait which represents the identifiers for multiplexed messages.
pub trait Tagged {
    /// The identifier type for a multiplexed message.
    type Tag: Eq + Clone + Debug + Hash;

    /// Returns the identifier type for a multiplexed message.
    fn tag(&self) -> Self::Tag;
}

/// Impl provided for tuples sent to addressed connections (like UDP)
impl<R, A> Tagged for (R, A)
where
    R: Tagged,
{
    type Tag = R::Tag;

    fn tag(&self) -> Self::Tag {
        self.0.tag()
    }
}

/// A protocol based on a framing codec
#[derive(Debug)]
pub struct FramedProtocol<C, Req, Res> {
    codec: C,
    messages: PhantomData<fn(Req) -> Res>,
}

impl<C, Req, Res> FramedProtocol<C, Req, Res> {
    /// Create a new framed protocol`
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            messages: PhantomData,
        }
    }
}

impl<C, Req, Res> Clone for FramedProtocol<C, Req, Res>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            messages: PhantomData,
        }
    }
}

impl<C, Req, Res, IO> tower::Service<IO> for FramedProtocol<C, Req, Res>
where
    C: Decoder<Item = Res> + Encoder<Req, Error = <C as Decoder>::Error> + Clone + Send + 'static,
    <C as Decoder>::Error: From<io::Error> + std::error::Error + Send + Sync + 'static,
    Res: Tagged,
    IO: HasConnectionInfo + AsyncRead + AsyncWrite + Send + 'static,
    Req: Tagged + Send + 'static,
    Res: Tagged<Tag = Req::Tag> + Send + 'static,
    Req::Tag: Send + 'static,
{
    type Error = <C as Decoder>::Error;
    type Response = FramedConnection<Framed<IO, C>, Req, Res>;
    type Future = Ready<Result<Self::Response, <C as Decoder>::Error>>;

    fn call(&mut self, request: IO) -> <Self as Protocol<IO, Req>>::Future {
        ready(Ok(FramedConnection::new(Framed::new(
            request,
            self.codec.clone(),
        ))))
    }

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), <Self as Protocol<IO, Req>>::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// A connection built on a codec and an IO type
pub struct FramedConnection<C, Req, Res>
where
    Res: Tagged,
{
    inner: Arc<InnerProtocol<C, Req, Res>>,
}

impl<C, Req, Res> Clone for FramedConnection<C, Req, Res>
where
    Res: Tagged,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C, Req, Res> fmt::Debug for FramedConnection<C, Req, Res>
where
    Res: Tagged,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FramedProtocol").finish()
    }
}

impl<C, Req, Res> FramedConnection<C, Req, Res>
where
    Res: Tagged,
{
    /// Create a new framed protocol
    pub fn new(codec: C) -> Self {
        Self {
            inner: Arc::new(InnerProtocol::new(codec)),
        }
    }

    /// Send a message, and return a response future representing the resposne to that message.
    ///
    /// The response future must be polled to make progress on the request.
    pub fn send(&self, message: Req) -> ResponseFuture<C, Req, Res>
    where
        Req: Tagged<Tag = Res::Tag>,
    {
        ResponseFuture::new(Arc::clone(&self.inner), message)
    }

    /// Get a connection driver which can be spawned to a background thread.
    ///
    /// The driver should be polled to make progress on the connection. If it is not polled,
    /// or it is dropped, the connection will resort to only making progress when a response
    /// future is polled. For unordered connections (e.g. UDP), this might result in a backlog of
    /// responses in response buffer.
    pub fn driver(&self) -> ConnectionDriver<C, Req, Res> {
        ConnectionDriver::new(Arc::clone(&self.inner))
    }
}

impl<C, Req, Res> Connection<Req> for FramedConnection<C, Req, Res>
where
    Req: Tagged + Send + 'static,
    Res: Tagged<Tag = Req::Tag> + Send + 'static,
    Req::Tag: Send + 'static,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>> + Send + 'static,
    C::Error: From<io::Error> + std::error::Error + Send + Sync + 'static,
{
    type Response = Res;
    type Error = C::Error;
    type Future = ResponseFuture<C, Req, Res>;

    fn send_request(&mut self, request: Req) -> Self::Future {
        ResponseFuture::new(Arc::clone(&self.inner), request)
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
}

impl<C, Req, Res> tower::Service<Req> for FramedConnection<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
    C::Error: From<io::Error>,
{
    type Response = Res;
    type Error = C::Error;
    type Future = ResponseFuture<C, Req, Res>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        trace!("Creating response future");
        ResponseFuture::new(Arc::clone(&self.inner), req)
    }
}

/// Future returned by FramedProtocol to represent waiting for a response.
///
/// This future must be used in some form to send the request and receive the response, even
/// if a connection driver is spawned in the background. This is a consequence of three things:
///
/// 1. Rust futures do not do work until they are polled.
/// 2. There is no internal queue to store messages. If you'd like to use a queue, consider
/// the appropriate tower middleware.
/// 3. Framed codecs must be polled to readiness to be ready to send a message. If the codec or underlying
/// connection is still doing work from a previous message, a new message cannot be sent.
///
/// There is a heap-allocated queue which holds wakers for pending requests, but wakers
/// are just a fat pointer, so the heap usage of this will not pile up, and the sending future retains control of the message
/// memory.
#[pin_project::pin_project(PinnedDrop)]
#[must_use = "futures do nothing unless polled"]
pub struct ResponseFuture<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
{
    inner: Arc<InnerProtocol<C, Req, Res>>,
    tag: Req::Tag,
    message: Option<Req>,
}

impl<C, Req, Res> fmt::Debug for ResponseFuture<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture")
            .field("tag", &self.tag)
            .finish()
    }
}

impl<C, Req, Res> ResponseFuture<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
{
    fn new(inner: Arc<InnerProtocol<C, Req, Res>>, message: Req) -> Self {
        Self {
            inner,
            tag: message.tag(),
            message: Some(message),
        }
    }
}

impl<C, Req, Res> ResponseFuture<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
    C::Error: From<io::Error>,
{
    /// Drive this future long enough to enqueue the request.
    ///
    /// This async method will drive the future far enough such that the request is sent to the codec for processing.
    /// At this point, if a [`FramedConnection::driver`] is spawned on the runtime, it can take over driving the request-
    /// response cycle. Without doing this, the request will never be sent (since Rust futures do not do any work until polled).
    pub async fn enqueue(&mut self) -> Result<(), C::Error> {
        std::future::poll_fn(|cx| Pin::new(&self.inner).poll_send(cx, &mut self.message)).await?;
        Ok(())
    }

    /// Attempt to enqueue the request without yielding to the runtime.
    ///
    /// If this method returns Ok(false), then the message is *NOT* enqueued, and something must make progress
    /// polling the underlying codec before this message will be sent. See the [`FramedConnection::driver`]
    /// method for more info on driving the connection from another task.
    pub fn try_enqueue_request(&mut self) -> Result<bool, C::Error> {
        self.inner.try_send(&mut self.message)
    }

    /// Check if the request has already been sent to the codec for processing.
    pub fn is_request_enqueued(&self) -> bool {
        self.message.is_none()
    }
}

impl<C, Req, Res> Future for ResponseFuture<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
    C::Error: From<io::Error>,
{
    type Output = Result<Res, C::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _span = tracing::trace_span!("response.poll", tag=?this.tag).entered();
        trace!("poll response");
        ready!(this.inner.poll_request(cx, this.message))?;
        this.inner.poll_response(cx, this.tag)
    }
}

#[pin_project::pinned_drop]
impl<C, Res, Req> PinnedDrop for ResponseFuture<C, Res, Req>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
{
    fn drop(self: Pin<&mut Self>) {
        self.inner.inbox.remove(&self.tag);
    }
}

/// A driver just polls the connection in the background,
/// receiving messages, and notifying other response handles
/// when messages are received.
pub struct ConnectionDriver<C, Req, Res>
where
    Res: Tagged,
{
    inner: Arc<InnerProtocol<C, Req, Res>>,
}

impl<C, Req, Res> ConnectionDriver<C, Req, Res>
where
    Res: Tagged,
{
    fn new(protocol: Arc<InnerProtocol<C, Req, Res>>) -> Self {
        Self { inner: protocol }
    }
}

impl<C, Req, Res> fmt::Debug for ConnectionDriver<C, Req, Res>
where
    Res: Tagged,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionDriver").finish()
    }
}

impl<C, Req, Res> IntoFuture for ConnectionDriver<C, Req, Res>
where
    Res: Tagged + Unpin,
    Req: Tagged<Tag = Res::Tag>,
    Res::Tag: Unpin,
    C: Stream<Item = Result<Res, C::Error>> + Sink<Req>,
{
    type Output = Result<(), C::Error>;
    type IntoFuture = ConnectionDriverFuture<C, Req, Res>;

    fn into_future(self) -> Self::IntoFuture {
        ConnectionDriverFuture {
            inner: self.inner,
            codec: None,
        }
    }
}

/// Future returned to drive a connection in a thread.
#[pin_project::pin_project(PinnedDrop)]
pub struct ConnectionDriverFuture<C, Req, Res>
where
    Res: Tagged,
{
    inner: Arc<InnerProtocol<C, Req, Res>>,
    codec: Option<ArcMutexGuard<RawMutex, Pin<Box<C>>>>,
}

impl<C, Req, Res> fmt::Debug for ConnectionDriverFuture<C, Req, Res>
where
    Res: Tagged,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionDriverFuture").finish()
    }
}

#[pin_project::pinned_drop]
impl<C, Req, Res> PinnedDrop for ConnectionDriverFuture<C, Req, Res>
where
    Res: Tagged,
{
    fn drop(self: Pin<&mut Self>) {
        if let Some(waker) = self
            .inner
            .send_queue
            .try_lock()
            .and_then(|mut g| g.queue.pop_front())
        {
            // Wake up a queued sender to make progress.
            waker.wake();
        } else {
            // Wake one other task up to make progress.
            self.inner.inbox.wake_one();
        }
    }
}

impl<C, Req, Res> Future for ConnectionDriverFuture<C, Req, Res>
where
    Res: Tagged + Unpin,
    Req: Tagged<Tag = Res::Tag>,
    Res::Tag: Unpin,
    C: Stream<Item = Result<Res, C::Error>> + Sink<Req>,
{
    type Output = Result<(), C::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _span = tracing::trace_span!("driver.poll").entered();

        let pc = self.as_mut().project();
        let codec = {
            if pc.codec.is_none() {
                *pc.codec = Some(pc.inner.codec.lock_arc());
            }
            pc.codec.as_mut().unwrap()
        };

        let out = ready!(pc.inner.poll_drive_connection(cx, codec));
        pc.codec.take();
        Poll::Ready(out)
    }
}

enum Inflight<M> {
    Pending(Waker),
    Response(M),
    Tombstone,
}

impl<M> Inflight<M> {
    fn is_pending(&self) -> bool {
        matches!(self, Self::Pending(_))
    }
}

#[pin_project::pin_project]
struct SendQueue<M> {
    // Next message to send, plus a waker to let the sending
    // task know it should start polling for a response.
    pending: Option<(M, Waker)>,

    // Notify when a new spot opens up.
    queue: VecDeque<Waker>,
}

struct InnerProtocol<C, Req, Res>
where
    Res: Tagged,
{
    inbox: Inbox<Res>,
    send_queue: Mutex<SendQueue<Req>>,
    notify: AtomicWaker,
    codec: Arc<Mutex<Pin<Box<C>>>>,
    ready: AtomicBool,
    response: PhantomData<fn() -> Res>,
}

impl<C, Req, Res> InnerProtocol<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
{
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), C::Error>> {
        if self.ready.load(Ordering::Relaxed) {
            trace!("poll ready already done");
            return Poll::Ready(Ok(()));
        };

        if let Some(mut codec) = self.codec.try_lock() {
            trace!("poll ready codec");
            if let Err(error) = ready!(codec.poll_ready_unpin(cx)) {
                return Poll::Ready(Err(error));
            }
            self.ready.store(true, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        } else {
            trace!("poll ready queue");
            let mut send_queue = self.send_queue.lock();
            if send_queue.pending.is_none() {
                trace!("pending slot available");
                return Poll::Ready(Ok(()));
            }
            trace!("enqueued waiter");
            send_queue.queue.push_back(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Attempt to send the message immediately, without polling or waiting.
    ///
    /// When a driver is driving this protocol elsewhere, this can be used to enque a message.
    fn try_send(&self, item: &mut Option<Req>) -> Result<bool, C::Error> {
        if let Some(mut codec) = self.codec.try_lock() {
            if !self.ready.load(Ordering::Acquire) {
                return Ok(false);
            }

            let message = item.take().expect("message stolen");
            let tag = message.tag();
            trace!(?tag, "start message send");
            codec.start_send_unpin(message)?;
            self.ready.store(false, Ordering::Release);
            let mut send_queue = self.send_queue.lock();
            if let Some(waker) = send_queue.queue.pop_front() {
                // Notify another task which was waiting for the slot to send messages.
                trace!("wake next in queue");
                waker.wake();
            }
            Ok(true)
        } else {
            let mut send_queue = self.send_queue.lock();
            if send_queue.pending.is_some() {
                return Ok(false);
            }
            let message = item.take().expect("message stolen");
            let waker = noop_waker();
            send_queue.pending = Some((message, waker));
            self.notify.wake();
            Ok(true)
        }
    }

    fn poll_send(
        &self,
        cx: &mut Context<'_>,
        item: &mut Option<Req>,
    ) -> Poll<Result<bool, C::Error>> {
        debug_assert!(item.is_some(), "poll_send without message for queue?");
        if let Some(mut codec) = self.codec.try_lock() {
            trace!("codec unlocked, no driver");

            if !self.ready.load(Ordering::Acquire) {
                if let Err(error) = ready!(codec.poll_ready_unpin(cx)) {
                    return Poll::Ready(Err(error));
                }
            }

            let message = item.take().expect("message stolen");
            let tag = message.tag();
            trace!(?tag, "start message send");
            codec.start_send_unpin(message)?;
            self.ready.store(false, Ordering::Release);
            self.inbox.pending_response(tag, cx.waker());
            let mut send_queue = self.send_queue.lock();
            if let Some(waker) = send_queue.queue.pop_front() {
                // Notify another task which was waiting for the slot to send messages.
                trace!("wake next in queue");
                waker.wake();
            }
            Poll::Ready(Ok(true))
        } else {
            trace!("codec locked, using queue");
            let mut send_queue = self.send_queue.lock();
            if send_queue.pending.is_some() {
                send_queue.queue.push_back(cx.waker().clone());
                Poll::Ready(Ok(false))
            } else {
                send_queue.pending =
                    Some((item.take().expect("stolen message"), cx.waker().clone()));
                self.notify.wake();
                Poll::Ready(Ok(false))
            }
        }
    }

    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Res, C::Error>>> {
        if let Some(mut codec) = self.codec.try_lock() {
            (*codec).poll_next_unpin(cx)
        } else {
            Poll::Pending
        }
    }

    fn poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), C::Error>> {
        if let Some(mut codec) = self.codec.try_lock() {
            match (*codec).poll_flush_unpin(cx) {
                Poll::Ready(Ok(())) => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

impl<C, Req, Res> InnerProtocol<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
    C::Error: From<io::Error>,
{
    fn poll_request(
        &self,
        cx: &mut Context<'_>,
        message: &mut Option<Req>,
    ) -> Poll<Result<(), C::Error>> {
        if message.is_some() {
            trace!("poll response send");
            if !ready!(self.poll_send(cx, message))? {
                trace!("poll send placed in queue");
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_response(&self, cx: &mut Context<'_>, tag: &Req::Tag) -> Poll<Result<Res, C::Error>> {
        loop {
            trace!("poll response check inbox");
            if let Some(message) = self.inbox.check_inbox(tag) {
                return Poll::Ready(Ok(message));
            }

            trace!("poll response recv");
            match self.poll_next(cx) {
                Poll::Ready(Some(Ok(message))) => {
                    let tag = message.tag();
                    trace!(?tag, "Received message");
                    self.inbox.recieve(message);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "Connection closed",
                    )
                    .into()));
                }
                Poll::Pending => break,
            }
        }

        match self.poll_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<C, Req, Res> InnerProtocol<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
{
    fn poll_drive_connection(
        &self,
        cx: &mut Context<'_>,
        codec: &mut Pin<Box<C>>,
    ) -> Poll<Result<(), C::Error>> {
        trace!("polling connection driver");
        loop {
            self.notify.register(cx.waker());

            // Check for messages to send first.
            if let Some(mut send_queue) = self.send_queue.try_lock() {
                trace!("checking send queue");
                if send_queue.pending.is_some() {
                    if !self.ready.load(Ordering::Acquire) {
                        if let Err(error) = ready!(codec.poll_ready_unpin(cx)) {
                            return Poll::Ready(Err(error));
                        }
                    }

                    let (message, waker) = send_queue.pending.take().expect("message stolen");
                    let tag = message.tag();
                    trace!(?tag, "sending pending message from slot");

                    codec.start_send_unpin(message)?;
                    self.inbox.pending_response(tag, &waker);
                    self.ready.store(false, Ordering::Release);
                    if let Some(waker) = send_queue.queue.pop_front() {
                        // Notify another task which was waiting for the slot to send messages.
                        waker.wake();
                    }
                }
            } else {
                trace!("send queue locked");
                // If nothing needs to be sent right away, make progress on writes anyways.
                if let Err(error) = ready!(codec.poll_flush_unpin(cx)) {
                    return Poll::Ready(Err(error));
                }
            }

            trace!("checking recv stream");
            match codec.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(message))) => {
                    let tag = message.tag();
                    trace!(?tag, "Recieved message");
                    self.inbox.recieve(message);
                }
                Poll::Ready(Some(Err(error))) => {
                    trace!("Connection driver encountered stream error");
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(None) => {
                    trace!("Finished polling connection driver, stream ended");
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    if let Err(error) = ready!(codec.poll_flush_unpin(cx)) {
                        return Poll::Ready(Err(error));
                    }
                    trace!("pending next recv");
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<C, Req, Res> InnerProtocol<C, Req, Res>
where
    Res: Tagged,
{
    fn new(codec: C) -> Self {
        Self {
            inbox: Inbox::new(),
            send_queue: Mutex::new(SendQueue {
                pending: None,
                queue: VecDeque::with_capacity(0),
            }),
            notify: AtomicWaker::new(),
            ready: AtomicBool::new(false),
            codec: Arc::new(Mutex::new(Box::pin(codec))),
            response: Default::default(),
        }
    }
}

struct Inbox<M>
where
    M: Tagged,
{
    items: Mutex<HashMap<M::Tag, Inflight<M>>>,
}

impl<M> Default for Inbox<M>
where
    M: Tagged,
{
    fn default() -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
        }
    }
}

impl<M> Inbox<M>
where
    M: Tagged,
{
    fn new() -> Self {
        Self::default()
    }

    /// Called to mark a response as pending and update the waker held here.
    fn pending_response(&self, tag: M::Tag, waker: &Waker) {
        let mut inbox = self.items.lock();
        match inbox.get_mut(&tag) {
            Some(Inflight::Pending(pending)) => pending.clone_from(waker),
            Some(Inflight::Response(_)) => {
                // We shouldn't allow two identically tagged responses to fire anyways.
                panic!("Waker for response already received");
            }
            Some(target @ Inflight::Tombstone) => *target = Inflight::Pending(waker.clone()),
            None => {
                inbox.insert(tag, Inflight::Pending(waker.clone()));
            }
        }
    }

    fn check_inbox(&self, tag: &M::Tag) -> Option<M> {
        let mut inbox = self.items.lock();
        match inbox.get_mut(tag) {
            Some(inflight @ Inflight::Response(_)) => {
                let Inflight::Response(response) = std::mem::replace(inflight, Inflight::Tombstone)
                else {
                    panic!("inflight changed");
                };

                Some(response)
            }
            _ => None,
        }
    }

    fn wake_one(&self) {
        let inbox = self.items.lock();
        if let Some(waker) = inbox.values().find_map(|inflight| match inflight {
            Inflight::Pending(waker) => Some(waker),
            Inflight::Response(_) => None,
            Inflight::Tombstone => None,
        }) {
            waker.wake_by_ref();
        }
    }

    fn remove(&self, tag: &M::Tag) {
        let mut inbox = self.items.lock();
        inbox.remove(tag);
    }

    fn recieve(&self, message: M) {
        let tag = message.tag();
        let mut inbox = self.items.lock();
        match inbox.get_mut(&tag) {
            Some(entry) => {
                if entry.is_pending() {
                    let Inflight::Pending(waker) =
                        std::mem::replace(entry, Inflight::Response(message))
                    else {
                        unreachable!("We just checked above");
                    };
                    trace!(?tag, "Message received, waking task");
                    waker.wake();
                }
            }
            None => {
                warn!(?tag, "Dropping unknown/unrequested message");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{Sink, Stream};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use static_assertions::assert_impl_all;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestTag(u32);

    #[derive(Debug, Clone)]
    struct TestRequest {
        id: u32,
        #[allow(dead_code)]
        data: String,
    }

    impl Tagged for TestRequest {
        type Tag = TestTag;
        fn tag(&self) -> Self::Tag {
            TestTag(self.id)
        }
    }

    #[derive(Debug, Clone)]
    struct TestResponse {
        id: u32,
        result: String,
    }

    impl Tagged for TestResponse {
        type Tag = TestTag;
        fn tag(&self) -> Self::Tag {
            TestTag(self.id)
        }
    }

    #[derive(Debug)]
    struct MockCodec {
        requests: VecDeque<TestRequest>,
        responses: VecDeque<Result<TestResponse, io::Error>>,
        ready: bool,
    }

    impl Clone for MockCodec {
        fn clone(&self) -> Self {
            Self {
                requests: VecDeque::new(),
                responses: VecDeque::new(),
                ready: self.ready,
            }
        }
    }

    impl MockCodec {
        fn new() -> Self {
            Self {
                requests: VecDeque::new(),
                responses: VecDeque::new(),
                ready: true,
            }
        }

        #[allow(dead_code)]
        fn add_response(&mut self, response: TestResponse) {
            self.responses.push_back(Ok(response));
        }
    }

    impl Sink<TestRequest> for MockCodec {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn start_send(mut self: Pin<&mut Self>, item: TestRequest) -> Result<(), Self::Error> {
            self.requests.push_back(item);
            self.ready = false;
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Stream for MockCodec {
        type Item = Result<TestResponse, io::Error>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(response) = self.responses.pop_front() {
                Poll::Ready(Some(response))
            } else {
                Poll::Pending
            }
        }
    }

    impl Unpin for MockCodec {}

    assert_impl_all!(TestTag: Eq, Clone, Debug, Hash);
    assert_impl_all!(TestRequest: Tagged);
    assert_impl_all!(TestResponse: Tagged);
    assert_impl_all!(FramedProtocol<MockCodec, TestRequest, TestResponse>: Send, Sync, Clone);

    #[test]
    fn test_tagged_trait() {
        let request = TestRequest {
            id: 42,
            data: "test".to_string(),
        };
        assert_eq!(request.tag(), TestTag(42));

        let response = TestResponse {
            id: 42,
            result: "success".to_string(),
        };
        assert_eq!(response.tag(), TestTag(42));
    }

    #[test]
    fn test_tagged_tuple() {
        let request = TestRequest {
            id: 1,
            data: "test".to_string(),
        };
        let addr = "127.0.0.1:8080";
        let tuple = (request.clone(), addr);

        assert_eq!(tuple.tag(), TestTag(1));
    }

    #[test]
    fn test_framed_protocol_new() {
        let codec = MockCodec::new();
        let protocol = FramedProtocol::<_, TestRequest, TestResponse>::new(codec);

        let debug_str = format!("{protocol:?}");
        assert!(debug_str.contains("FramedProtocol"));
    }

    #[test]
    fn test_framed_protocol_clone() {
        let codec = MockCodec::new();
        let protocol = FramedProtocol::<_, TestRequest, TestResponse>::new(codec);
        let cloned = protocol.clone();

        let debug1 = format!("{protocol:?}");
        let debug2 = format!("{cloned:?}");
        assert_eq!(debug1, debug2);
    }

    #[test]
    fn test_framed_connection_new() {
        let codec = MockCodec::new();
        let connection = FramedConnection::<_, TestRequest, TestResponse>::new(codec);

        let debug_str = format!("{connection:?}");
        assert!(debug_str.contains("FramedProtocol"));
    }

    #[test]
    fn test_framed_connection_clone() {
        let codec = MockCodec::new();
        let connection = FramedConnection::<_, TestRequest, TestResponse>::new(codec);
        let cloned = connection.clone();

        let debug1 = format!("{connection:?}");
        let debug2 = format!("{cloned:?}");
        assert_eq!(debug1, debug2);
    }

    #[test]
    fn test_response_future_debug() {
        let codec = MockCodec::new();
        let connection = FramedConnection::<_, TestRequest, TestResponse>::new(codec);

        let request = TestRequest {
            id: 1,
            data: "test".to_string(),
        };

        let future = connection.send(request);
        let debug_str = format!("{future:?}");

        assert!(debug_str.contains("ResponseFuture"));
        assert!(debug_str.contains("tag"));
    }

    #[test]
    fn test_connection_driver_debug() {
        let codec = MockCodec::new();
        let connection = FramedConnection::<_, TestRequest, TestResponse>::new(codec);

        let driver = connection.driver();
        let debug_str = format!("{driver:?}");

        assert!(debug_str.contains("ConnectionDriver"));
    }

    #[test]
    fn test_connection_driver_future_debug() {
        let codec = MockCodec::new();
        let connection = FramedConnection::<_, TestRequest, TestResponse>::new(codec);

        let driver = connection.driver();
        let future = driver.into_future();
        let debug_str = format!("{future:?}");

        assert!(debug_str.contains("ConnectionDriverFuture"));
    }

    #[test]
    fn test_inbox_operations() {
        let inbox: Inbox<TestResponse> = Inbox::new();
        let tag = TestTag(1);

        let waker = Waker::noop();
        inbox.pending_response(tag.clone(), waker);

        assert!(inbox.check_inbox(&tag).is_none());

        let response = TestResponse {
            id: 1,
            result: "test".to_string(),
        };

        inbox.recieve(response.clone());

        let received = inbox.check_inbox(&tag);
        assert!(received.is_some());
        assert_eq!(received.unwrap().result, "test");

        assert!(inbox.check_inbox(&tag).is_none());
    }

    #[test]
    fn test_inbox_remove() {
        let inbox: Inbox<TestResponse> = Inbox::new();
        let tag = TestTag(1);

        let waker = Waker::noop();
        inbox.pending_response(tag.clone(), waker);

        inbox.remove(&tag);

        assert!(inbox.check_inbox(&tag).is_none());
    }

    #[test]
    fn test_inbox_wake_one() {
        let inbox: Inbox<TestResponse> = Inbox::new();
        inbox.wake_one();
    }

    #[test]
    fn test_inflight_is_pending() {
        let waker = Waker::noop();
        let pending = Inflight::<TestResponse>::Pending(waker.clone());
        assert!(pending.is_pending());

        let response = TestResponse {
            id: 1,
            result: "test".to_string(),
        };
        let response_inflight = Inflight::Response(response);
        assert!(!response_inflight.is_pending());

        let tombstone = Inflight::<TestResponse>::Tombstone;
        assert!(!tombstone.is_pending());
    }

    #[test]
    fn test_response_future_enqueue_methods() {
        let codec = MockCodec::new();
        let connection = FramedConnection::<_, TestRequest, TestResponse>::new(codec);

        let request = TestRequest {
            id: 1,
            data: "test".to_string(),
        };

        let mut future = connection.send(request);

        assert!(!future.is_request_enqueued());

        let result = future.try_enqueue_request();
        assert!(result.is_ok());
    }
}
