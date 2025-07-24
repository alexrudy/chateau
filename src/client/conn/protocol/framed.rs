//! Codec-basd clients which support multiplexed protocols
//!

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug};
use std::future::{Ready, ready};
use std::hash::Hash;
use std::io;
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker, ready};

use futures::{Sink, SinkExt, Stream, StreamExt as _, task::AtomicWaker};
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
    /// future is polled. For some connections (e.g. UDP), this might result in a backlog of
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
/// This future must be awaited to send the request and receive the response, even
/// if a connection driver is spawned in the background.
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

impl<C, Req, Res> Future for ResponseFuture<C, Req, Res>
where
    Req: Tagged,
    Res: Tagged<Tag = Req::Tag>,
    C: Sink<Req> + Stream<Item = Result<Res, C::Error>>,
    C::Error: From<io::Error>,
{
    type Output = Result<Res, C::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let _span = tracing::trace_span!("response.poll", tag=?this.tag).entered();
        trace!("poll response");
        if this.message.is_some() {
            trace!("poll response send");
            if !ready!(this.inner.poll_send(cx, &mut this.message))? {
                trace!("poll send placed in queue");
                // return Poll::Pending;
            }
        }
        loop {
            trace!("poll response check inbox");
            if let Some(message) = this.inner.inbox.check_inbox(&this.tag) {
                return Poll::Ready(Ok(message));
            }

            trace!("poll response recv");
            match this.inner.poll_next(cx) {
                Poll::Ready(Some(Ok(message))) => {
                    let tag = message.tag();
                    trace!(?tag, "Received message");
                    this.inner.inbox.recieve(message);
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

        match this.inner.poll_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
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
    // task know it should be polled again.
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
            (&mut *codec).poll_next_unpin(cx)
        } else {
            Poll::Pending
        }
    }

    fn poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), C::Error>> {
        if let Some(mut codec) = self.codec.try_lock() {
            match (&mut *codec).poll_flush_unpin(cx) {
                Poll::Ready(Ok(())) => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }

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
        inbox
            .values()
            .filter_map(|inflight| match inflight {
                Inflight::Pending(waker) => Some(waker),
                Inflight::Response(_) => None,
                Inflight::Tombstone => None,
            })
            .next()
            .map(|waker| waker.wake_by_ref());
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
