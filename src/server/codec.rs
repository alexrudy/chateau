//! Codec-backed server protocol
//!
//! This protocol uses a [`Framed`] sink and stream to implement the driver for
//! a [`tower::Service`].

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{Sink, Stream, TryStream};

use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, trace, warn};

use super::{Connection, Protocol};

/// A protocol for serving framed, codec-based services.
#[derive(Debug, Clone)]
pub struct FramedProtocol<C> {
    codec: C,
    multiplex: bool,
}

impl<C> FramedProtocol<C> {
    /// Create a new framed protocol from a codec
    pub fn new(codec: C, multiplex: bool) -> Self {
        Self { codec, multiplex }
    }
}

impl<S, IO, C, Req> Protocol<S, IO, Req> for FramedProtocol<C>
where
    S: tower::Service<Req> + 'static,
    S::Error: fmt::Display + Into<Box<dyn std::error::Error + Send + Sync>> + From<io::Error>,
    S::Response: Send + 'static,
    C: Decoder<Item = Req, Error = S::Error>
        + Encoder<S::Response, Error = S::Error>
        + Clone
        + 'static,
    IO: AsyncRead + AsyncWrite + 'static,
    Req: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Connection = FramedConnection<S, Framed<IO, C>, Req>;

    fn serve_connection(&self, stream: IO, service: S) -> Self::Connection {
        FramedConnection::framed(service, stream, self.codec.clone(), self.multiplex)
    }
}

enum PendingTasks<F>
where
    F: Future,
{
    Ordered(FuturesOrdered<F>),
    Unordered(FuturesUnordered<F>),
}

impl<F> PendingTasks<F>
where
    F: Future,
{
    fn new(multiplex: bool) -> Self {
        if multiplex {
            PendingTasks::Unordered(FuturesUnordered::new())
        } else {
            PendingTasks::Ordered(FuturesOrdered::new())
        }
    }

    fn push(&mut self, future: F) {
        match self {
            PendingTasks::Ordered(pending) => pending.push_back(future),
            PendingTasks::Unordered(pending) => pending.push(future),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            PendingTasks::Ordered(pending) => pending.is_empty(),
            PendingTasks::Unordered(pending) => pending.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match self {
            PendingTasks::Ordered(pending) => pending.len(),
            PendingTasks::Unordered(pending) => pending.len(),
        }
    }
}

impl<F> Stream for PendingTasks<F>
where
    F: Future,
{
    type Item = F::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            PendingTasks::Ordered(pending) => Pin::new(pending).poll_next(cx),
            PendingTasks::Unordered(pending) => Pin::new(pending).poll_next(cx),
        }
    }
}

/// A connection type that uses a Codec to frame
/// reads and writes from a stream.
#[pin_project::pin_project]
pub struct FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
{
    service: S,
    service_ready: bool,
    #[pin]
    framed: F,
    framed_send_ready: bool,

    #[pin]
    tasks: PendingTasks<S::Future>,
    cancelled: bool,
}

impl<S, F, Req> FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
{
    /// Create a new codec server
    pub fn new(service: S, framed: F, multiplex: bool) -> Self {
        Self {
            service,
            service_ready: false,
            framed,
            framed_send_ready: false,
            tasks: PendingTasks::new(multiplex),
            cancelled: false,
        }
    }

    /// Get the number of in-flight requests for this connection
    pub fn in_flight_requests(&self) -> usize {
        self.tasks.len()
    }
}

impl<S, F, Req> Connection for FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
{
    fn graceful_shutdown(self: Pin<&mut Self>) {
        *self.project().cancelled = true;
    }
}

impl<S, IO, C, Req> FramedConnection<S, Framed<IO, C>, Req>
where
    S: tower::Service<Req>,
    IO: AsyncRead + AsyncWrite,
{
    /// Create a new framed connection from a service, a stream, and a codec
    pub fn framed(service: S, stream: IO, codec: C, multiplex: bool) -> Self {
        Self::new(service, Framed::new(stream, codec), multiplex)
    }
}

impl<S, F, Req> fmt::Debug for FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodecServer").finish()
    }
}

enum ReadAction {
    Spawned,
    Terminated,
}

impl<S, F, Req> FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
    F: Stream<Item = Result<Req, S::Error>>,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ReadAction, S::Error>> {
        let mut this = self.as_mut().project();

        if !*this.service_ready {
            ready!(this.service.poll_ready(cx))?;
            *this.service_ready = true;
        }

        match ready!(this.framed.as_mut().try_poll_next(cx)) {
            Some(Ok(req)) => {
                let future = this.service.call(req);
                this.tasks.push(future);
                *this.service_ready = false;
                return Ok(ReadAction::Spawned).into();
            }

            Some(Err(error)) => {
                debug!("Codec Error");
                return Err(error).into();
            }
            None => {
                trace!("Codec Empty");
                return Ok(ReadAction::Terminated).into();
            }
        }
    }
}

impl<S, F, Req> FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
    S::Error: fmt::Display,
{
    fn poll_tasks(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<S::Response>, S::Error>> {
        loop {
            match ready!(self.as_mut().project().tasks.try_poll_next(cx)) {
                Some(Ok(response)) => {
                    return Ok(Some(response)).into();
                }
                Some(Err(error)) => {
                    warn!("Task encountered an unhandled error: {error}");
                }
                None => return Ok(None).into(),
            }
        }
    }
}

impl<S, F, Req> FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
    S::Error: fmt::Display,
    F: Sink<S::Response, Error = S::Error>,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        loop {
            if !self.framed_send_ready {
                let this = self.as_mut().project();
                ready!(this.framed.poll_ready(cx))?;
                *this.framed_send_ready = true;
            }

            let message = match ready!(self.as_mut().poll_tasks(cx))? {
                Some(message) => message,
                None => {
                    return Ok(()).into();
                }
            };

            let this = self.as_mut().project();
            trace!("Writing response");
            *this.framed_send_ready = false;
            this.framed.start_send(message)?;
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.as_mut().project().framed.poll_close(cx)
    }
}

impl<S, F, Req> Future for FramedConnection<S, F, Req>
where
    S: tower::Service<Req>,
    S::Error: fmt::Display,
    F: Stream<Item = Result<Req, S::Error>> + Sink<S::Response, Error = S::Error>,
{
    type Output = Result<(), S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            // Write as much as we can before pending.
            if let Poll::Ready(Err(error)) = self.as_mut().poll_write(cx) {
                debug!("Write error: {error}");
                return Err(error).into();
            };

            // Read and start new tasks only if we are still running.
            if !self.cancelled {
                match self.as_mut().poll_read(cx) {
                    Poll::Ready(Ok(ReadAction::Terminated)) => {
                        trace!("Read terminated: cancel");
                        let this = self.as_mut().project();
                        *this.cancelled = true;
                    }

                    Poll::Ready(Ok(ReadAction::Spawned)) => {}
                    Poll::Ready(Err(error)) => {
                        debug!("Read error: {error}");
                        return Err(error).into();
                    }
                    Poll::Pending if self.tasks.is_empty() => {
                        // No more tasks to poll, but there might be data sitting
                        // in the outbound buffer, so try to flush it. Normally, this isn't
                        // worth it, but there is nothing left to do before we return pending.
                        ready!(self.as_mut().project().framed.poll_flush(cx)).inspect_err(
                            |error| {
                                debug!("flush error: {error}");
                            },
                        )?;
                        return Poll::Pending;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            } else if self.tasks.is_empty() {
                // Cancelled, flush and close writer.
                return self.as_mut().poll_shutdown(cx);
            } else {
                // Tasks are still running.
                return Poll::Pending;
            }
        }
    }
}
