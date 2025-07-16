//! Serving components

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::{fmt, io};

use tracing::Instrument;
use tracing::debug;
use tracing::instrument::Instrumented;

use crate::BoxError;
use crate::{notify, services::MakeServiceRef};

use self::conn::drivers::{ConnectionDriver, ServerExecutor};
use self::conn::drivers::{GracefulConnectionDriver, GracefulServerExecutor};
pub use self::conn::{Accept, Connection, Protocol};

pub mod conn;

/// A server that can accept connections, and run each connection
/// using a [tower::Service].
///
/// To use the server, call `.await` on it. This will start the server
/// and serve until the future is cancelled or the acceptor encounters an
/// error. To cancel the server, drop the future.
///
/// The server also supports graceful shutdown. Provide a future which will
/// resolve when the server should shut down to [`Server::with_graceful_shutdown`]
/// to enable this behavior. In graceful shutdown mode, individual connections
/// will have an opportunity to finish processing before the server stops.
///
/// The generic parameters can be a bit tricky. They are as follows:
///
/// - `A` is the type that can accept connections. This must implement [`Accept`].
/// - `P` is the protocol to use for serving connections. This must implement [`Protocol`].
/// - `S` is the "make service" which generates a service to handle each connection.
///   This must implement [`MakeServiceRef`], and will be passed a reference to the
///   connection stream (to facilitate connection information).
/// - `R` is the request type for the service.
/// - `E` is the executor to use for running the server. This is used to spawn the
///   connection futures.
pub struct Server<A, P, S, R, E> {
    acceptor: A,
    protocol: P,
    make_service: S,
    executor: E,
    request: PhantomData<fn(R) -> ()>,
}

impl<A, P, S, R, E> fmt::Debug for Server<A, P, S, R, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server").finish()
    }
}

impl<A, P, S, R, E> Server<A, P, S, R, E> {
    /// Create a new server with the given `MakeService` and `Acceptor`, and a custom [Protocol].
    ///
    /// The default protocol is [AutoBuilder], which can serve both HTTP/1 and HTTP/2 connections,
    /// and will automatically detect the protocol used by the client.
    pub fn new(acceptor: A, protocol: P, make_service: S, executor: E) -> Self {
        Self {
            acceptor,
            protocol,
            make_service,
            executor,
            request: PhantomData,
        }
    }

    /// Shutdown the server gracefully when the given future resolves.
    ///
    ///
    /// # Example
    /// ```rust
    /// use hyperdriver::stream::duplex;
    /// use hyperdriver::Server;
    /// use hyperdriver::Body;
    /// use tower::service_fn;
    ///
    /// # #[derive(Debug)]
    /// # struct MyError;
    /// #
    /// # impl std::fmt::Display for MyError {
    /// #   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    /// #        f.write_str("MyError")
    /// #  }
    /// # }
    /// #
    /// # impl std::error::Error for MyError {}
    /// #
    /// # async fn example() {
    /// let (_, incoming) = duplex::pair();
    /// let server = Server::builder()
    ///     .with_acceptor(incoming)
    ///     .with_shared_service(service_fn(|req: http::Request<Body>| async move {
    ///        Ok::<_, MyError>(http::Response::new(Body::empty()))
    ///    }))
    ///    .with_auto_http()
    ///    .with_tokio()
    ///    .with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; }).await.unwrap();
    /// # }
    /// ```
    pub fn with_graceful_shutdown<F>(self, signal: F) -> GracefulShutdown<A, P, S, R, E, F>
    where
        S: MakeServiceRef<A::Connection, R>,
        P: Protocol<S::Service, A::Connection, R>,
        A: Accept + Unpin,
        F: Future<Output = ()> + Send + 'static,
        E: ServerExecutor<P, S, A, R>,
    {
        GracefulShutdown::new(self, signal)
    }
}

impl<A, P, S, R, E> IntoFuture for Server<A, P, S, R, E>
where
    S: MakeServiceRef<A::Connection, R>,
    P: Protocol<S::Service, A::Connection, R>,
    A: Accept + Unpin,
    E: ServerExecutor<P, S, A, R>,
{
    type IntoFuture = Serving<A, P, S, R, E>;
    type Output = Result<(), ServerError>;

    fn into_future(self) -> Self::IntoFuture {
        Serving {
            server: self,
            state: State::Preparing,
            span: tracing::debug_span!("accept"),
        }
    }
}

/// A future that drives the server to accept connections.
#[derive(Debug)]
#[pin_project::pin_project]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Serving<A, P, S, R, E>
where
    S: MakeServiceRef<A::Connection, R>,
    A: Accept,
{
    server: Server<A, P, S, R, E>,

    span: tracing::Span,

    #[pin]
    state: State<A::Connection, S::Future>,
}

#[derive(Debug)]
#[pin_project::pin_project(project = StateProj, project_replace = StateProjOwn)]
enum State<S, F> {
    Preparing,
    Accepting,
    Making {
        #[pin]
        future: F,
        stream: S,
    },
}

impl<A, P, S, R, E> Serving<A, P, S, R, E>
where
    S: MakeServiceRef<A::Connection, R>,
    P: Protocol<S::Service, A::Connection, R>,
    A: Accept + Unpin,
{
    /// Polls the server to accept a single new connection.
    ///
    /// The returned connection should be spawned on the runtime.
    #[allow(clippy::type_complexity)]
    fn poll_once(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Instrumented<P::Connection>>, ServerError>> {
        let mut me = self.as_mut().project();

        let _guard = me.span.enter();

        match me.state.as_mut().project() {
            StateProj::Preparing => {
                ready!(me.server.make_service.poll_ready_ref(cx)).map_err(ServerError::ready)?;
                me.state.set(State::Accepting);
            }
            StateProj::Accepting => match ready!(Pin::new(&mut me.server.acceptor).poll_accept(cx))
            {
                Ok(stream) => {
                    let future = me.server.make_service.make_service_ref(&stream);
                    me.state.set(State::Making { future, stream });
                }
                Err(e) => {
                    return Poll::Ready(Err(ServerError::accept(e)));
                }
            },
            StateProj::Making { future, .. } => {
                let service = ready!(future.poll(cx)).map_err(ServerError::make)?;
                if let StateProjOwn::Making { stream, .. } =
                    me.state.project_replace(State::Preparing)
                {
                    let span = tracing::debug_span!(parent: None, "connection");
                    let conn = me
                        .server
                        .protocol
                        .serve_connection(stream, service)
                        .instrument(span);

                    return Poll::Ready(Ok(Some(conn)));
                } else {
                    unreachable!("state must still be accepting");
                }
            }
        };
        Poll::Ready(Ok(None))
    }
}

impl<A, P, S, R, E> Future for Serving<A, P, S, R, E>
where
    S: MakeServiceRef<A::Connection, R>,
    P: Protocol<S::Service, A::Connection, R>,
    A: Accept + Unpin,
    E: ServerExecutor<P, S, A, R>,
{
    type Output = Result<(), ServerError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().poll_once(cx) {
                Poll::Ready(Ok(Some(conn))) => {
                    self.as_mut()
                        .project()
                        .server
                        .executor
                        .execute(ConnectionDriver::new(conn));
                }
                Poll::Ready(Ok(None)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// A server that can accept connections, and run each connection, and can
/// also process graceful shutdown signals.
///
/// See [`Server::with_graceful_shutdown`] for more details.
#[pin_project::pin_project]
pub struct GracefulShutdown<A, P, S, B, E, F>
where
    S: MakeServiceRef<A::Connection, B>,
    A: Accept,
{
    #[pin]
    server: Serving<A, P, S, B, E>,

    #[pin]
    signal: F,

    channel: notify::Receiver,
    shutdown: notify::Sender,

    #[pin]
    finished: notify::Notified,
    connection: notify::Sender,
}

impl<A, P, S, R, E, F> GracefulShutdown<A, P, S, R, E, F>
where
    S: MakeServiceRef<A::Connection, R>,
    P: Protocol<S::Service, A::Connection, R>,
    A: Accept + Unpin,
    F: Future<Output = ()>,
    E: ServerExecutor<P, S, A, R>,
{
    fn new(server: Server<A, P, S, R, E>, signal: F) -> Self {
        let (tx, rx) = notify::channel();
        let (tx2, rx2) = notify::channel();
        Self {
            server: server.into_future(),
            signal,
            channel: rx,
            shutdown: tx,
            finished: rx2.into_future(),
            connection: tx2,
        }
    }
}

impl<A, P, S, B, E, F> fmt::Debug for GracefulShutdown<A, P, S, B, E, F>
where
    S: MakeServiceRef<A::Connection, B>,
    A: Accept,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GracefulShutdown").finish()
    }
}

impl<A, P, S, Body, E, F> Future for GracefulShutdown<A, P, S, Body, E, F>
where
    S: MakeServiceRef<A::Connection, Body>,
    P: Protocol<S::Service, A::Connection, Body>,
    A: Accept + Unpin,
    F: Future<Output = ()>,
    E: GracefulServerExecutor<P, S, A, Body>,
{
    type Output = Result<(), ServerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // This loop means that we will greedily accept available connections
        // from `poll_once` until it returns `Poll::Pending` or Ok(None).
        loop {
            // Check the shutdown signal before accepting a connection.
            match this.signal.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    debug!("received shutdown signal");
                    this.shutdown.send();
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {}
            }

            // If all connections have been closed, we definitely don't want to poll
            // for a new connection, so we check this first.
            match this.finished.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    debug!("all connections closed");
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {}
            }

            match this.server.as_mut().poll_once(cx) {
                Poll::Ready(Ok(Some(conn))) => {
                    let shutdown_rx = this.channel.clone();
                    let finished_tx = this.connection.clone();

                    let span = conn.span().clone();

                    this.server
                        .server
                        .executor
                        .execute(GracefulConnectionDriver::new(
                            conn,
                            shutdown_rx,
                            finished_tx,
                            span,
                        ));
                }
                Poll::Ready(Ok(None)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// An error that can occur when serving connections.
///
/// This error is only returned at the end of the server. Individual connection's
/// errors are discarded and not returned. To handle an individual connection's
/// error, apply a middleware which can process that error in the Service.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// Accept Error
    #[error("accept error: {0}")]
    Accept(#[source] BoxError),

    /// IO Errors
    #[error(transparent)]
    Io(#[from] io::Error),

    /// Errors from the MakeService part of the server.
    #[error("make service: {0}")]
    MakeService(#[source] BoxError),
}

impl ServerError {
    fn accept<A>(error: A) -> Self
    where
        A: Into<BoxError>,
    {
        let boxed = error.into();
        debug!("accept error: {}", boxed);
        Self::Accept(boxed)
    }
}

impl ServerError {
    fn make<E>(error: E) -> Self
    where
        E: Into<BoxError>,
    {
        let boxed = error.into();
        debug!("make service error: {}", boxed);
        Self::MakeService(boxed)
    }

    fn ready<E>(error: E) -> Self
    where
        E: Into<BoxError>,
    {
        let boxed = error.into();
        debug!("ready error: {}", boxed);
        Self::MakeService(boxed)
    }
}
