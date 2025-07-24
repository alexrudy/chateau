use std::fmt;
use std::future::Future;
use std::pin::Pin;

use std::task::Context;
use std::task::Poll;
use std::task::ready;

use pin_project::pin_project;
use pin_project::pinned_drop;
use tokio::sync::oneshot::Receiver;
use tracing::debug;
use tracing::trace;

use crate::client::conn::Protocol;
use crate::client::conn::Transport;
use crate::client::conn::connector::Error as ConnectorError;
use crate::client::conn::connector::{Connector, ConnectorMeta};
use crate::client::conn::dns::Resolver;

#[cfg(debug_assertions)]
use self::ids::CheckoutId;
use super::Config;
use super::PoolRef;
use super::PoolableConnection;
use super::Pooled;
use super::key::Token;

#[cfg(debug_assertions)]
mod ids {
    use core::fmt;

    static CHECKOUT_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct CheckoutId(pub(super) usize);

    impl CheckoutId {
        pub(super) fn new() -> Self {
            CheckoutId(CHECKOUT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
        }
    }

    impl fmt::Display for CheckoutId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "checkout-{}", self.0)
        }
    }
}

#[pin_project(project = WaitingProjected)]
pub(crate) enum Waiting<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    /// The checkout is waiting on an idle connection, and should
    /// attempt its own connection in the interim as well.
    Idle(#[pin] Receiver<Pooled<C, B>>),

    /// The checkout is waiting on a connection currently in the process
    /// of connecting, and should wait for that connection to complete,
    /// not starting its own connection.
    Connecting(#[pin] Receiver<Pooled<C, B>>),

    /// There is no pool for connections to wait for.
    NoPool,
}

impl<C, B> Waiting<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    fn close(&mut self) {
        match self {
            Waiting::Idle(rx) => {
                rx.close();
            }
            Waiting::Connecting(rx) => {
                rx.close();
            }
            Waiting::NoPool => {}
        }

        *self = Waiting::NoPool;
    }
}

impl<C, B> fmt::Debug for Waiting<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Waiting::Idle(_) => f.debug_tuple("Idle").finish(),
            Waiting::Connecting(_) => f.debug_tuple("Connecting").finish(),
            Waiting::NoPool => f.debug_tuple("NoPool").finish(),
        }
    }
}

pub(crate) enum WaitingPoll<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    Connected(Pooled<C, B>),
    Closed,
    NotReady,
}

impl<C, B> Future for Waiting<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    type Output = WaitingPoll<C, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = match self.as_mut().project() {
            WaitingProjected::Idle(rx) => match rx.poll(cx) {
                Poll::Ready(Ok(connection)) => Poll::Ready(WaitingPoll::Connected(connection)),
                Poll::Ready(Err(_)) => Poll::Ready(WaitingPoll::Closed),
                Poll::Pending => Poll::Ready(WaitingPoll::NotReady),
            },
            WaitingProjected::Connecting(rx) => match rx.poll(cx) {
                Poll::Ready(Ok(connection)) => Poll::Ready(WaitingPoll::Connected(connection)),
                Poll::Ready(Err(_)) => Poll::Ready(WaitingPoll::Closed),
                Poll::Pending => Poll::Pending,
            },
            WaitingProjected::NoPool => Poll::Ready(WaitingPoll::Closed),
        };

        if polled.is_ready() {
            self.as_mut().set(Waiting::NoPool);
        };

        polled
    }
}

#[pin_project(project = CheckoutConnectingProj)]
pub(crate) enum InnerCheckoutConnecting<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    Waiting,
    Connected,
    Connecting(Pin<Box<Connector<D, T, P, R>>>),
    ConnectingWithDelayDrop(Option<Pin<Box<Connector<D, T, P, R>>>>),
    ConnectingDelayed(Pin<Box<Connector<D, T, P, R>>>),
}

impl<D, T, P, R> fmt::Debug for InnerCheckoutConnecting<D, T, P, R>
where
    D: Resolver<R>,
    T: Transport<D::Address>,
    P: Protocol<T::IO, R>,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            InnerCheckoutConnecting::Waiting => f.debug_tuple("Waiting").finish(),
            InnerCheckoutConnecting::Connected => f.debug_tuple("Connected").finish(),
            InnerCheckoutConnecting::Connecting(connector) => {
                f.debug_tuple("Connecting").field(connector).finish()
            }
            InnerCheckoutConnecting::ConnectingWithDelayDrop(connector) => f
                .debug_tuple("ConnectingWithDelayDrop")
                .field(connector)
                .finish(),
            InnerCheckoutConnecting::ConnectingDelayed(connector) => {
                f.debug_tuple("ConnectingDelayed").field(connector).finish()
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct Checkout<D, T, P, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    token: Token,
    pool: PoolRef<P::Connection, R>,
    #[pin]
    waiter: Waiting<P::Connection, R>,
    #[pin]
    inner: InnerCheckoutConnecting<D, T, P, R>,
    request: Option<R>,
    connection: Option<P::Connection>,
    meta: ConnectorMeta,
    #[cfg(debug_assertions)]
    id: CheckoutId,
}

impl<D, T, P, R> fmt::Debug for Checkout<D, T, P, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkout")
            .field("token", &self.token)
            .field("pool", &self.pool)
            .field("waiter", &self.waiter)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<D, T, P, R> Checkout<D, T, P, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    /// Converts this checkout into a "delayed drop" checkout.
    fn as_delayed(self: Pin<&mut Self>) -> Option<Self> {
        let mut this = self.project();

        match this.inner.as_mut().project() {
            CheckoutConnectingProj::ConnectingWithDelayDrop(connector) if connector.is_some() => {
                tracing::trace!("converting checkout to delayed drop");
                Some(Checkout {
                    token: *this.token,
                    pool: this.pool.clone(),
                    waiter: Waiting::NoPool,
                    inner: InnerCheckoutConnecting::ConnectingDelayed(connector.take().unwrap()),
                    request: None,
                    connection: None,
                    meta: ConnectorMeta::new(), // New meta to avoid holding spans in the spawned task
                    #[cfg(debug_assertions)]
                    id: *this.id,
                })
            }
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn token(&self) -> Token {
        self.token
    }

    pub(crate) fn take_request_pinned(mut self: Pin<&mut Self>) -> R {
        self.as_mut()
            .project()
            .request
            .take()
            .expect("request is available")
    }

    /// Constructs a checkout which does not hold a reference to the pool
    /// and so is only waiting on the connector.
    ///
    /// This checkout will always proceed with the connector, uninterrupted by
    /// alternative connection solutions. It will not use the "delayed drop"
    /// procedure to finish connections if dropped.
    ///
    /// This is useful when using a checkout to poll a connection to readiness
    /// without a pool, or in a context in which the associated connection cannot
    /// or should not be shared with the pool.
    pub(crate) fn detached(connector: Connector<D, T, P, R>) -> Self {
        #[cfg(debug_assertions)]
        let id = CheckoutId::new();

        #[cfg(debug_assertions)]
        tracing::trace!(%id, "creating detached checkout");

        Self {
            token: Token::zero(),
            pool: PoolRef::none(),
            waiter: Waiting::NoPool,
            inner: InnerCheckoutConnecting::Connecting(Box::pin(connector)),
            request: None,
            connection: None,
            meta: ConnectorMeta::new(),
            #[cfg(debug_assertions)]
            id,
        }
    }

    pub(super) fn new(
        token: Token,
        pool: PoolRef<P::Connection, R>,
        waiter: Receiver<Pooled<P::Connection, R>>,
        connect: Option<Connector<D, T, P, R>>,
        connection: Option<P::Connection>,
        request: Option<R>,
        config: &Config,
    ) -> Self {
        #[cfg(debug_assertions)]
        let id = CheckoutId::new();
        let meta = ConnectorMeta::new();

        #[cfg(debug_assertions)]
        tracing::trace!(?token, %id, "creating new checkout");

        if connection.is_some() {
            tracing::trace!(?token, "connection recieved from pool");
            Self {
                token,
                pool,
                waiter: Waiting::Idle(waiter),
                inner: InnerCheckoutConnecting::Connected,
                request: request.or(connect.map(|mut c| c.take_request_unpinned())),
                connection,
                meta,
                #[cfg(debug_assertions)]
                id,
            }
        } else if let Some(connector) = connect {
            tracing::trace!(?token, "connecting to pool");

            let inner = if config.continue_after_preemption {
                InnerCheckoutConnecting::ConnectingWithDelayDrop(Some(Box::pin(connector)))
            } else {
                InnerCheckoutConnecting::Connecting(Box::pin(connector))
            };

            Self {
                token,
                pool,
                waiter: Waiting::Idle(waiter),
                inner,
                request,
                connection,
                meta,
                #[cfg(debug_assertions)]
                id,
            }
        } else {
            tracing::trace!(?token, "waiting for connection");
            Self {
                token,
                pool,
                waiter: Waiting::Connecting(waiter),
                inner: InnerCheckoutConnecting::Waiting,
                request,
                connection,
                meta,
                #[cfg(debug_assertions)]
                id,
            }
        }
    }
}

impl<D, T, P, R> Future for Checkout<D, T, P, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    type Output = Result<
        Pooled<P::Connection, R>,
        ConnectorError<
            D::Error,
            <T as Transport<D::Address>>::Error,
            <P as Protocol<T::IO, R>>::Error,
        >,
    >;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        let _entered = this.meta.current().clone().entered();

        {
            // Outcomes from .poll_waiter:
            // - Ready(Some(connection)) => return connection
            // - Ready(None) => continue to check pool, we don't have a waiter.
            // - Pending => wait on the waiter to complete, don't bother to check pool.

            // Open questions: Should we check the pool for a different connection when the
            // waiter is pending? Probably not, ideally our semantics should keep the pool
            // from containing multiple connections if they can be multiplexed.
            if let WaitingPoll::Connected(connection) = ready!(this.waiter.as_mut().poll(cx)) {
                debug!(token=?this.token, "connection recieved from waiter");

                match this.inner.as_mut().project() {
                    CheckoutConnectingProj::ConnectingWithDelayDrop(Some(connector))
                    | CheckoutConnectingProj::ConnectingDelayed(connector)
                    | CheckoutConnectingProj::Connecting(connector) => {
                        *this.request = Some(connector.as_mut().take_request_pinned());
                    }
                    _ => {}
                };

                return Poll::Ready(Ok(connection));
            }
        }

        trace!(token=?this.token, "polling for new connection");
        // Try to connect while we also wait for a checkout to be ready.

        match this.inner.as_mut().project() {
            CheckoutConnectingProj::Waiting => {
                // We're waiting on a connection to be ready.
                // If that were still happening, we would bail out above, since the waiter
                // would return Poll::Pending.
                Poll::Ready(Err(ConnectorError::Unavailable))
            }
            CheckoutConnectingProj::Connected => {
                // We've already connected, we can just return the connection.
                let connection = this
                    .connection
                    .take()
                    .expect("future was polled after completion");

                this.waiter.close();
                this.inner.set(InnerCheckoutConnecting::Connected);
                Poll::Ready(Ok(register_connected(this.pool, *this.token, connection)))
            }
            CheckoutConnectingProj::Connecting(connector) => {
                let result = ready!(connector.as_mut().poll_connector(
                    {
                        let pool = this.pool.clone();
                        let token = *this.token;
                        move || {
                            trace!(
                                ?token,
                                "connection can be shared, telling pool to wait for handshake"
                            );
                            if let Some(mut pool) = pool.lock() {
                                pool.connected_in_handshake(token);
                            }
                        }
                    },
                    this.meta,
                    cx
                ));

                this.waiter.close();
                *this.request = Some(connector.as_mut().take_request_pinned());
                this.inner.set(InnerCheckoutConnecting::Connected);

                match result {
                    Ok(connection) => {
                        Poll::Ready(Ok(register_connected(this.pool, *this.token, connection)))
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            CheckoutConnectingProj::ConnectingWithDelayDrop(Some(connector))
            | CheckoutConnectingProj::ConnectingDelayed(connector) => {
                let result = ready!(connector.as_mut().poll_connector(
                    {
                        let pool = this.pool.clone();
                        let token = *this.token;
                        move || {
                            trace!(
                                ?token,
                                "connection can be shared, telling pool to wait for handshake"
                            );
                            if let Some(mut pool) = pool.lock() {
                                pool.connected_in_handshake(token);
                            }
                        }
                    },
                    this.meta,
                    cx
                ));

                this.waiter.close();
                *this.request = Some(connector.as_mut().take_request_pinned());
                this.inner.set(InnerCheckoutConnecting::Connected);

                match result {
                    Ok(connection) => {
                        Poll::Ready(Ok(register_connected(this.pool, *this.token, connection)))
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            CheckoutConnectingProj::ConnectingWithDelayDrop(None) => {
                // Something stole our connection, this is an error state.
                panic!("connection was stolen from checkout")
            }
        }
    }
}

/// Register a connection with the pool referenced here.
fn register_connected<C, B>(
    poolref: &PoolRef<C, B>,
    token: Token,
    mut connection: C,
) -> Pooled<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    if let Some(mut pool) = poolref.lock() {
        if let Some(reused) = connection.reuse() {
            pool.push(token, reused, poolref.clone());
            return Pooled {
                connection: Some(connection),
                token: Token::zero(),
                pool: PoolRef::none(),
            };
        } else {
            return Pooled {
                connection: Some(connection),
                token,
                pool: poolref.clone(),
            };
        }
    }

    // No pool or lock was available, so we can't add the connection to the pool.
    //
    // Returning the original poolref + token means that if this was temporary,
    // and we can grab the pool later, we will do so.
    Pooled {
        connection: Some(connection),
        token,
        pool: poolref.clone(),
    }
}

#[pinned_drop]
impl<D, T, P, R> PinnedDrop for Checkout<D, T, P, R>
where
    D: Resolver<R> + Send + 'static,
    D::Address: Send + 'static,
    D::Future: Send + 'static,
    T: Transport<D::Address> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    fn drop(mut self: Pin<&mut Self>) {
        #[cfg(debug_assertions)]
        tracing::trace!(id=%self.id, "drop for checkout");

        if let Some(checkout) = self.as_mut().as_delayed() {
            tokio::task::spawn(async move {
                if let Err(err) = checkout.await {
                    tracing::error!(error=%err, "error during delayed drop");
                }
            });
        } else if let Some(mut pool) = self.pool.lock() {
            // Connection is only cancled when no delayed drop occurs.
            pool.cancel_connection(self.token);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use static_assertions::assert_impl_all;

    assert_impl_all!(ConnectorError<std::io::Error, std::io::Error, std::io::Error>: std::error::Error, Send, Sync, Into<BoxError>);

    use crate::BoxError;

    #[cfg(feature = "mock")]
    use crate::client::conn::transport::mock::MockTransport;

    #[test]
    fn verify_checkout_id() {
        let id = CheckoutId(0);
        assert_eq!(id.to_string(), "checkout-0");
        assert_eq!(id, CheckoutId(0));
        assert_eq!(format!("{id:?}"), "CheckoutId(0)");
        assert_eq!(id.clone(), CheckoutId(0));
    }

    #[cfg(feature = "mock")]
    #[tokio::test]
    async fn detatched_checkout() {
        use crate::client::conn::protocol::mock::MockRequest;

        let transport = MockTransport::single();

        let checkout = Checkout::detached(transport.connector(MockRequest));

        assert!(checkout.token.is_zero());
        assert!(checkout.pool.is_none());
        assert!(matches!(
            checkout.inner,
            InnerCheckoutConnecting::Connecting(_)
        ));
        assert!(matches!(checkout.waiter, Waiting::NoPool));

        let dbg = format!("{checkout:?}");
        assert!(dbg.starts_with("Checkout { "));

        let connection = checkout.await.unwrap();
        assert!(connection.is_open());
    }
}
