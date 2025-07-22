//! Connection Pooling for Clients
//!
//! The `pool` module provides a connection pool for clients, which allows for multiple connections to be made to a
//! remote host and reused across multiple requests. This is supported in the `ClientService` type.
//!
//! This connection pool is specifically designed with HTTP connections in mind. It separates the treatment of the
//! connection (e.g. HTTP/1.1, HTTP/2, etc) from the transport (e.g. TCP, TCP+TLS, etc). This allows the pool to be used
//! with any type of connection, as long as it implements the `PoolableConnection` trait, and any type of transport,
//! as long as it implements the `PoolableTransport` trait. This also allows the pool to be used with upgradeable
//! connections, such as HTTP/1.1 connections that can be upgraded to HTTP/2, where the pool will have new HTTP/2
//! connections wait for in-progress upgrades from HTTP/1.1 connections to complete and use those, rather than creating
//! new connections.
//!
//! Pool configuration happens in the `Config` type, which allows for setting the maximum idle duration of a connection,
//! and the maximum number of idle connections per host.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;

use std::time::Duration;

use parking_lot::ArcMutexGuard;
use parking_lot::Mutex;
use tokio::sync::oneshot::Sender;
use tracing::trace;

mod checkout;
mod idle;
mod key;
pub(super) mod service;
mod weakopt;

use crate::BoxError;

pub(super) use self::checkout::Checkout;
use self::idle::IdleConnections;
pub(super) use self::key::Token;
use self::key::TokenMap;
use self::weakopt::WeakOpt;

use super::conn::Connection;
use super::conn::Connector;
use super::conn::Protocol;
use super::conn::Transport;
use super::conn::dns::Resolver;

/// Error building a key
#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct KeyError {
    #[source]
    inner: BoxError,
}

/// Key which links an address and request to a connection.
pub trait Key<R>: Eq + std::hash::Hash + fmt::Debug {
    /// Build a pool key from an address and request
    fn build(request: &R) -> Result<Self, KeyError>
    where
        Self: Sized;
}

/// A pool of connections to remote hosts.
///
/// The pool makes use of a `Checkout` to represent a connection that is being checked out of the pool. The `Checkout`
/// type requires a `Connector` to be provided, which provides a future that will create a new connection to the remote
/// host, and a future that will perform the handshake for the connection. The `Checkout` ensures that in-progress
/// connection state is correctly managed, and that duplicate connections are not made unnecessarily.
///
/// The pool also provides a `Pooled` type, which is a wrapper around a connection that will return the connection to
/// the pool when dropped, if the connection is still open and has not been marked as reusable (reusable connections
/// are always kept in the pool - there is no need to return dropped copies).
#[derive(Debug)]
pub(crate) struct Pool<C, R, K>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
    K: Key<R>,
{
    inner: Arc<Mutex<PoolInner<C, R>>>,

    // NOTE: The token map is stored on the Pool, not the PoolInner so that the generic K argument doesn't
    // propogate into the pool inner and reference, only the connection type propogates.
    keys: Arc<Mutex<TokenMap<K>>>,
}

impl<C, R, K> Clone for Pool<C, R, K>
where
    R: Send + 'static,
    C: PoolableConnection<R>,
    K: Key<R>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            keys: self.keys.clone(),
        }
    }
}

impl<C, R, K> Pool<C, R, K>
where
    R: Send + 'static,
    C: PoolableConnection<R>,
    K: Key<R>,
{
    pub(crate) fn new(config: Config) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner::new(config))),
            keys: Arc::new(Mutex::new(TokenMap::default())),
        }
    }

    pub(in crate::client) fn as_ref(&self) -> PoolRef<C, R> {
        PoolRef {
            inner: WeakOpt::downgrade(&self.inner),
        }
    }
}

impl<C, R, K> Default for Pool<C, R, K>
where
    R: Send + 'static,
    C: PoolableConnection<R>,
    K: Key<R>,
{
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl<C, R, K> Pool<C, R, K>
where
    R: Send + 'static,
    C: PoolableConnection<R>,
    K: Key<R>,
{
    /// Create a checkout for a connection to the given key (host/port pair).
    ///
    /// The checkout has several potential behaviors:
    ///
    /// 1. A connection already exists in the pool. It will be immediately available when
    /// polling the checkout object.
    /// 2. A connection is pending (probably in the handshake phase). The checkout will wait
    /// for the pending connection to be ready and re-used.
    /// 3. The checkout will create a new connection if none is available.
    /// 4. During the connection phase, if a new connection is returned to the pool, it will be returned
    /// in place of this one. If `continue_after_preemtion` is `true` in the pool config, the in-progress
    /// connection will continue in the background and be returned to the pool on completion.
    #[cfg_attr(not(tarpaulin), tracing::instrument(skip_all, fields(?key), level="debug"))]
    pub(crate) fn checkout<D, T, P>(
        &self,
        key: K,
        multiplex: bool,
        connector: Connector<D, T, P, R>,
    ) -> Checkout<D, T, P, R>
    where
        D: Resolver<R> + Send + 'static,
        D::Address: Send,
        D::Future: Send,
        T: Transport<D::Address>,
        P: Protocol<T::IO, R, Connection = C> + Send + 'static,
        C: PoolableConnection<R>,
    {
        let mut inner = self.inner.lock();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut connector: Option<Connector<D, T, P, R>> = Some(connector);
        let token = self.keys.lock().insert(key);

        if let Some(connection) = inner.pop(token) {
            trace!("connection found in pool");
            let request = connector.take().map(|mut c| c.take_request_unpinned());
            return Checkout::new(
                token,
                self.as_ref(),
                rx,
                connector,
                Some(connection),
                request,
                &inner.config,
            );
        }

        trace!("checkout interested in pooled connections");
        inner.waiting.entry(token).or_default().push_back(tx);

        if inner.connecting.contains(&token) {
            trace!("connection in progress elsewhere, will wait");
            let request = connector.take().map(|mut c| c.take_request_unpinned());
            Checkout::new(
                token,
                self.as_ref(),
                rx,
                connector,
                None,
                request,
                &inner.config,
            )
        } else {
            if multiplex {
                // Only block new connection attempts if we can multiplex on this one.
                trace!("checkout of multiplexed connection, other connections should wait");
                inner.connecting.insert(token);
            }
            trace!("connecting to host");
            Checkout::new(
                token,
                self.as_ref(),
                rx,
                connector,
                None,
                None,
                &inner.config,
            )
        }
    }
}

pub(in crate::client) struct PoolRef<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    inner: WeakOpt<Mutex<PoolInner<C, R>>>,
}

impl<C, R> fmt::Debug for PoolRef<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PoolRef").field(&self.inner).finish()
    }
}

impl<C, R> PoolRef<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    pub(in crate::client) fn none() -> Self {
        Self {
            inner: WeakOpt::none(),
        }
    }

    #[allow(dead_code)]
    pub(in crate::client) fn try_lock(&self) -> Option<PoolGuard<C, R>> {
        self.inner
            .upgrade()
            .and_then(|inner| inner.try_lock_arc().map(PoolGuard))
    }

    pub(in crate::client) fn lock(&self) -> Option<PoolGuard<C, R>> {
        self.inner
            .upgrade()
            .map(|inner| PoolGuard(inner.lock_arc()))
    }

    #[allow(dead_code)]
    pub(in crate::client) fn is_none(&self) -> bool {
        self.inner.is_none()
    }
}

impl<C, R> Clone for PoolRef<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub(in crate::client) struct PoolGuard<C: PoolableConnection<R>, R: Send + 'static>(
    ArcMutexGuard<parking_lot::RawMutex, PoolInner<C, R>>,
);

impl<C, R> Deref for PoolGuard<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    type Target = PoolInner<C, R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C, R> DerefMut for PoolGuard<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub(in crate::client) struct PoolInner<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    config: Config,

    connecting: HashSet<Token>,
    waiting: HashMap<Token, VecDeque<Sender<Pooled<C, R>>>>,

    idle: HashMap<Token, IdleConnections<C, R>>,
}

impl<C, R> PoolInner<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn new(config: Config) -> Self {
        Self {
            config,
            connecting: HashSet::new(),
            waiting: HashMap::new(),
            idle: HashMap::new(),
        }
    }

    pub(in crate::client) fn cancel_connection(&mut self, token: Token) {
        let existed = self.connecting.remove(&token);
        if existed {
            trace!("pending connection cancelled");
        }
    }
}

impl<C, R> PoolInner<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    /// Mark a connection as connected, but not done with the handshake.
    ///
    /// New connection attempts will wait for this connection to complete the
    /// handshake and re-use it if possible.
    pub(in crate::client) fn connected_in_handshake(&mut self, token: Token) {
        self.connecting.insert(token);
    }
}

impl<C, R> PoolInner<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn push(&mut self, token: Token, mut connection: C, pool_ref: PoolRef<C, R>) {
        self.connecting.remove(&token);

        if let Some(waiters) = self.waiting.get_mut(&token) {
            trace!(waiters=%waiters.len(), ?token, "walking waiters");

            while let Some(waiter) = waiters.pop_front() {
                if waiter.is_closed() {
                    trace!("skipping closed waiter");
                    continue;
                }

                if let Some(conn) = connection.reuse() {
                    trace!("re-usable connection will be sent to waiter");
                    let pooled = Pooled {
                        connection: Some(conn),
                        token: Token::zero(),
                        pool: pool_ref.clone(),
                    };

                    if waiter.send(pooled).is_err() {
                        trace!("waiter closed, skipping");
                        continue;
                    };
                } else {
                    trace!(
                        ?token,
                        "connection not re-usable, but will be sent to waiter"
                    );
                    let pooled = Pooled {
                        connection: Some(connection),
                        token,
                        pool: pool_ref.clone(),
                    };

                    let Err(pooled) = waiter.send(pooled) else {
                        trace!("connection sent");
                        return;
                    };

                    trace!("waiter closed, continuing");
                    connection = pooled.take().unwrap();
                }
            }
        }

        self.idle.entry(token).or_default().push(connection);
    }

    fn pop(&mut self, token: Token) -> Option<C> {
        let mut empty = false;
        let mut idle_entry = None;

        tracing::trace!(?token, "pop");

        if let Some(idle) = self.idle.get_mut(&token) {
            idle_entry = idle.pop(self.config.idle_timeout);
            empty = idle.is_empty();
        }

        if empty && !idle_entry.as_ref().map(|i| i.can_share()).unwrap_or(false) {
            trace!(?token, "removing empty idle list");
            self.idle.remove(&token);
        }

        idle_entry
    }
}

/// Configuration for a connection pool.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// The maximum idle duration of a connection.
    pub idle_timeout: Option<Duration>,

    /// The maximum number of idle connections per host.
    pub max_idle_per_host: usize,

    /// Should in-progress connections continue after they get pre-empted by a new connection?
    pub continue_after_preemption: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(90)),
            max_idle_per_host: 32,
            continue_after_preemption: true,
        }
    }
}

/// A [`crate::client::conn::Transport`] Stream that can produce connections
/// which might be poolable.
///
/// This trait is used by the pool connection checkout process before
/// the handshake occurs to check if the connection has negotiated or
/// upgraded to a protocol which enables multiplexing. This is an
/// optimistic check, and the connection will be checked again after
/// the handshake is complete.
pub trait PoolableStream: Unpin + Send + Sized + 'static {
    /// Returns `true` if the transport can be re-used, usually
    /// because it has used ALPN to negotiate a protocol that can
    /// be multiplexed.
    ///
    /// This is effectively speculative, so should only return `true`
    /// when we are sure that the connection on this transport
    /// will be able to multiplex.
    fn can_share(&self) -> bool;
}

/// A [`crate::client::conn::Connection`] that can be pooled.
///
/// These connections must report to the pool whether they remain open,
/// and whether they can be shared / multiplexed.
///
/// The pool will call [`PoolableConnection::reuse`] to get a new connection
/// to return to the pool, which will multiplex against this one. If multiplexing
/// is not possible, then `None` should be returned.
pub trait PoolableConnection<R>: Connection<R> + Unpin + Send + Sized + 'static
where
    R: Send + 'static,
{
    /// Returns `true` if the connection is open.
    fn is_open(&self) -> bool;

    /// Returns `true` if the connection can be shared / multiplexed.
    ///
    /// If this returns `true`, then [`PoolableConnection::reuse`] will be called to get
    /// a new connection to return to the pool.
    fn can_share(&self) -> bool;

    /// Returns a new connection to return to the pool, which will multiplex
    /// against this one if possible.
    fn reuse(&mut self) -> Option<Self>;
}

/// Wrapper type for a connection which is managed by a pool.
///
/// This type is used outside of the Pool to ensure that dropped
/// connections are returned to the pool. The underlying connection
/// is available via `Deref` and `DerefMut`.
pub struct Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    connection: Option<C>,
    token: Token,
    pool: PoolRef<C, R>,
}

impl<C, R> Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn take(mut self) -> Option<C> {
        self.connection.take()
    }

    /// Checks if this connection is being re-used
    /// by the pool (implying that it is multiplexed)
    pub fn is_reused(&self) -> bool {
        self.token.is_zero()
    }
}

impl<C, R> fmt::Debug for Pooled<C, R>
where
    C: fmt::Debug + PoolableConnection<R>,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Pooled").field(&self.connection).finish()
    }
}

impl<C, R> Deref for Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("connection only taken on Drop")
    }
}

impl<C, R> DerefMut for Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
            .as_mut()
            .expect("connection only taken on Drop")
    }
}

impl<C, R> Connection<R> for Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    type Response = C::Response;
    type Error = <C as Connection<R>>::Error;
    type Future = C::Future;

    fn send_request(&mut self, request: R) -> Self::Future {
        self.connection
            .as_mut()
            .expect("connection only taken on Drop")
            .send_request(request)
    }

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        <C as Connection<R>>::poll_ready(
            self.connection
                .as_mut()
                .expect("connection only taken on Drop"),
            cx,
        )
    }
}

impl<C, R> PoolableConnection<R> for Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn reuse(&mut self) -> Option<Self> {
        self.connection.as_mut().unwrap().reuse().map(|c| Pooled {
            connection: Some(c),
            token: self.token,
            pool: self.pool.clone(),
        })
    }

    fn is_open(&self) -> bool {
        self.connection.as_ref().unwrap().is_open()
    }

    fn can_share(&self) -> bool {
        self.connection.as_ref().unwrap().can_share()
    }
}

impl<C, R> Drop for Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            if !connection.can_share() {
                tokio::spawn(WhenReady {
                    connection: Some(connection),
                    token: self.token,
                    pool: self.pool.clone(),
                });
            }
        }
    }
}

/// A future which resolves when the connection is ready again
#[derive(Debug)]
struct WhenReady<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    connection: Option<C>,
    token: Token,
    pool: PoolRef<C, R>,
}

impl<C, R> std::future::Future for WhenReady<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        match self
            .connection
            .as_mut()
            .expect("connection polled after drop")
            .poll_ready(cx)
        {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(()),
            std::task::Poll::Ready(Err(err)) => {
                tracing::trace!(error = %err, "Connection errored while polling for readiness");
                std::task::Poll::Ready(())
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<C, R> Drop for WhenReady<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            if connection.is_open() && !self.token.is_zero() {
                if let Some(mut pool) = self.pool.lock() {
                    trace!("open connection returned to pool");
                    pool.push(self.token, connection, self.pool.clone());
                }
            }
        }
    }
}

#[cfg(all(test, feature = "mock"))]
mod tests {

    use futures::FutureExt as _;
    use static_assertions::assert_impl_all;

    use crate::client::conn::transport::mock::MockConnectionError;

    use super::*;
    use crate::client::conn::connector::Error;
    use crate::client::conn::protocol::mock::{MockRequest, MockSender};
    use crate::client::conn::stream::mock::MockStream;
    use crate::client::conn::transport::mock::MockTransport;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct MockKey;

    impl super::Key<MockRequest> for MockKey {
        fn build(_: &MockRequest) -> Result<Self, KeyError>
        where
            Self: Sized,
        {
            Ok(MockKey)
        }
    }

    #[test]
    fn sensible_config() {
        let _ = tracing_subscriber::fmt::try_init();

        let config = Config::default();
        let pool: Pool<MockSender, MockRequest, MockKey> = Pool::new(config);

        assert!(pool.inner.lock().config.idle_timeout.unwrap() > Duration::from_secs(1));
        assert!(pool.inner.lock().config.max_idle_per_host > 0);
        assert!(pool.inner.lock().config.max_idle_per_host < 2048);
    }

    assert_impl_all!(Pool<MockSender, MockRequest, MockKey>: Clone);

    #[tokio::test]
    async fn checkout_simple() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let conn = pool
            .checkout(
                key.clone(),
                false,
                MockTransport::single().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();
        drop(conn);

        let conn = pool
            .checkout(
                key.clone(),
                false,
                MockTransport::single().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), cid, "connection should be re-used");
        conn.close();
        drop(conn);

        let c2 = pool
            .checkout(key, false, MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(c2.is_open());
        assert_ne!(c2.id(), cid, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_multiplex() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let conn = pool
            .checkout(
                key.clone(),
                true,
                MockTransport::reusable().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();
        drop(conn);

        let conn = pool
            .checkout(
                key.clone(),
                true,
                MockTransport::reusable().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), cid, "connection should be re-used");
        conn.close();
        drop(conn);

        let conn = pool
            .checkout(
                key.clone(),
                true,
                MockTransport::reusable().connector(MockRequest),
            )
            .await
            .unwrap();
        assert!(conn.is_open());
        assert_ne!(conn.id(), cid, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_multiplex_contended() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });
        let key = MockKey;

        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut checkout_a = std::pin::pin!(pool.checkout(
            key.clone(),
            true,
            MockTransport::channel(rx).connector(MockRequest)
        ));

        assert!(futures::poll!(&mut checkout_a).is_pending());

        let mut checkout_b = std::pin::pin!(pool.checkout(
            key.clone(),
            true,
            MockTransport::reusable().connector(MockRequest),
        ));

        assert!(futures::poll!(&mut checkout_b).is_pending());
        assert!(tx.send(MockStream::reusable()).is_ok());
        assert!(futures::poll!(&mut checkout_b).is_pending());

        let conn_a = checkout_a.await.unwrap();
        assert!(conn_a.is_open());

        let conn_b = checkout_b.await.unwrap();
        assert!(conn_b.is_open());
        assert_eq!(conn_b.id(), conn_a.id(), "connection should be re-used");
    }

    #[tokio::test]
    async fn checkout_idle_returned() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let conn = MockSender::single();

        let first_id = conn.id();

        let checkout = pool.checkout(
            key.clone(),
            false,
            MockTransport::single().connector(MockRequest),
        );

        let token = checkout.token();

        // Return the connection to the pool, sending it out to the new checkout
        // that is waiting, cancelling the checkout connect.

        pool.inner.lock().push(token, conn, pool.as_ref());

        let conn = checkout.now_or_never().unwrap().unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), first_id, "connection should be re-used");
    }

    #[tokio::test]
    async fn checkout_idle_connected() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let conn_first = MockSender::single();

        let first_id = conn_first.id();

        tracing::debug!("Checkout from pool");

        let checkout = pool.checkout(
            key.clone(),
            false,
            MockTransport::single().connector(MockRequest),
        );

        let token = checkout.token();

        tracing::debug!("Checking interest");

        // At least one connection should be happening / waiting.
        assert!(
            !pool
                .inner
                .lock()
                .waiting
                .get(&token)
                .expect("no waiting connections in pool")
                .is_empty()
        );

        tracing::debug!("Resolving checkout");

        let conn = checkout.now_or_never().unwrap().unwrap();

        tracing::debug!("Inserting original connection");
        // Return the connection to the pool, sending it out to the new checkout
        // that is waiting, cancelling the checkout connect.
        pool.inner.lock().push(token, conn_first, pool.as_ref());

        assert!(conn.is_open());
        assert_ne!(conn.id(), first_id, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_drop_pool_err() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let start = pool.checkout(
            key.clone(),
            true,
            MockTransport::reusable().connector(MockRequest),
        );

        let checkout = pool.checkout(
            key.clone(),
            true,
            MockTransport::reusable().connector(MockRequest),
        );

        drop(start);
        drop(pool);

        assert!(checkout.now_or_never().unwrap().is_err());
    }

    #[tokio::test]
    async fn checkout_drop_pool() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let checkout = pool.checkout(
            key.clone(),
            true,
            MockTransport::reusable().connector(MockRequest),
        );

        drop(pool);

        assert!(checkout.now_or_never().unwrap().is_ok());
    }

    #[tokio::test]
    async fn checkout_connection_error() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let key = MockKey;

        let checkout = pool.checkout(
            key.clone(),
            true,
            MockTransport::error().connector(MockRequest),
        );

        let outcome = checkout.now_or_never().unwrap();
        let error = outcome.unwrap_err();
        assert!(matches!(error, Error::Connecting(MockConnectionError)));
    }

    #[tokio::test]
    async fn checkout_pool_cloned() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });
        let other = pool.clone();

        let key = MockKey;

        let conn = pool
            .checkout(
                key.clone(),
                false,
                MockTransport::single().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();
        drop(conn);

        let conn = other
            .checkout(
                key.clone(),
                false,
                MockTransport::single().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), cid, "connection should be re-used");
        conn.close();
        drop(conn);

        let c2 = pool
            .checkout(key, false, MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(c2.is_open());
        assert_ne!(c2.id(), cid, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_delayed_drop() {
        let _ = tracing_subscriber::fmt::try_init();

        let pool = Pool::new(Config {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: true,
        });

        let key = MockKey;

        let conn = pool
            .checkout(
                key.clone(),
                false,
                MockTransport::single().connector(MockRequest),
            )
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();

        let checkout = pool.checkout(
            key.clone(),
            false,
            MockTransport::single().connector(MockRequest),
        );

        let token = checkout.token();

        drop(conn);
        let conn = checkout.await.unwrap();
        assert!(conn.is_open());
        assert_eq!(cid, conn.id());

        let inner = pool.inner.lock();
        let idles = inner.idle.get(&token).unwrap();
        assert_eq!(idles.len(), 1);
    }
}
