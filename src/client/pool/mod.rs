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
use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::trace;

mod checkout;
mod idle;
mod key;
mod lock;
pub mod manager;
pub(super) mod service;

use crate::BoxError;

pub(super) use self::checkout::Checkout;
use self::idle::IdleConnections;
pub(super) use self::key::Token;
use self::key::TokenMap;
use self::lock::WeakMutex;
use self::manager::ConnectionManager;
use self::manager::ConnectionManagerConfig;
use self::manager::InnerConnectionManager;

use super::conn::Connection;
use super::conn::Connector;
use super::conn::Protocol;
use super::conn::Transport;

/// Error building a key
#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct KeyError {
    #[source]
    inner: BoxError,
}

impl KeyError {
    /// Create a new `KeyError` from an error
    pub fn new<E>(err: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self { inner: err.into() }
    }
}

/// Key which links an address and request to a connection.
pub trait Key<R>: Eq + std::hash::Hash + fmt::Debug {
    /// Build a pool key from an address and request
    fn build_key(request: &R) -> Result<Self, KeyError>
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
    pub(crate) fn new(config: ConnectionManagerConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner::new(config))),
            keys: Arc::new(Mutex::new(TokenMap::default())),
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
        Self::new(ConnectionManagerConfig::default())
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
    pub(crate) fn checkout<T, P>(&self, key: K, connector: Connector<T, P, R>) -> Checkout<T, P, R>
    where
        T: Transport<R> + Send,
        P: Protocol<T::IO, R, Connection = C> + Send + 'static,
        C: PoolableConnection<R>,
    {
        let token = self.keys.lock().insert(key);

        let mut inner = self.inner.lock();
        let config = inner.config.clone();
        let manager = inner
            .connections
            .entry(token)
            .or_insert_with(|| ConnectionManager::new(config));

        manager.checkout(connector)
    }
}

#[derive(Debug)]
pub(in crate::client) struct PoolInner<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    config: Arc<ConnectionManagerConfig>,
    connections: HashMap<Token, ConnectionManager<C, R>>,
}

impl<C, R> PoolInner<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn new(config: ConnectionManagerConfig) -> Self {
        Self {
            config: Arc::new(config),
            connections: HashMap::new(),
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

type ManagerRef<C, R> = WeakMutex<InnerConnectionManager<C, R>>;

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
    manager: ManagerRef<C, R>,
}

impl<C, R> Pooled<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    fn take(mut self) -> Option<C> {
        self.connection.take()
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
            manager: self.manager.clone(),
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
                    pool: self.manager.clone(),
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
    pool: ManagerRef<C, R>,
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
                tracing::trace!(error = %err, "connection errored while polling for readiness");
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
            if connection.is_open() {
                if let Some(mut pool) = self.pool.lock() {
                    trace!("open connection returned to pool");
                    pool.push(connection, &self.pool);
                }
            }
        }
    }
}
