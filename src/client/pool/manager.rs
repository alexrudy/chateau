//! Connection pool management for a single connection "target"
//!
//! Connection managers manage each connection target (usually a host)
//! and maintain the bookeeping necessary to provide connection sharing
//! and re-use.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot::Sender;
use tracing::trace;

use crate::client::conn::Connector;
use crate::client::conn::Protocol;
use crate::client::conn::Transport;

use super::IdleConnections;
use super::PoolableConnection;
use super::Pooled;
use super::lock::ArcMutex;
use super::lock::ArcMutexGuard;
use super::lock::WeakMutex;

pub use super::checkout::Checkout;

/// Configuration for a connection pool.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectionManagerConfig {
    /// The maximum idle duration of a connection.
    pub idle_timeout: Option<Duration>,

    /// The maximum number of idle connections per host.
    pub max_idle_per_host: usize,

    /// Should in-progress connections continue after they get pre-empted by a new connection?
    pub continue_after_preemption: bool,
}

impl Default for ConnectionManagerConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(90)),
            max_idle_per_host: 32,
            continue_after_preemption: true,
        }
    }
}

/// Manage a group of connections targeting the same host.
///
/// This is the unit of work for a connection pool, and manages connection
/// sharing, idle connections, and connection bookkeeping.
#[derive(Debug)]
pub struct ConnectionManager<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    inner: ArcMutex<InnerConnectionManager<C, R>>,
}

impl<C, R> Clone for ConnectionManager<C, R>
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

impl<C, R> ConnectionManager<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    /// Create a new connection manager with the given configuration.
    pub fn new(config: impl Into<Arc<ConnectionManagerConfig>>) -> Self {
        Self {
            inner: InnerConnectionManager::new(config),
        }
    }

    /// Checkout a connection from this connection manager, using the given
    /// connector.
    pub fn checkout<T, P>(&self, connector: Connector<T, P, R>) -> Checkout<T, P, R>
    where
        T: Transport<R> + Send,
        P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    {
        InnerConnectionManager::checkout(&mut self.inner.lock(), connector)
    }
}

/// Manage a set of connections in the pool
#[derive(Debug)]
pub(super) struct InnerConnectionManager<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    connecting: bool,
    waiting: VecDeque<Sender<Pooled<C, R>>>,
    idle: IdleConnections<C, R>,
    config: Arc<ConnectionManagerConfig>,
}

impl<C, R> InnerConnectionManager<C, R>
where
    C: PoolableConnection<R>,
    R: Send + 'static,
{
    /// Creates a new connection manager with the given configuration.
    fn new(config: impl Into<Arc<ConnectionManagerConfig>>) -> ArcMutex<Self> {
        ArcMutex::new(Self {
            connecting: false,
            waiting: VecDeque::new(),
            idle: IdleConnections::default(),
            config: config.into(),
        })
    }

    /// Checks out a connection from the connection manager.
    fn checkout<T, P>(
        manager: &mut ArcMutexGuard<Self>,
        connector: Connector<T, P, R>,
    ) -> Checkout<T, P, R>
    where
        T: Transport<R> + Send,
        P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let multiplex = connector.multiplex();
        let mut connector: Option<Connector<T, P, R>> = Some(connector);
        if let Some(connection) = manager.pop() {
            trace!("connection found in pool");
            let request = connector.take().map(|mut c| c.take_request_unpinned());
            return Checkout::new(
                manager.downgrade(),
                rx,
                connector,
                Some(connection),
                request,
                &manager.config,
            );
        }

        trace!("checkout interested in pooled connections");
        manager.waiting.push_back(tx);

        if manager.connecting {
            trace!("connection in progress elsewhere, will wait");
            let request = connector.take().map(|mut c| c.take_request_unpinned());
            Checkout::new(
                manager.downgrade(),
                rx,
                connector,
                None,
                request,
                &manager.config,
            )
        } else {
            if multiplex {
                // Only block new connection attempts if we can multiplex on this one.
                trace!("checkout of multiplexed connection, other connections should wait");
                manager.connecting = true;
            }
            trace!("connecting to host");
            Checkout::new(
                manager.downgrade(),
                rx,
                connector,
                None,
                None,
                &manager.config,
            )
        }
    }

    /// Cancel the pending connection attempt.
    pub(in crate::client) fn cancel_connection(&mut self) {
        if self.connecting {
            trace!("pending connection cancelled");
        }
        self.connecting = false;
    }

    /// Mark a connection as connected, but not done with the handshake.
    ///
    /// New connection attempts will wait for this connection to complete the
    /// handshake and re-use it if possible.
    pub(in crate::client) fn connected_in_handshake(&mut self) {
        self.connecting = true
    }

    /// Push a connection back onto this manager
    //TODO: If arbitrary self types ever stabilize, this could use them.
    pub(super) fn push(&mut self, mut connection: C, manager: &WeakMutex<Self>) {
        self.connecting = false;
        let _span = tracing::trace_span!("manager::push").entered();

        trace!(waiters=%self.waiting.len(), "walking waiters");

        while let Some(waiter) = self.waiting.pop_front() {
            if waiter.is_closed() {
                trace!("skipping closed waiter");
                continue;
            }

            if let Some(conn) = connection.reuse() {
                trace!("re-usable connection will be sent to waiter");
                let pooled = Pooled {
                    connection: Some(conn),
                    manager: manager.clone(),
                };

                if waiter.send(pooled).is_err() {
                    trace!("waiter closed, skipping");
                    continue;
                };
            } else {
                trace!("connection not re-usable, but will be sent to waiter");
                let pooled = Pooled {
                    connection: Some(connection),
                    manager: manager.clone(),
                };

                let Err(pooled) = waiter.send(pooled) else {
                    trace!("connection sent");
                    return;
                };

                trace!("waiter closed, continuing");
                connection = pooled.take().unwrap();
            }
        }

        trace!("push idle connection");
        self.idle
            .push(connection, Some(self.config.max_idle_per_host));
    }

    pub(super) fn pop(&mut self) -> Option<C> {
        self.idle.pop(self.config.idle_timeout)
    }
}

#[cfg(all(test, feature = "mock"))]
mod tests {

    use std::time::Duration;

    use futures::FutureExt as _;

    use crate::client::conn::transport::mock::MockConnectionError;

    use super::*;
    use crate::client::conn::connector::Error;
    use crate::client::conn::protocol::mock::{MockRequest, MockSender};
    use crate::client::conn::stream::mock::MockStream;
    use crate::client::conn::transport::mock::MockTransport;

    #[tokio::test]
    async fn checkout_simple() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let conn = manager
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();
        drop(conn);

        let conn = manager
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), cid, "connection should be re-used");
        conn.close();
        drop(conn);

        let c2 = manager
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(c2.is_open());
        assert_ne!(c2.id(), cid, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_multiplex() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let conn = manager
            .checkout(MockTransport::reusable().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();
        drop(conn);

        let conn = manager
            .checkout(MockTransport::reusable().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), cid, "connection should be re-used");
        conn.close();
        drop(conn);

        let conn = manager
            .checkout(MockTransport::reusable().connector(MockRequest))
            .await
            .unwrap();
        assert!(conn.is_open());
        assert_ne!(conn.id(), cid, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_multiplex_contended() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut checkout_a =
            std::pin::pin!(manager.checkout(MockTransport::channel(rx).connector(MockRequest),));

        assert!(futures::poll!(&mut checkout_a).is_pending());

        let mut checkout_b =
            std::pin::pin!(manager.checkout(MockTransport::reusable().connector(MockRequest),));

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

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let conn = MockSender::single();

        let first_id = conn.id();

        let checkout = manager.checkout(MockTransport::single().connector(MockRequest));

        // Return the connection to the pool, sending it out to the new checkout
        // that is waiting, cancelling the checkout connect.

        manager
            .inner
            .lock()
            .push(conn, &WeakMutex::downgrade(&manager.inner));

        let conn = checkout.now_or_never().unwrap().unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), first_id, "connection should be re-used");
    }

    #[tokio::test]
    async fn checkout_idle_connected() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let conn_first = MockSender::single();

        let first_id = conn_first.id();

        tracing::debug!("Checkout from pool");

        let checkout = manager.checkout(MockTransport::single().connector(MockRequest));

        tracing::debug!("Checking interest");

        // At least one connection should be happening / waiting.
        assert!(
            !manager.inner.lock().waiting.is_empty(),
            "No connections are waiting"
        );

        tracing::debug!("Resolving checkout");

        let conn = checkout.now_or_never().unwrap().unwrap();

        tracing::debug!("Inserting original connection");
        // Return the connection to the pool, sending it out to the new checkout
        // that is waiting, cancelling the checkout connect.
        manager
            .inner
            .lock()
            .push(conn_first, &WeakMutex::downgrade(&manager.inner));

        assert!(conn.is_open());
        assert_ne!(conn.id(), first_id, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_drop_pool_err() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let start = manager.checkout(MockTransport::reusable().connector(MockRequest));
        let checkout = manager.checkout(MockTransport::reusable().connector(MockRequest));

        drop(start);
        drop(manager);

        assert!(checkout.now_or_never().unwrap().is_err());
    }

    #[tokio::test]
    async fn checkout_drop_pool() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let checkout = manager.checkout(MockTransport::reusable().connector(MockRequest));

        drop(manager);

        assert!(checkout.now_or_never().unwrap().is_ok());
    }

    #[tokio::test]
    async fn checkout_connection_error() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let checkout = manager.checkout(MockTransport::error().connector(MockRequest));

        let outcome = checkout.now_or_never().unwrap();
        let error = outcome.unwrap_err();
        assert!(matches!(error, Error::Connecting(MockConnectionError)));
    }

    #[tokio::test]
    async fn checkout_pool_cloned() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });
        let other = manager.clone();

        let conn = manager
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();
        drop(conn);

        let conn = other
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        assert_eq!(conn.id(), cid, "connection should be re-used");
        conn.close();
        drop(conn);

        let c2 = manager
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(c2.is_open());
        assert_ne!(c2.id(), cid, "connection should not be re-used");
    }

    #[tokio::test]
    async fn checkout_delayed_drop() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: true,
        });

        let conn = manager
            .checkout(MockTransport::single().connector(MockRequest))
            .await
            .unwrap();

        assert!(conn.is_open());
        let cid = conn.id();

        let checkout = manager.checkout(MockTransport::single().connector(MockRequest));

        drop(conn);
        let conn = checkout.await.unwrap();
        assert!(conn.is_open());
        assert_eq!(cid, conn.id());

        assert_eq!(manager.inner.lock().idle.len(), 1);
    }
}
