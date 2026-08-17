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
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
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
        mut connector: Connector<T, P, R>,
    ) -> Checkout<T, P, R>
    where
        T: Transport<R> + Send,
        P: Protocol<T::IO, R, Connection = C> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let multiplex = connector.multiplex();
        if let Some(connection) = manager.pop() {
            trace!("connection found in pool");
            let request = connector.take_request_unpinned();
            return Checkout::new_connected(manager.downgrade(), rx, connection, request);
        }

        trace!("checkout interested in pooled connections");
        manager.waiting.push_back(tx);

        if manager.connecting {
            trace!("connection in progress elsewhere, will wait");
            Checkout::new_connecting(manager.downgrade(), rx, connector)
        } else {
            // If we're about to block other connection attempts behind this
            // one, this checkout becomes the "leader" responsible for that
            // shared state: if it's abandoned before finishing, it must
            // release any checkouts that queued up waiting for it.
            let is_leader = multiplex;
            if multiplex {
                // Only block new connection attempts if we can multiplex on this one.
                trace!("checkout of multiplexed connection, other connections should wait");
                manager.connecting = true;
            }
            trace!("connecting to host");
            Checkout::new_idle(
                manager.downgrade(),
                rx,
                connector,
                &manager.config,
                is_leader,
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

    /// Mark the in-progress connection attempt as having failed.
    ///
    /// The checkouts that were waiting for this connection attempt (because
    /// it looked like it could be shared) can not be served by it any more.
    /// Rather than releasing all of them at once -- which would turn a
    /// single failure into a thundering herd of simultaneous connection
    /// attempts -- this releases exactly one of them (dropping its `Sender`
    /// wakes it, so it can attempt its own connection) and leaves the rest
    /// queued up behind it, as if it were the new leader. If that one also
    /// fails or is abandoned, its own call to this method releases the next
    /// one in turn, and so on, until either someone succeeds (waking
    /// everyone still queued via `push`) or the queue is exhausted.
    pub(in crate::client) fn connection_failed(&mut self) {
        while let Some(waiter) = self.waiting.pop_front() {
            if waiter.is_closed() {
                // Already abandoned, e.g. this is our own waiter (already
                // closed above, before we got here) or a waiter belonging to
                // a checkout that has already given up.
                continue;
            }

            trace!("connection attempt failed, releasing next waiter in line");
            // Leave `connecting` set: whoever we just released is now
            // effectively the leader that everyone else still in the queue
            // is waiting behind. Dropping `waiter` here closes its receiver.
            return;
        }

        trace!("connection attempt failed, no other waiters remain");
        self.connecting = false;
    }

    /// Mark a connection as connected, but not done with the handshake.
    ///
    /// New connection attempts will wait for this connection to complete the
    /// handshake and re-use it if possible.
    pub(in crate::client) fn connected_in_handshake(&mut self, multiplex: bool) {
        self.connecting = multiplex;
        if !multiplex {
            // We can't multiplex on this connection, so all waiters should give up
            // and start their own connection attempts.
            trace!(waiters=%self.waiting.len(), "dropping waiters");
            self.waiting.clear();
        }
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
    use crate::client::conn::protocol::mock::{MockProtocol, MockRequest, MockSender};
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
    async fn checkout_drop_pool_recover() {
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

        assert!(checkout.now_or_never().unwrap().is_ok());
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

    /// If the leading connection attempt fails before ever reaching the
    /// handshake stage (so `connected_in_handshake` is never called), any
    /// checkouts that queued up waiting for it must be released so they can
    /// fall back to their own connection attempts, rather than hang forever
    /// waiting on a connection that will never arrive.
    #[tokio::test]
    async fn checkout_connection_failure_releases_waiters() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let (stream_tx, stream_rx) = tokio::sync::oneshot::channel();

        // The leader uses a `Channel` transport, which reports that it can be
        // reused, so the manager will eagerly mark itself as `connecting` and
        // queue up the follower to wait for this connection instead of
        // connecting on its own.
        let leader = manager.checkout(MockTransport::channel(stream_rx).connector(MockRequest));
        let follower = manager.checkout(MockTransport::reusable().connector(MockRequest));

        assert_eq!(
            manager.inner.lock().waiting.len(),
            2,
            "leader and follower should both be queued waiting"
        );

        // Fail the leader's connection attempt before it ever reaches the
        // handshake, so `connected_in_handshake` is never invoked.
        drop(stream_tx);
        let leader_result = leader.await;
        assert!(leader_result.is_err(), "leader should fail to connect");

        // The follower must be released rather than hang forever waiting on a
        // connection that will never arrive, and should fall back on its own
        // connection attempt instead.
        let follower_result = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("follower should not hang waiting on the failed leader");
        assert!(follower_result.unwrap().is_open());
    }

    /// A variant of [`checkout_connection_failure_releases_waiters`] which
    /// verifies that the follower is actually woken up by the release, rather
    /// than merely being observed as released the next time it happens to be
    /// polled.
    #[tokio::test]
    async fn checkout_connection_failure_wakes_waiting_checkout() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let (stream_tx, stream_rx) = tokio::sync::oneshot::channel();

        let leader = manager.checkout(MockTransport::channel(stream_rx).connector(MockRequest));
        let follower = manager.checkout(MockTransport::reusable().connector(MockRequest));

        // Spawn the follower onto its own task, so it can only make progress
        // if something wakes it up; we are not polling it ourselves.
        let follower_task = tokio::spawn(follower);

        // Give the spawned task a chance to run and register its waker while
        // pending on the leader.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        drop(stream_tx);
        let leader_result = leader.await;
        assert!(leader_result.is_err(), "leader should fail to connect");

        let follower_result = tokio::time::timeout(Duration::from_secs(1), follower_task)
            .await
            .expect("follower task should be woken and complete promptly")
            .expect("follower task should not panic");

        assert!(follower_result.unwrap().is_open());
    }

    /// When a connection attempt fails with multiple checkouts queued up
    /// behind it, only one of them should be released to attempt its own
    /// connection; the rest should remain queued behind that one, as if it
    /// were the new leader. This avoids turning a single failure into a
    /// thundering herd of simultaneous connection attempts. If that newly
    /// released checkout also fails, it releases the next one in turn.
    #[tokio::test]
    async fn checkout_connection_failure_releases_one_waiter_at_a_time() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let (leader_tx, leader_rx) = tokio::sync::oneshot::channel();
        let (follower_a_tx, follower_a_rx) = tokio::sync::oneshot::channel();

        let mut leader = std::pin::pin!(
            manager.checkout(MockTransport::channel(leader_rx).connector(MockRequest))
        );
        assert!(futures::poll!(&mut leader).is_pending());

        let mut follower_a = std::pin::pin!(
            manager.checkout(MockTransport::channel(follower_a_rx).connector(MockRequest))
        );
        assert!(futures::poll!(&mut follower_a).is_pending());

        let mut follower_b =
            std::pin::pin!(manager.checkout(MockTransport::reusable().connector(MockRequest)));
        assert!(futures::poll!(&mut follower_b).is_pending());

        assert_eq!(
            manager.inner.lock().waiting.len(),
            3,
            "leader and both followers should be queued waiting"
        );

        // Fail the leader. Only `follower_a` should be released to attempt
        // its own connection; `follower_b` must remain queued.
        drop(leader_tx);
        let leader_result = leader.await;
        assert!(leader_result.is_err(), "leader should fail to connect");

        assert_eq!(
            manager.inner.lock().waiting.len(),
            1,
            "only follower_b should remain queued after the leader fails"
        );

        // `follower_b` must still be untouched: nobody has released it yet,
        // so it should still just be waiting, not attempting its own
        // connection (which would resolve immediately for a reusable mock
        // transport).
        assert!(futures::poll!(&mut follower_b).is_pending());

        // `follower_a` has been promoted and should now be driving its own
        // (still-blocked) connection attempt, rather than resolving.
        assert!(futures::poll!(&mut follower_a).is_pending());

        // Fail `follower_a`'s own connection attempt too. This should, in
        // turn, release `follower_b`.
        drop(follower_a_tx);
        let follower_a_result = follower_a.await;
        assert!(
            follower_a_result.is_err(),
            "follower_a should fail to connect"
        );

        assert_eq!(
            manager.inner.lock().waiting.len(),
            0,
            "follower_b should have been released after follower_a also failed"
        );

        let follower_b_result = tokio::time::timeout(Duration::from_secs(1), follower_b)
            .await
            .expect("follower_b should not hang once released");
        assert!(follower_b_result.unwrap().is_open());
    }

    /// If the protocol eagerly reports that it can multiplex, but once
    /// connected decides that this particular connection cannot actually be
    /// shared, any checkouts that queued up waiting for it must fall back on
    /// their own connection attempts.
    #[tokio::test]
    async fn checkout_multiplex_ready_false_releases_waiters() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let leader_connector = Connector::new(
            MockTransport::reusable(),
            MockProtocol::new(true).with_multiplex_ready(false),
            MockRequest,
        );

        let leader = manager.checkout(leader_connector);
        let follower = manager.checkout(MockTransport::reusable().connector(MockRequest));

        assert_eq!(
            manager.inner.lock().waiting.len(),
            2,
            "leader and follower should both be queued waiting"
        );

        let leader_conn = leader.await.unwrap();
        assert!(leader_conn.is_open());

        // The follower should not have received the leader's connection,
        // since it turned out not to be shareable; instead it should have
        // connected on its own.
        let follower_conn = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("follower should not hang waiting on a non-multiplexed leader")
            .unwrap();
        assert!(follower_conn.is_open());
        assert_ne!(
            follower_conn.id(),
            leader_conn.id(),
            "follower should have connected on its own, not shared the leader's connection"
        );
    }

    /// If the checkout responsible for the manager's shared connecting
    /// state (the leader) is abandoned before its connection attempt ever
    /// finishes (e.g. because its caller lost a select race, hit a timeout,
    /// or otherwise dropped it), any checkouts that queued up waiting for it
    /// must be released, rather than hang forever waiting on a connection
    /// attempt that will never resume.
    #[tokio::test]
    async fn checkout_leader_abandoned_releases_waiters() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let (_stream_tx, stream_rx) = tokio::sync::oneshot::channel();

        let leader = manager.checkout(MockTransport::channel(stream_rx).connector(MockRequest));
        let follower = manager.checkout(MockTransport::reusable().connector(MockRequest));

        assert_eq!(
            manager.inner.lock().waiting.len(),
            2,
            "leader and follower should both be queued waiting"
        );

        // Abandon the leader entirely, without ever polling it to
        // completion, as if its caller lost interest.
        drop(leader);

        let follower_result = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("follower should not hang waiting on an abandoned leader");
        assert!(follower_result.unwrap().is_open());

        assert!(
            !manager.inner.lock().connecting,
            "connecting flag should be reset"
        );
    }

    /// Dropping a checkout that was only waiting on someone else's
    /// connection (a follower, not the leader) must not disrupt the leader
    /// or any other followers: the leader's connection attempt is still
    /// healthy and may still be shared.
    #[tokio::test]
    async fn checkout_follower_drop_does_not_disrupt_leader_or_other_waiters() {
        let _ = tracing_subscriber::fmt::try_init();

        let manager = ConnectionManager::<MockSender, MockRequest>::new(ConnectionManagerConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_idle_per_host: 5,
            continue_after_preemption: false,
        });

        let (stream_tx, stream_rx) = tokio::sync::oneshot::channel();

        let mut leader = std::pin::pin!(
            manager.checkout(MockTransport::channel(stream_rx).connector(MockRequest))
        );
        assert!(futures::poll!(&mut leader).is_pending());

        let follower_a = manager.checkout(MockTransport::reusable().connector(MockRequest));
        let follower_b = manager.checkout(MockTransport::reusable().connector(MockRequest));

        // follower_a's own caller gives up on it, but the leader's
        // connection attempt is still healthy and in progress, so
        // follower_b should still be served by it.
        drop(follower_a);

        assert!(stream_tx.send(MockStream::reusable()).is_ok());

        let leader_conn = leader.await.unwrap();
        assert!(leader_conn.is_open());

        let follower_b_conn = tokio::time::timeout(Duration::from_secs(1), follower_b)
            .await
            .expect("follower_b should not hang")
            .unwrap();

        assert_eq!(
            follower_b_conn.id(),
            leader_conn.id(),
            "follower_b should still share the leader's connection despite follower_a's drop"
        );
    }
}
