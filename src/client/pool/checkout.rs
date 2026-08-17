use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use std::task::Context;
use std::task::Poll;
use std::task::ready;

use pin_project::pin_project;
use pin_project::pinned_drop;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::oneshot::Receiver;
use tokio_util::sync::PollSemaphore;
use tracing::debug;
use tracing::trace;

use crate::client::conn::Protocol;
use crate::client::conn::Transport;
use crate::client::conn::connector::Error as ConnectorError;
use crate::client::conn::connector::{Connector, ConnectorMeta};

#[cfg(debug_assertions)]
use self::ids::CheckoutId;
use super::ManagerRef;
use super::PoolableConnection;
use super::Pooled;
use super::manager::ConnectionManagerConfig;

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

    /// There is no manager for connections to wait for.
    None,
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
            Waiting::None => {}
        }

        *self = Waiting::None;
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
            Waiting::None => f.debug_tuple("Nomanager").finish(),
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

impl<C, B> WaitingPoll<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    fn is_ready(&self) -> bool {
        matches!(self, WaitingPoll::Connected(_) | WaitingPoll::Closed)
    }
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
            WaitingProjected::None => Poll::Ready(WaitingPoll::Closed),
        };

        if let Poll::Ready(p) = &polled {
            if p.is_ready() {
                self.as_mut().set(Waiting::None);
            }
        };

        polled
    }
}

#[pin_project(project = CheckoutConnectingProj)]
pub(crate) enum InnerCheckoutConnecting<T, P, R>
where
    T: Transport<R>,
    P: Protocol<T::IO, R>,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    Done(Option<R>),
    Connected(Option<(P::Connection, R)>),
    Connecting(Pin<Box<Connector<T, P, R>>>),
    #[allow(clippy::type_complexity)]
    ConnectingWithDelayDrop(Option<Pin<Box<Connector<T, P, R>>>>),
    ConnectingDelayed(Pin<Box<Connector<T, P, R>>>),
}

impl<T, P, R> fmt::Debug for InnerCheckoutConnecting<T, P, R>
where
    T: Transport<R>,
    P: Protocol<T::IO, R>,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            InnerCheckoutConnecting::Done(_) => f.debug_tuple("Done").finish(),
            InnerCheckoutConnecting::Connected(_) => f.debug_tuple("Connected").finish(),
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

/// Tracks whether this checkout needs to (or already has) acquired a
/// permit from the connection manager's limit on simultaneous connection
/// attempts, if one is configured.
enum Permit {
    /// No limit is configured for this manager, or a permit is not
    /// required at all (e.g. this checkout is not tied to a manager, or
    /// already has a connection and will never drive a connector).
    Unbounded,
    /// Waiting to acquire a permit before starting or continuing to poll
    /// our connection attempt.
    Acquiring(PollSemaphore),
    /// Currently holds a permit for our in-progress connection attempt.
    /// The permit itself is never read; it is held only so that dropping it
    /// (when we release, or when the whole checkout is dropped) returns the
    /// slot to the semaphore.
    Acquired(#[allow(dead_code)] OwnedSemaphorePermit),
}

impl Permit {
    fn new(limit: Option<Arc<Semaphore>>) -> Self {
        match limit {
            Some(semaphore) => Permit::Acquiring(PollSemaphore::new(semaphore)),
            None => Permit::Unbounded,
        }
    }

    /// Ensure a permit is held before continuing to poll a connection
    /// attempt, acquiring one from the manager's semaphore if necessary.
    fn poll_acquire(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match self {
                Permit::Unbounded | Permit::Acquired(_) => return Poll::Ready(()),
                Permit::Acquiring(semaphore) => match semaphore.poll_acquire(cx) {
                    Poll::Ready(Some(permit)) => *self = Permit::Acquired(permit),
                    Poll::Ready(None) => {
                        // The semaphore was closed. We never close it
                        // ourselves, so this should not happen in practice,
                        // but fall back to treating the attempt as
                        // unbounded rather than hanging forever.
                        *self = Permit::Unbounded;
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }

    /// Release any held (or pending) permit once a connection attempt has
    /// concluded, successfully or not.
    fn release(&mut self) {
        *self = Permit::Unbounded;
    }
}

/// A checkout of a connection from a connection manager.
#[pin_project(PinnedDrop)]
pub struct Checkout<T, P, R>
where
    T: Transport<R> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    manager: ManagerRef<P::Connection, R>,
    #[pin]
    waiter: Waiting<P::Connection, R>,
    #[pin]
    inner: InnerCheckoutConnecting<T, P, R>,
    meta: ConnectorMeta,
    /// Whether this checkout is the one responsible for the manager's shared
    /// `connecting` flag (i.e. it is the checkout other checkouts are
    /// queued up waiting behind). Only such a checkout should release those
    /// waiters if it is abandoned before its connection attempt finishes.
    is_leader: bool,
    /// Tracks the permit for the manager's limit on simultaneous connection
    /// attempts, if one is configured.
    permit: Permit,
    #[cfg(debug_assertions)]
    id: CheckoutId,
}

impl<T, P, R> fmt::Debug for Checkout<T, P, R>
where
    T: Transport<R> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Checkout")
            .field("manager", &self.manager)
            .field("waiter", &self.waiter)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, P, R> Checkout<T, P, R>
where
    T: Transport<R> + Send + 'static,
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
                    manager: this.manager.clone(),
                    waiter: Waiting::None,
                    inner: InnerCheckoutConnecting::ConnectingDelayed(connector.take().unwrap()),
                    meta: ConnectorMeta::new(), // New meta to avoid holding spans in the spawned task
                    is_leader: *this.is_leader,
                    // Carry over whatever permit state we already have (held
                    // or still being acquired): this is the same connection
                    // attempt continuing in the background, not a new one.
                    permit: std::mem::replace(this.permit, Permit::Unbounded),
                    #[cfg(debug_assertions)]
                    id: *this.id,
                })
            }
            _ => None,
        }
    }

    pub(crate) fn take_request_pinned(mut self: Pin<&mut Self>) -> R {
        match self.as_mut().project().inner.project() {
            CheckoutConnectingProj::Done(request) => {
                request.take().expect("checkout request already taken")
            }
            CheckoutConnectingProj::Connected(connection) => {
                let (_, request) = connection.take().expect("checkout request already taken");
                request
            }
            CheckoutConnectingProj::Connecting(pin) => pin.as_mut().take_request_pinned(),
            CheckoutConnectingProj::ConnectingWithDelayDrop(pin) => pin
                .take()
                .expect("checkout connector already taken")
                .as_mut()
                .take_request_pinned(),
            CheckoutConnectingProj::ConnectingDelayed(pin) => pin.as_mut().take_request_pinned(),
        }
    }

    /// Constructs a checkout which does not hold a reference to the manager
    /// and so is only waiting on the connector.
    ///
    /// This checkout will always proceed with the connector, uninterrupted by
    /// alternative connection solutions. It will not use the "delayed drop"
    /// procedure to finish connections if dropped.
    ///
    /// This is useful when using a checkout to poll a connection to readiness
    /// without a manager, or in a context in which the associated connection cannot
    /// or should not be shared with the manager.
    pub(crate) fn detached(connector: Connector<T, P, R>) -> Self {
        #[cfg(debug_assertions)]
        let id = CheckoutId::new();

        #[cfg(debug_assertions)]
        tracing::trace!(%id, "creating detached checkout");

        Self {
            manager: ManagerRef::none(),
            waiter: Waiting::None,
            inner: InnerCheckoutConnecting::Connecting(Box::pin(connector)),
            meta: ConnectorMeta::new(),
            is_leader: false,
            // A detached checkout has no manager, and so no shared limit on
            // simultaneous connection attempts to observe.
            permit: Permit::Unbounded,
            #[cfg(debug_assertions)]
            id,
        }
    }

    pub(super) fn new_connected(
        manager: ManagerRef<P::Connection, R>,
        waiter: Receiver<Pooled<P::Connection, R>>,
        connection: P::Connection,
        request: R,
    ) -> Self {
        #[cfg(debug_assertions)]
        let id = CheckoutId::new();
        let meta = ConnectorMeta::new();

        tracing::trace!("connection recieved from manager");
        Self {
            manager,
            waiter: Waiting::Idle(waiter),
            inner: InnerCheckoutConnecting::Connected(Some((connection, request))),
            meta,
            is_leader: false,
            // We already have a connection; we will never drive a
            // connector, so there is nothing to acquire a permit for.
            permit: Permit::Unbounded,
            #[cfg(debug_assertions)]
            id,
        }
    }

    /// Constructs a checkout which is only waiting behind another
    /// checkout's connection attempt, but carries its own connector in case
    /// it needs to fall back to attempting a connection itself.
    ///
    /// `connecting_permits` should be the manager's limit (if any) on
    /// simultaneous connection attempts: if this checkout is later released
    /// from waiting and must attempt its own connection, it will first
    /// acquire a permit, just as the original leader would have.
    pub(super) fn new_connecting(
        manager: ManagerRef<P::Connection, R>,
        waiter: Receiver<Pooled<P::Connection, R>>,
        connector: Connector<T, P, R>,
        connecting_permits: Option<Arc<Semaphore>>,
    ) -> Self {
        #[cfg(debug_assertions)]
        let id = CheckoutId::new();
        let meta = ConnectorMeta::new();

        #[cfg(debug_assertions)]
        tracing::trace!(%id, "creating new checkout");

        Self {
            manager,
            waiter: Waiting::Connecting(waiter),
            inner: InnerCheckoutConnecting::Connecting(Box::pin(connector)),
            meta,
            is_leader: false,
            permit: Permit::new(connecting_permits),
            #[cfg(debug_assertions)]
            id,
        }
    }

    /// Constructs a checkout which is responsible for establishing a new
    /// connection on behalf of the manager.
    ///
    /// `is_leader` should be `true` exactly when this checkout is the one
    /// that caused the manager's shared `connecting` flag to be set (i.e.
    /// other checkouts may be queued up waiting on it). If such a checkout is
    /// abandoned before its connection attempt finishes, those waiters must
    /// be released so they can attempt their own connections instead of
    /// hanging forever.
    ///
    /// `connecting_permits` is the manager's limit (if any) on simultaneous
    /// connection attempts; a permit will be acquired from it before this
    /// checkout actually starts driving its connector.
    pub(super) fn new_idle(
        manager: ManagerRef<P::Connection, R>,
        waiter: Receiver<Pooled<P::Connection, R>>,
        connector: Connector<T, P, R>,
        config: &ConnectionManagerConfig,
        is_leader: bool,
        connecting_permits: Option<Arc<Semaphore>>,
    ) -> Self {
        #[cfg(debug_assertions)]
        let id = CheckoutId::new();
        let meta = ConnectorMeta::new();

        #[cfg(debug_assertions)]
        tracing::trace!( %id, "creating new checkout");

        let inner = if config.continue_after_preemption {
            InnerCheckoutConnecting::ConnectingWithDelayDrop(Some(Box::pin(connector)))
        } else {
            InnerCheckoutConnecting::Connecting(Box::pin(connector))
        };

        Self {
            manager,
            waiter: Waiting::Idle(waiter),
            inner,

            meta,
            is_leader,
            permit: Permit::new(connecting_permits),
            #[cfg(debug_assertions)]
            id,
        }
    }
}

impl<T, P, R> Future for Checkout<T, P, R>
where
    T: Transport<R> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    type Output = Result<
        Pooled<P::Connection, R>,
        ConnectorError<<T as Transport<R>>::Error, <P as Protocol<T::IO, R>>::Error>,
    >;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        let _entered = this.meta.current().clone().entered();

        {
            // Outcomes from .poll_waiter:
            // - Ready(Some(connection)) => return connection
            // - Ready(None) => continue to check manager, we don't have a waiter.
            // - Pending => wait on the waiter to complete, don't bother to check manager.

            // Open questions: Should we check the manager for a different connection when the
            // waiter is pending? Probably not, ideally our semantics should keep the manager
            // from containing multiple connections if they can be multiplexed.

            // If we were purely waiting behind someone else's connection
            // attempt (rather than also driving our own), remember that here
            // so we can tell, below, whether our waiter closing means we are
            // now the one responsible for driving a connection attempt.
            let was_following = matches!(*this.waiter, Waiting::Connecting(_));

            if let WaitingPoll::Connected(connection) = ready!(this.waiter.as_mut().poll(cx)) {
                debug!("connection recieved from waiter");

                return Poll::Ready(Ok(connection));
            }

            if was_following {
                // Whoever we were waiting behind gave up (failed, or was
                // abandoned) without ever delivering a connection to us. We
                // are now the one attempting a connection on everyone's
                // behalf: if we, in turn, fail or are abandoned, we must
                // release the next waiter in line ourselves.
                trace!("promoted from waiting to attempting our own connection");
                *this.is_leader = true;
            }
        }

        trace!("polling for new connection");
        // Try to connect while we also wait for a checkout to be ready.

        match this.inner.as_mut().project() {
            CheckoutConnectingProj::Done(_) => {
                // The connection was already returned elsewhere, did this future get polled again?
                Poll::Ready(Err(ConnectorError::Unavailable))
            }
            CheckoutConnectingProj::Connected(conn) => {
                this.waiter.close();
                let (connection, request) = conn.take().expect("checkout request already taken");
                this.inner.set(InnerCheckoutConnecting::Done(Some(request)));
                Poll::Ready(Ok(register_connected(this.manager, connection)))
            }
            CheckoutConnectingProj::Connecting(connector) => {
                ready!(this.permit.poll_acquire(cx));

                let result = ready!(connector.as_mut().poll_connector(
                    {
                        let manager = this.manager.clone();
                        move |multiplex| {
                            trace!(
                                "connection can be shared, telling manager to wait for handshake"
                            );
                            if let Some(mut manager) = manager.lock() {
                                manager.connected_in_handshake(multiplex);
                            }
                        }
                    },
                    this.meta,
                    cx
                ));

                this.waiter.close();
                this.permit.release();
                let request = connector.as_mut().take_request_pinned();
                this.inner.set(InnerCheckoutConnecting::Done(Some(request)));

                match result {
                    Ok(connection) => Poll::Ready(Ok(register_connected(this.manager, connection))),
                    Err(e) => {
                        release_waiters_on_failure(this.manager);
                        Poll::Ready(Err(e))
                    }
                }
            }
            CheckoutConnectingProj::ConnectingWithDelayDrop(Some(connector))
            | CheckoutConnectingProj::ConnectingDelayed(connector) => {
                ready!(this.permit.poll_acquire(cx));

                let result = ready!(connector.as_mut().poll_connector(
                    {
                        let manager = this.manager.clone();
                        move |multiplex| {
                            trace!(
                                "connection can be shared, telling manager to wait for handshake"
                            );
                            if let Some(mut manager) = manager.lock() {
                                manager.connected_in_handshake(multiplex);
                            }
                        }
                    },
                    this.meta,
                    cx
                ));

                this.waiter.close();
                this.permit.release();
                let request = connector.as_mut().take_request_pinned();
                this.inner.set(InnerCheckoutConnecting::Done(Some(request)));

                match result {
                    Ok(connection) => Poll::Ready(Ok(register_connected(this.manager, connection))),
                    Err(e) => {
                        release_waiters_on_failure(this.manager);
                        Poll::Ready(Err(e))
                    }
                }
            }
            CheckoutConnectingProj::ConnectingWithDelayDrop(None) => {
                // Something stole our connection, this is an error state.
                panic!("connection was stolen from checkout")
            }
        }
    }
}

/// Release any other checkouts waiting on this (now failed) connection attempt.
///
/// When a checkout is connecting on behalf of the manager (e.g. because it is
/// expected to produce a connection that can be shared), other checkouts may
/// be parked in [`Waiting::Connecting`], purely waiting for this attempt to
/// finish rather than connecting themselves. If this attempt fails, nothing
/// else will ever complete their waiting channel, so they must be released
/// here so they can fall back to their own connection attempts.
fn release_waiters_on_failure<C, B>(managerref: &ManagerRef<C, B>)
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    if let Some(mut manager) = managerref.lock() {
        manager.connection_failed();
    }
}

/// Register a connection with the manager referenced here.
fn register_connected<C, B>(managerref: &ManagerRef<C, B>, mut connection: C) -> Pooled<C, B>
where
    C: PoolableConnection<B>,
    B: Send + 'static,
{
    if let Some(mut manager) = managerref.lock() {
        if let Some(reused) = connection.reuse() {
            manager.push(reused, managerref);
            return Pooled {
                connection: Some(connection),
                manager: ManagerRef::none(),
            };
        } else {
            return Pooled {
                connection: Some(connection),
                manager: managerref.clone(),
            };
        }
    }

    // No manager or lock was available, so we can't add the connection to the manager.
    //
    // Returning the original managerref + token means that if this was temporary,
    // and we can grab the manager later, we will do so.
    Pooled {
        connection: Some(connection),
        manager: managerref.clone(),
    }
}

#[pinned_drop]
impl<T, P, R> PinnedDrop for Checkout<T, P, R>
where
    T: Transport<R> + Send + 'static,
    P: Protocol<T::IO, R> + Send + 'static,
    P::Connection: PoolableConnection<R>,
    R: Send + 'static,
{
    fn drop(mut self: Pin<&mut Self>) {
        if let Some(checkout) = self.as_mut().as_delayed() {
            #[cfg(debug_assertions)]
            tracing::trace!(id=%self.id, "drop for delayed checkout");
            tokio::task::spawn(async move {
                if let Err(err) = checkout.await {
                    tracing::error!(error=%err, "error during delayed drop");
                }
            });
        } else {
            // If we are the checkout responsible for the manager's shared
            // `connecting` flag, and we're being dropped without ever
            // having finished our connection attempt (successfully or
            // with an error, both of which transition `inner` to `Done`),
            // then anyone queued up waiting for us must be released so they
            // can attempt their own connection instead of hanging forever
            // on a connection that will never arrive.
            //
            // A non-leader (a checkout that was only ever waiting on someone
            // else) has no such obligation: other waiters may still be
            // legitimately served by the real leader, so its drop should
            // only clear its own claim on `connecting`.
            let abandoned_before_finishing =
                !matches!(self.inner, InnerCheckoutConnecting::Done(_));
            let is_leader = self.is_leader;

            // Close our own waiter first. Otherwise, if we're the leader and
            // end up releasing the next waiter below, we could mistake our
            // own (still technically open) entry in the queue for one that
            // actually needs releasing, wasting the release on ourselves.
            self.as_mut().project().waiter.close();

            if let Some(mut manager) = self.manager.lock() {
                if is_leader && abandoned_before_finishing {
                    manager.connection_failed();
                } else {
                    manager.cancel_connection();
                }
            }
            #[cfg(debug_assertions)]
            tracing::trace!(id=%self.id, "drop for checkout");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use static_assertions::assert_impl_all;

    assert_impl_all!(ConnectorError<std::io::Error, std::io::Error>: std::error::Error, Send, Sync, Into<BoxError>);

    use crate::BoxError;

    #[cfg(feature = "mock")]
    use crate::client::conn::transport::mock::MockTransport;

    #[cfg(feature = "mock")]
    use crate::client::conn::protocol::mock::{MockRequest, MockSender};

    #[cfg(feature = "mock")]
    fn mock_pooled() -> Pooled<MockSender, MockRequest> {
        // Use a shareable connection so that dropping the `Pooled` value in a
        // plain (non-Tokio) test does not try to spawn a background task.
        Pooled {
            connection: Some(MockSender::reusable()),
            manager: ManagerRef::none(),
        }
    }

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
        let transport = MockTransport::single();

        let checkout = Checkout::detached(transport.connector(MockRequest));

        assert!(checkout.manager.is_none());
        assert!(matches!(
            checkout.inner,
            InnerCheckoutConnecting::Connecting(_)
        ));
        assert!(matches!(checkout.waiter, Waiting::None));

        let dbg = format!("{checkout:?}");
        assert!(dbg.starts_with("Checkout { "));

        let connection = checkout.await.unwrap();
        assert!(connection.is_open());
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_none_is_immediately_closed() {
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> = Box::pin(Waiting::None);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(matches!(poll, Poll::Ready(WaitingPoll::Closed)));
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_debug_variants() {
        let none: Waiting<MockSender, MockRequest> = Waiting::None;
        assert_eq!(format!("{none:?}"), "Nomanager");

        let (_tx, rx) = tokio::sync::oneshot::channel();
        let idle: Waiting<MockSender, MockRequest> = Waiting::Idle(rx);
        assert_eq!(format!("{idle:?}"), "Idle");

        let (_tx, rx) = tokio::sync::oneshot::channel();
        let connecting: Waiting<MockSender, MockRequest> = Waiting::Connecting(rx);
        assert_eq!(format!("{connecting:?}"), "Connecting");
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_idle_pending_reports_not_ready_without_closing() {
        // An `Idle` waiter reports `NotReady` (rather than blocking the whole
        // future) when its channel is pending, so that the checkout can also
        // attempt its own connection while it waits.
        let (tx, rx) = tokio::sync::oneshot::channel::<Pooled<MockSender, MockRequest>>();
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> = Box::pin(Waiting::Idle(rx));

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(matches!(poll, Poll::Ready(WaitingPoll::NotReady)));
        assert!(!tx.is_closed(), "pending idle waiter should stay open");
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_idle_resolves_when_connection_sent() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> = Box::pin(Waiting::Idle(rx));

        assert!(tx.send(mock_pooled()).is_ok());

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(matches!(poll, Poll::Ready(WaitingPoll::Connected(_))));
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_idle_closed_when_sender_dropped() {
        let (tx, rx) = tokio::sync::oneshot::channel::<Pooled<MockSender, MockRequest>>();
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> = Box::pin(Waiting::Idle(rx));

        drop(tx);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(matches!(poll, Poll::Ready(WaitingPoll::Closed)));
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_connecting_pending_blocks_the_whole_future() {
        // Unlike `Idle`, a pending `Connecting` waiter blocks the entire
        // future rather than reporting `NotReady`, since a checkout in this
        // mode should not attempt its own connection while another is in
        // progress elsewhere.
        let (_tx, rx) = tokio::sync::oneshot::channel::<Pooled<MockSender, MockRequest>>();
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> =
            Box::pin(Waiting::Connecting(rx));

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(poll.is_pending());
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_connecting_resolves_when_connection_sent() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> =
            Box::pin(Waiting::Connecting(rx));

        assert!(tx.send(mock_pooled()).is_ok());

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(matches!(poll, Poll::Ready(WaitingPoll::Connected(_))));
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_connecting_closed_when_sender_dropped() {
        // This is the key recovery path: if the leading connection attempt
        // fails or is abandoned and releases its waiters, a checkout that was
        // purely waiting must observe `Closed` so it can fall back to its own
        // connection attempt.
        let (tx, rx) = tokio::sync::oneshot::channel::<Pooled<MockSender, MockRequest>>();
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> =
            Box::pin(Waiting::Connecting(rx));

        drop(tx);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let poll = waiting.as_mut().poll(&mut cx);
        assert!(matches!(poll, Poll::Ready(WaitingPoll::Closed)));
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_resets_to_none_once_ready() {
        let (tx, rx) = tokio::sync::oneshot::channel::<Pooled<MockSender, MockRequest>>();
        drop(tx);
        let mut waiting: Pin<Box<Waiting<MockSender, MockRequest>>> =
            Box::pin(Waiting::Connecting(rx));

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let _ = waiting.as_mut().poll(&mut cx);
        assert!(matches!(*waiting, Waiting::None));
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_close_marks_receiver_closed_and_resets_to_none() {
        let (tx, rx) = tokio::sync::oneshot::channel::<Pooled<MockSender, MockRequest>>();
        let mut waiting: Waiting<MockSender, MockRequest> = Waiting::Idle(rx);

        waiting.close();

        assert!(matches!(waiting, Waiting::None));
        assert!(tx.is_closed());
    }

    #[cfg(feature = "mock")]
    #[test]
    fn waiting_poll_is_ready_variants() {
        assert!(WaitingPoll::<MockSender, MockRequest>::Closed.is_ready());
        assert!(WaitingPoll::<MockSender, MockRequest>::Connected(mock_pooled()).is_ready());
        assert!(!WaitingPoll::<MockSender, MockRequest>::NotReady.is_ready());
    }
}
