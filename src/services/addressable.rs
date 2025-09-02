use std::{
    convert::Infallible,
    fmt,
    pin::Pin,
    task::{Context, Poll, ready},
};

use crate::client::conn::{ConnectionError, dns::Resolver};

/// A layer to run a resolver first, and continue with a tuple
/// of (request, address) for inner services, as well as stripping
/// a tuple of (response, address) down to just (response).
#[derive(Debug, Clone)]
pub struct ResolvedAddressableLayer<D> {
    resolver: D,
}

impl<D> ResolvedAddressableLayer<D> {
    /// Create a new addressable resolver layer
    pub fn new(resolver: D) -> Self {
        Self { resolver }
    }
}

impl<D, S> tower::Layer<S> for ResolvedAddressableLayer<D>
where
    D: Clone,
{
    type Service = ResolvedAddressableService<D, S>;

    fn layer(&self, inner: S) -> Self::Service {
        ResolvedAddressableService::new(self.resolver.clone(), inner)
    }
}

/// A service which runs a resolver, and passes the address along to
/// an "Addressable" service, one which accepts a request with an address,
/// and returns responses with their addresses.
#[derive(Debug, Clone)]
pub struct ResolvedAddressableService<D, S> {
    resolver: D,
    service: S,
}

impl<D, S> ResolvedAddressableService<D, S> {
    /// Create a new resolved addressable service
    pub fn new(resolver: D, service: S) -> Self {
        Self { resolver, service }
    }
}

impl<D, S, R, U> tower::Service<R> for ResolvedAddressableService<D, S>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address), Response = (U, D::Address)> + Clone,
{
    type Response = U;

    type Error = ConnectionError<D::Error, Infallible, Infallible, S::Error>;

    type Future = ResolvedAddressableFuture<D, S, R, U>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.resolver.poll_ready(cx)).map_err(ConnectionError::Resolving)?;
        self.service
            .poll_ready(cx)
            .map_err(ConnectionError::Service)
    }

    fn call(&mut self, req: R) -> Self::Future {
        let resolve = self.resolver.resolve(&req);

        // Get the ready service
        let service = self.service.clone();
        let service = std::mem::replace(&mut self.service, service);

        ResolvedAddressableFuture::new(resolve, service, req)
    }
}

#[pin_project::pin_project(project=StateProject)]
enum State<D, S, R>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address)>,
{
    Resolving {
        #[pin]
        resolver: D::Future,
        service: S,
        request: Option<R>,
    },

    Serving {
        #[pin]
        service: S::Future,
    },
}

impl<D, S, R> fmt::Debug for State<D, S, R>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address)>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            State::Resolving { .. } => write!(f, "Resolving"),
            State::Serving { .. } => write!(f, "Serving"),
        }
    }
}

/// Future returned by `ResolvedAddressableService`
#[pin_project::pin_project]
pub struct ResolvedAddressableFuture<D, S, R, U>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address), Response = (U, D::Address)>,
{
    #[pin]
    state: State<D, S, R>,
}

impl<D, S, R, U> fmt::Debug for ResolvedAddressableFuture<D, S, R, U>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address), Response = (U, D::Address)>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedAddressableFuture")
            .field("state", &self.state)
            .finish()
    }
}

impl<D, S, R, U> ResolvedAddressableFuture<D, S, R, U>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address), Response = (U, D::Address)>,
{
    fn new(resolver: D::Future, service: S, request: R) -> Self {
        Self {
            state: State::Resolving {
                resolver,
                service,
                request: Some(request),
            },
        }
    }
}

impl<D, S, R, U> Future for ResolvedAddressableFuture<D, S, R, U>
where
    D: Resolver<R>,
    S: tower::Service<(R, D::Address), Response = (U, D::Address)>,
{
    type Output = Result<U, ConnectionError<D::Error, Infallible, Infallible, S::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProject::Resolving {
                    resolver,
                    service,
                    request,
                } => {
                    let address = ready!(resolver.poll(cx)).map_err(ConnectionError::Resolving)?;
                    let request = request.take().expect("request stolen before ready");
                    let future = service.call((request, address));
                    this.state.as_mut().set(State::Serving { service: future });
                }
                StateProject::Serving { service } => {
                    return match service.poll(cx) {
                        Poll::Ready(Ok((response, _))) => Poll::Ready(Ok(response)),
                        Poll::Ready(Err(error)) => {
                            Poll::Ready(Err(ConnectionError::Service(error)))
                        }
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}
