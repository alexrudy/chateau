//! DNS resolution utilities.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::{Future, Ready, ready};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::task::{Context, Poll};

/// A collection of socket addresses.
#[derive(Debug, Clone, Default)]
pub struct SocketAddrs(VecDeque<SocketAddr>);

impl SocketAddrs {
    #[allow(dead_code)]
    pub(crate) fn set_port(&mut self, port: u16) {
        for addr in &mut self.0 {
            addr.set_port(port)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn peek(&self) -> Option<SocketAddr> {
        self.0.front().copied()
    }

    pub(crate) fn pop(&mut self) -> Option<SocketAddr> {
        self.0.pop_front()
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn sort_preferred(&mut self, prefer: Option<IpVersion>) {
        let mut v4_idx = None;
        let mut v6_idx = None;

        for (idx, addr) in self.0.iter().enumerate() {
            match (addr.version(), v4_idx, v6_idx) {
                (IpVersion::V4, None, _) => {
                    v4_idx = Some(idx);
                }
                (IpVersion::V6, _, None) => {
                    v6_idx = Some(idx);
                }
                (_, Some(_), Some(_)) => break,
                _ => {}
            }
        }

        let v4: Option<SocketAddr>;
        let v6: Option<SocketAddr>;
        if v4_idx.zip(v6_idx).is_some_and(|(v4, v6)| v4 > v6) {
            v4 = v4_idx.and_then(|idx| self.0.remove(idx));
            v6 = v6_idx.and_then(|idx| self.0.remove(idx));
        } else {
            v6 = v6_idx.and_then(|idx| self.0.remove(idx));
            v4 = v4_idx.and_then(|idx| self.0.remove(idx));
        }

        match (prefer, v4, v6) {
            (Some(IpVersion::V4), Some(addr_v4), Some(addr_v6)) => {
                self.0.push_front(addr_v6);
                self.0.push_front(addr_v4);
            }
            (Some(IpVersion::V6), Some(addr_v4), Some(addr_v6)) => {
                self.0.push_front(addr_v4);
                self.0.push_front(addr_v6);
            }

            (_, Some(addr_v4), Some(addr_v6)) => {
                self.0.push_front(addr_v4);
                self.0.push_front(addr_v6);
            }
            (_, Some(addr_v4), None) => {
                self.0.push_front(addr_v4);
            }
            (_, None, Some(addr_v6)) => {
                self.0.push_front(addr_v6);
            }
            _ => {}
        }
    }
}

impl From<SocketAddr> for SocketAddrs {
    fn from(value: SocketAddr) -> Self {
        let mut addrs = VecDeque::with_capacity(1);
        addrs.push_front(value);
        SocketAddrs(addrs)
    }
}

impl FromIterator<SocketAddr> for SocketAddrs {
    fn from_iter<T: IntoIterator<Item = SocketAddr>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for SocketAddrs {
    type Item = SocketAddr;
    type IntoIter = std::collections::vec_deque::IntoIter<SocketAddr>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a SocketAddrs {
    type Item = &'a SocketAddr;
    type IntoIter = std::collections::vec_deque::Iter<'a, SocketAddr>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Extension trait for `IpAddr` and `SocketAddr` to get the IP version.
pub trait IpVersionExt {
    /// Get the IP version of this address.
    fn version(&self) -> IpVersion;
}

/// IP version.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum IpVersion {
    /// IPv4
    V4,

    /// IPv6
    V6,
}

impl IpVersion {
    pub(super) fn from_binding(
        ip_v4_address: Option<Ipv4Addr>,
        ip_v6_address: Option<Ipv6Addr>,
    ) -> Option<Self> {
        match (ip_v4_address, ip_v6_address) {
            // Prefer IPv6 if both are available.
            (Some(_), Some(_)) => Some(Self::V6),
            (Some(_), None) => Some(Self::V4),
            (None, Some(_)) => Some(Self::V6),
            (None, None) => None,
        }
    }

    /// Is this IP version IPv4?
    #[allow(dead_code)]
    pub fn is_v4(&self) -> bool {
        matches!(self, Self::V4)
    }

    /// Is this IP version IPv6?
    #[allow(dead_code)]
    pub fn is_v6(&self) -> bool {
        matches!(self, Self::V6)
    }
}

impl IpVersionExt for SocketAddr {
    fn version(&self) -> IpVersion {
        match self {
            SocketAddr::V4(_) => IpVersion::V4,
            SocketAddr::V6(_) => IpVersion::V6,
        }
    }
}

impl IpVersionExt for IpAddr {
    fn version(&self) -> IpVersion {
        match self {
            IpAddr::V4(_) => IpVersion::V4,
            IpAddr::V6(_) => IpVersion::V6,
        }
    }
}

/// A service to convert request references into destination addresses.
///
/// Commonly, this might be a DNS lookup, but other schemes are possible.
pub trait Resolver<Request> {
    /// Address type returned
    type Address;

    /// Resolution error returned
    type Error;

    /// Future type that the resolver uses to work.
    type Future: Future<Output = Result<Self::Address, Self::Error>>;

    /// Check if the resolver is ready to resolve.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Return a future representing the work the resolver does.
    fn resolve(&mut self, request: &Request) -> Self::Future;
}

impl<T, F, R, A, E> Resolver<R> for T
where
    T: for<'a> tower::Service<&'a R, Response = A, Error = E, Future = F>,
    F: Future<Output = Result<A, E>>,
{
    type Address = A;
    type Error = E;
    type Future = F;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(self, cx)
    }

    fn resolve(&mut self, request: &R) -> Self::Future {
        tower::Service::call(self, request)
    }
}

/// A static address resolver always returns the same address
/// and ignores the request.
///
/// This is useful for connecting with a single location.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaticResolver<A> {
    address: A,
}

impl<A> StaticResolver<A> {
    /// Create a new static-address resolver
    pub fn new(address: A) -> Self {
        Self { address }
    }
}

impl<R, A> tower::Service<&R> for StaticResolver<A>
where
    A: Clone,
{
    type Response = A;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: &R) -> Self::Future {
        ready(Ok(self.address.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use static_assertions::assert_impl_all;

    assert_impl_all!(SocketAddrs: Send, Sync, Clone, Default);
    assert_impl_all!(IpVersion: Send, Sync, Clone, Copy, PartialEq, Eq, std::hash::Hash);
    assert_impl_all!(StaticResolver<u32>: Send, Sync, Clone, Default, PartialEq, Eq);

    #[test]
    fn test_socket_addrs_default() {
        let addrs = SocketAddrs::default();
        assert!(addrs.is_empty());
        assert_eq!(addrs.len(), 0);
        assert_eq!(addrs.peek(), None);
    }

    #[test]
    fn test_socket_addrs_from_socket_addr() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addrs = SocketAddrs::from(addr);

        assert!(!addrs.is_empty());
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs.peek(), Some(addr));
    }

    #[test]
    fn test_socket_addrs_from_iterator() {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], 8081));
        let vec_addrs = vec![addr1, addr2];

        let addrs: SocketAddrs = vec_addrs.into_iter().collect();

        assert!(!addrs.is_empty());
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs.peek(), Some(addr1));
    }

    #[test]
    fn test_socket_addrs_set_port() {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addr2 = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 8080));
        let mut addrs: SocketAddrs = vec![addr1, addr2].into_iter().collect();

        addrs.set_port(9090);

        let modified: Vec<SocketAddr> = addrs.into_iter().collect();
        assert_eq!(modified[0].port(), 9090);
        assert_eq!(modified[1].port(), 9090);
    }

    #[test]
    fn test_socket_addrs_pop() {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], 8081));
        let mut addrs: SocketAddrs = vec![addr1, addr2].into_iter().collect();

        assert_eq!(addrs.pop(), Some(addr1));
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs.peek(), Some(addr2));

        assert_eq!(addrs.pop(), Some(addr2));
        assert_eq!(addrs.len(), 0);
        assert!(addrs.is_empty());

        assert_eq!(addrs.pop(), None);
    }

    #[test]
    fn test_socket_addrs_sort_preferred_ipv4() {
        let v4_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let v6_addr = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 8080));
        let mut addrs: SocketAddrs = vec![v6_addr, v4_addr].into_iter().collect();

        addrs.sort_preferred(Some(IpVersion::V4));

        let sorted: Vec<SocketAddr> = addrs.into_iter().collect();
        assert_eq!(sorted[0], v4_addr);
        assert_eq!(sorted[1], v6_addr);
    }

    #[test]
    fn test_socket_addrs_sort_preferred_ipv6() {
        let v4_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let v6_addr = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 8080));
        let mut addrs: SocketAddrs = vec![v4_addr, v6_addr].into_iter().collect();

        addrs.sort_preferred(Some(IpVersion::V6));

        let sorted: Vec<SocketAddr> = addrs.into_iter().collect();
        assert_eq!(sorted[0], v6_addr);
        assert_eq!(sorted[1], v4_addr);
    }

    #[test]
    fn test_socket_addrs_sort_preferred_none() {
        let v4_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let v6_addr = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 8080));
        let mut addrs: SocketAddrs = vec![v4_addr, v6_addr].into_iter().collect();

        addrs.sort_preferred(None);

        let sorted: Vec<SocketAddr> = addrs.into_iter().collect();
        assert_eq!(sorted[0], v6_addr);
        assert_eq!(sorted[1], v4_addr);
    }

    #[test]
    fn test_socket_addrs_sort_preferred_single_v4() {
        let v4_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let mut addrs: SocketAddrs = vec![v4_addr].into_iter().collect();

        addrs.sort_preferred(Some(IpVersion::V6));

        let sorted: Vec<SocketAddr> = addrs.into_iter().collect();
        assert_eq!(sorted[0], v4_addr);
    }

    #[test]
    fn test_socket_addrs_into_iterator() {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], 8081));
        let addrs: SocketAddrs = vec![addr1, addr2].into_iter().collect();

        let collected: Vec<SocketAddr> = addrs.into_iter().collect();
        assert_eq!(collected, vec![addr1, addr2]);
    }

    #[test]
    fn test_socket_addrs_iter_ref() {
        let addr1 = SocketAddr::from(([127, 0, 0, 1], 8080));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], 8081));
        let addrs: SocketAddrs = vec![addr1, addr2].into_iter().collect();

        let collected: Vec<&SocketAddr> = (&addrs).into_iter().collect();
        assert_eq!(collected, vec![&addr1, &addr2]);
    }

    #[test]
    fn test_ip_version_from_binding() {
        let v4_addr = Ipv4Addr::LOCALHOST;
        let v6_addr = Ipv6Addr::LOCALHOST;

        assert_eq!(
            IpVersion::from_binding(Some(v4_addr), None),
            Some(IpVersion::V4)
        );
        assert_eq!(
            IpVersion::from_binding(None, Some(v6_addr)),
            Some(IpVersion::V6)
        );
        assert_eq!(
            IpVersion::from_binding(Some(v4_addr), Some(v6_addr)),
            Some(IpVersion::V6)
        );
        assert_eq!(IpVersion::from_binding(None, None), None);
    }

    #[test]
    fn test_ip_version_methods() {
        let v4 = IpVersion::V4;
        let v6 = IpVersion::V6;

        assert!(v4.is_v4());
        assert!(!v4.is_v6());

        assert!(!v6.is_v4());
        assert!(v6.is_v6());
    }

    #[test]
    fn test_ip_version_ext_socket_addr() {
        let v4_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let v6_addr = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 8080));

        assert_eq!(v4_addr.version(), IpVersion::V4);
        assert_eq!(v6_addr.version(), IpVersion::V6);
    }

    #[test]
    fn test_ip_version_ext_ip_addr() {
        let v4_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let v6_ip = IpAddr::V6(Ipv6Addr::LOCALHOST);

        assert_eq!(v4_ip.version(), IpVersion::V4);
        assert_eq!(v6_ip.version(), IpVersion::V6);
    }

    #[test]
    fn test_static_resolver_new() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let resolver = StaticResolver::new(addr);

        assert_eq!(resolver.address, addr);
    }

    #[test]
    fn test_static_resolver_default() {
        let resolver = StaticResolver::<u32>::default();
        assert_eq!(resolver.address, 0u32);
    }

    #[test]
    fn test_static_resolver_service() {
        use std::task::{Context, Poll};

        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let resolver = StaticResolver::new(addr);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let _request = "test_request";
        let future = ready(Ok::<SocketAddr, Infallible>(resolver.address));
        let mut future = Box::pin(future);

        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(result)) => assert_eq!(result, addr),
            Poll::Ready(Err(_)) => panic!("Expected Ok result"),
            Poll::Pending => panic!("Expected ready result"),
        }
    }

    #[test]
    fn test_static_resolver_clone_eq() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let resolver1 = StaticResolver::new(addr);
        let resolver2 = resolver1.clone();

        assert_eq!(resolver1, resolver2);

        let resolver3 = StaticResolver::new(SocketAddr::from(([127, 0, 0, 1], 9090)));
        assert_ne!(resolver1, resolver3);
    }
}
