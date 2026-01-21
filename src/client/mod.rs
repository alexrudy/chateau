//! Client implementations

pub mod builder;
pub mod conn;
pub mod pool;

use tower::ServiceExt;

use crate::services::SharedService;

use self::builder::NeedsProtocol;
use self::builder::NeedsRequest;
use self::builder::NeedsTransport;
pub use self::pool::manager::ConnectionManagerConfig as PoolConfig;
pub use self::pool::service::ConnectionPoolLayer;
pub use self::pool::service::ConnectionPoolService;
pub use self::pool::service::{ConnectionManagerLayer, ConnectionManagerService};

#[cfg(feature = "tls")]
/// Get a default TLS client configuration by loading the platform's native certificates.
pub fn default_tls_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
        roots.add(cert).unwrap();
    }

    let mut cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    cfg.alpn_protocols.push(b"h2".to_vec());
    cfg.alpn_protocols.push(b"http/1.1".to_vec());
    cfg
}

/// Simple wrapper struct for clients
#[derive(Debug, Clone)]
pub struct Client<Req, Res, Err> {
    service: SharedService<Req, Res, Err>,
}

impl<Req, Res, Err> Client<Req, Res, Err> {
    /// Send a request
    pub async fn request(&self, request: Req) -> Result<Res, Err> {
        self.service.clone().oneshot(request).await
    }

    /// Create a new client builder
    pub fn builder() -> builder::ClientBuilder<NeedsTransport, NeedsProtocol, NeedsRequest> {
        builder::ClientBuilder::new()
    }
}
