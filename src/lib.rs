//! Client and server primitives for tower-style services.

use std::{fmt, pin::Pin};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "codec")]
pub mod framed;
#[cfg(feature = "happy_eyeballs")]
pub(crate) mod happy_eyeballs;
pub mod info;
#[cfg(feature = "server")]
mod notify;
pub mod rt;
#[cfg(feature = "server")]
pub mod server;
pub mod services;
pub mod stream;

#[cfg(all(
    feature = "tls",
    not(any(feature = "tls-ring", feature = "tls-aws-lc"))
))]
compile_error!(
    "The 'tls' feature requires a backend, enable 'tls-ring' or 'tls-aws-lc' to select a backend"
);

#[allow(dead_code)]
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[allow(dead_code)]
type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[allow(unused)]
/// Utility struct for formatting a `Display` type in a `Debug` context.
pub(crate) struct DebugLiteral<T: fmt::Display>(T);

impl<T: fmt::Display> fmt::Debug for DebugLiteral<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Utility function for attaching a `follows_from` relationship to the current span
/// in a polling context.
#[allow(unused)]
#[track_caller]
pub(crate) fn polled_span(span: &tracing::Span) {
    tracing::dispatcher::get_default(|dispatch| {
        let id = span.id().expect("Missing ID; this is a bug");
        if let Some(current) = dispatch.current_span().id() {
            dispatch.record_follows_from(&id, current)
        }
    });
}

pub(crate) mod private {

    #[allow(unused)]
    pub trait Sealed<T> {}
}

/// Test fixtures
#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod fixtures {

    use std::sync::Once;

    #[cfg(feature = "tls")]
    use rustls::ServerConfig;

    /// Registers a global default tracing subscriber when called for the first time. This is intended
    /// for use in tests.
    pub fn subscribe() {
        static INSTALL_TRACING_SUBSCRIBER: Once = Once::new();
        INSTALL_TRACING_SUBSCRIBER.call_once(|| {
            let subscriber = tracing_subscriber::FmtSubscriber::builder()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_test_writer()
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        });
    }

    #[cfg(feature = "tls")]
    pub(crate) fn tls_server_config() -> rustls::ServerConfig {
        let (_, cert) = pem_rfc7468::decode_vec(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/minica/example.com/cert.pem"
        )))
        .unwrap();
        let (label, key) = pem_rfc7468::decode_vec(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/minica/example.com/key.pem"
        )))
        .unwrap();

        let cert = rustls::pki_types::CertificateDer::from(cert);
        let key = match label {
            "PRIVATE KEY" => rustls::pki_types::PrivateKeyDer::Pkcs8(key.into()),
            "RSA PRIVATE KEY" => rustls::pki_types::PrivateKeyDer::Pkcs1(key.into()),
            "EC PRIVATE KEY" => rustls::pki_types::PrivateKeyDer::Sec1(key.into()),
            _ => panic!("unknown key type"),
        };

        let mut cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();

        cfg.alpn_protocols.push(b"h2".to_vec());
        cfg.alpn_protocols.push(b"http/1.1".to_vec());

        cfg
    }

    #[cfg(feature = "tls")]
    fn tls_root_store() -> rustls::RootCertStore {
        let mut root_store = rustls::RootCertStore::empty();
        let (_, cert) = pem_rfc7468::decode_vec(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/minica/minica.pem"
        )))
        .unwrap();
        root_store
            .add(rustls::pki_types::CertificateDer::from(cert))
            .unwrap();
        root_store
    }

    #[cfg(feature = "tls")]
    pub(crate) fn tls_client_config() -> rustls::ClientConfig {
        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(tls_root_store())
            .with_no_client_auth();
        config.alpn_protocols.push(b"h2".to_vec());
        config.alpn_protocols.push(b"http/1.1".to_vec());
        config
    }

    #[cfg(feature = "tls")]
    pub(crate) fn tls_install_default() {
        #[cfg(feature = "tls-ring")]
        {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }

        #[cfg(all(feature = "tls-aws-lc", not(feature = "tls-ring")))]
        {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        }

        #[cfg(not(any(feature = "tls-aws-lc", feature = "tls-ring")))]
        {
            panic!("No TLS backend enabled, please enable one of `tls-ring` or `tls-aws-lc`");
        }
    }
}
