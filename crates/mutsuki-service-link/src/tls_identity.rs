//! Load Quinn TLS identity from PEM materials supplied by the caller.
//!
//! Product configs only store Host secret key references; this module never
//! reads files or invents a default trust policy.

use std::sync::Arc;

use quinn::{ClientConfig, ServerConfig};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

/// Structured failure when PEM identity material cannot become a Quinn config.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TlsIdentityError {
    #[error("tls identity cert PEM is empty")]
    EmptyCertPem,
    #[error("tls identity key PEM is empty")]
    EmptyKeyPem,
    #[error("tls identity CA PEM is empty")]
    EmptyCaPem,
    #[error("failed to parse TLS certificate PEM: {0}")]
    InvalidCertPem(String),
    #[error("failed to parse TLS private key PEM: {0}")]
    InvalidKeyPem(String),
    #[error("failed to parse TLS CA certificate PEM: {0}")]
    InvalidCaPem(String),
    #[error("TLS certificate PEM contained no certificates")]
    MissingCertificate,
    #[error("failed to build QUIC server TLS config: {0}")]
    ServerConfig(String),
    #[error("failed to build QUIC client TLS config: {0}")]
    ClientConfig(String),
}

/// Build a Quinn server config from PEM-encoded certificate chain + private key.
pub fn server_config_from_pem(
    cert_pem: &str,
    key_pem: &str,
) -> Result<ServerConfig, TlsIdentityError> {
    if cert_pem.trim().is_empty() {
        return Err(TlsIdentityError::EmptyCertPem);
    }
    if key_pem.trim().is_empty() {
        return Err(TlsIdentityError::EmptyKeyPem);
    }
    let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TlsIdentityError::InvalidCertPem(error.to_string()))?;
    if certs.is_empty() {
        return Err(TlsIdentityError::MissingCertificate);
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|error| TlsIdentityError::InvalidKeyPem(error.to_string()))?;
    ServerConfig::with_single_cert(certs, key)
        .map_err(|error| TlsIdentityError::ServerConfig(error.to_string()))
}

/// Build a Quinn client config that trusts the provided PEM CA / server certificate(s).
pub fn client_config_from_ca_pem(ca_pem: &str) -> Result<ClientConfig, TlsIdentityError> {
    if ca_pem.trim().is_empty() {
        return Err(TlsIdentityError::EmptyCaPem);
    }
    let certs = CertificateDer::pem_slice_iter(ca_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TlsIdentityError::InvalidCaPem(error.to_string()))?;
    if certs.is_empty() {
        return Err(TlsIdentityError::MissingCertificate);
    }
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .map_err(|error| TlsIdentityError::InvalidCaPem(error.to_string()))?;
    }
    ClientConfig::with_root_certificates(Arc::new(roots))
        .map_err(|error| TlsIdentityError::ClientConfig(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_pem() -> (String, String) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        (generated.cert.pem(), generated.key_pair.serialize_pem())
    }

    #[test]
    fn server_and_client_configs_roundtrip_from_pem() {
        let (cert_pem, key_pem) = fixture_pem();
        assert!(server_config_from_pem(&cert_pem, &key_pem).is_ok());
        assert!(client_config_from_ca_pem(&cert_pem).is_ok());
    }

    #[test]
    fn empty_or_invalid_pem_fails_loud() {
        assert_eq!(
            server_config_from_pem("", "x").unwrap_err(),
            TlsIdentityError::EmptyCertPem
        );
        assert_eq!(
            server_config_from_pem("x", "").unwrap_err(),
            TlsIdentityError::EmptyKeyPem
        );
        assert_eq!(
            client_config_from_ca_pem("").unwrap_err(),
            TlsIdentityError::EmptyCaPem
        );
        assert!(matches!(
            server_config_from_pem("not-a-pem", "also-not").unwrap_err(),
            TlsIdentityError::InvalidCertPem(_) | TlsIdentityError::MissingCertificate
        ));
        assert!(matches!(
            client_config_from_ca_pem("not-a-pem").unwrap_err(),
            TlsIdentityError::InvalidCaPem(_) | TlsIdentityError::MissingCertificate
        ));
    }
}
