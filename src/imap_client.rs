use crate::config::ImapConfig;
use anyhow::{self as ah, Context as _};
use async_imap::{
    Client, Session,
    types::{Capability, NameAttribute},
};
use chrono::{TimeDelta, Utc};
use itertools::sorted_unstable;
use log::{debug, error, warn};
use rustls_native_certs::load_native_certs;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
};
use tokio_stream::StreamExt as _;

type TlsSession = Session<TlsStream<TcpStream>>;

pub struct ImapClient {
    session: Option<TlsSession>,
}

/// A fetched message with its UID, raw RFC822 bytes, and server receive time.
pub struct FetchedMessage {
    /// The IMAP UID (unique identifier) of the message.
    pub uid: u32,
    /// The raw RFC822 message bytes as returned by the server.
    pub raw: Vec<u8>,
    /// IMAP INTERNALDATE as a Unix timestamp (seconds since epoch).
    pub received_at: u64,
}

/// An IMAP mailbox discovered via LIST.
pub struct MailboxInfo {
    /// Full mailbox name as reported by the server (e.g. `INBOX/Sent`).
    pub name: String,
    /// Hierarchy delimiter used by the server (e.g. `/` or `.`).
    pub delimiter: char,
}

impl ImapClient {
    pub async fn connect(config: &ImapConfig) -> ah::Result<Self> {
        let tcp = TcpStream::connect((config.host(), config.port()))
            .await
            .with_context(|| format!("Failed to connect to {}:{}", config.host(), config.port()))?;
        let certs = load_native_certs();
        if !certs.errors.is_empty() {
            warn!(
                "Some native CA certificates could not be loaded: {:?}",
                certs.errors
            );
        }
        let mut root_store = RootCertStore::empty();
        for cert in certs.certs {
            if let Err(e) = root_store.add(cert) {
                warn!("Failed to add native root certificate: {e}");
            }
        }
        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(tls_config));
        let server_name = ServerName::try_from(config.host())
            .with_context(|| format!("Invalid IMAP server hostname '{}'", config.host()))?
            .to_owned();
        let tls = connector
            .connect(server_name, tcp)
            .await
            .with_context(|| format!("TLS handshake failed for {}", config.host()))?;
        let client = Client::new(tls);
        let session = client
            .login(config.username(), config.password())
            .await
            .map_err(|(err, _client)| err)
            .with_context(|| "IMAP login failed")?;
        Ok(Self {
            session: Some(session),
        })
    }

    /// Return all selectable mailboxes on the server via IMAP LIST.
    pub async fn list_mailboxes(&mut self) -> ah::Result<Vec<MailboxInfo>> {
        let session = self.session.as_mut().context("Not connected")?;
        let mut stream = session
            .list(Some(""), Some("*"))
            .await
            .context("IMAP LIST failed")?;
        let mut mailboxes = Vec::with_capacity(64);
        while let Some(name) = stream.next().await.transpose()? {
            // Skip containers that cannot be selected.
            if name.attributes().contains(&NameAttribute::NoSelect) {
                continue;
            }
            let delimiter = name
                .delimiter()
                .and_then(|d| d.chars().next())
                .unwrap_or('/');
            mailboxes.push(MailboxInfo {
                name: name.name().to_owned(),
                delimiter,
            });
        }
        Ok(mailboxes)
    }

    pub async fn select_mailbox(&mut self, mailbox: &str) -> ah::Result<()> {
        let session = self.session.as_mut().context("Not connected")?;
        session
            .select(mailbox)
            .await
            .with_context(|| format!("Failed to select mailbox '{mailbox}'"))?;
        Ok(())
    }

    /// Return UIDs of messages that are Seen (read) and older than `min_age_days`.
    pub async fn fetch_archivable_uids(&mut self, min_age_days: u32) -> ah::Result<Vec<u32>> {
        let session = self.session.as_mut().context("Not connected")?;

        let delta = TimeDelta::try_days(i64::from(min_age_days)).with_context(|| {
            format!("min_age_days={min_age_days} is too large to represent as a duration")
        })?;
        // Calculate the cutoff date.
        // Add 1 day to be inclusive of messages that are exactly min_age_days old.
        let cutoff = (Utc::now() - delta) + TimeDelta::days(1);
        // IMAP date format: DD-Mon-YYYY
        let cutoff_date = cutoff.format("%d-%b-%Y");

        // Search for read messages older than the cutoff
        let search_criteria = format!("SEEN BEFORE {cutoff_date}");
        debug!("IMAP SEARCH: {search_criteria}");

        let uid_set = session
            .uid_search(&search_criteria)
            .await
            .with_context(|| "UID SEARCH failed")?;

        Ok(sorted_unstable(uid_set).collect())
    }

    /// Fetch the raw RFC822 body for a batch of UIDs.
    pub async fn fetch_messages(&mut self, uids: &[u32]) -> ah::Result<Vec<FetchedMessage>> {
        if uids.is_empty() {
            return Ok(vec![]);
        }

        let session = self.session.as_mut().context("Not connected")?;

        let uid_list = uids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");

        let mut messages = Vec::with_capacity(uids.len());
        let mut stream = session
            .uid_fetch(&uid_list, "(RFC822 INTERNALDATE)")
            .await
            .with_context(|| "UID FETCH failed")?;

        while let Some(fetch) = stream.next().await.transpose()? {
            let Some(uid) = fetch.uid else {
                warn!("Skipping message with no UID in fetch response");
                continue;
            };
            let Some(b) = fetch.body() else {
                warn!("Skipping UID {uid}: empty body in fetch response");
                continue;
            };
            let raw = b.to_vec();
            let received_at = fetch.internal_date().map_or_else(
                || {
                    warn!("UID {uid}: INTERNALDATE missing, using current time");
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                },
                |dt| u64::try_from(dt.timestamp()).unwrap_or(0),
            );
            messages.push(FetchedMessage {
                uid,
                raw,
                received_at,
            });
        }

        Ok(messages)
    }

    /// Mark all `uids` as `\Deleted` in one `UID STORE` command, then `EXPUNGE`.
    pub async fn delete_messages(&mut self, uids: &[u32]) -> ah::Result<()> {
        if uids.is_empty() {
            return Ok(());
        }

        let session = self.session.as_mut().context("Not connected")?;

        let uid_set = uids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");

        {
            let mut store_stream = session
                .uid_store(&uid_set, "+FLAGS (\\Deleted)")
                .await
                .with_context(|| {
                    format!("Failed to mark {n} UID(s) as \\Deleted", n = uids.len())
                })?;
            while let Some(r) = store_stream.next().await {
                r?;
            }
        }

        {
            let caps = session.capabilities().await.context("CAPABILITY failed")?;
            if caps.has(&Capability::Atom("UIDPLUS".into())) {
                let mut expunge_stream = Box::pin(
                    session
                        .uid_expunge(&uid_set)
                        .await
                        .context("UID EXPUNGE failed")?,
                );
                while let Some(r) = expunge_stream.next().await {
                    r?;
                }
            } else {
                warn!(
                    "Server does not advertise UIDPLUS; skipping expunge to avoid data loss. \
                     Messages have been marked \\Deleted but not removed."
                );
            }
        }

        debug!("Deleted {} UID(s) from server", uids.len());
        Ok(())
    }

    pub async fn logout(&mut self) -> ah::Result<()> {
        if let Some(mut session) = self.session.take() {
            session.logout().await.context("IMAP logout failed")?;
        }
        Ok(())
    }
}

impl Drop for ImapClient {
    fn drop(&mut self) {
        if let Some(mut session) = self.session.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| {
                    handle.block_on(async move {
                        if let Err(e) = session.logout().await {
                            error!("IMAP logout during drop failed: {e}");
                        } else {
                            warn!("IMAP session logged out during drop");
                        }
                    });
                });
            } else {
                error!("IMAP session dropped outside of async runtime; LOGOUT not sent");
            }
        }
    }
}
