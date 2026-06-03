use anyhow::{self as ah, Context as _};
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    imap: ImapConfig,
    archive: ArchiveConfig,
}

impl Config {
    pub fn imap(&self) -> &ImapConfig {
        &self.imap
    }

    pub fn archive(&self) -> &ArchiveConfig {
        &self.archive
    }
}

#[derive(Debug, Deserialize)]
pub struct ImapConfig {
    host: String,
    port: Option<u16>,
    username: String,
    password: String,
}

impl ImapConfig {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(993)
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }
}

/// Local mailbox storage format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MailboxFormat {
    /// Standard Maildir: nested subdirectory per mailbox, messages in `cur/`.
    #[default]
    Maildir,
    /// Maildir++: flat dot-prefixed directories, messages in `cur/`.
    #[serde(rename = "maildir++")]
    MaildirPP,
    /// MH format (Claws-Mail native): numbered message files, `.mh_sequences`.
    Mh,
}

impl std::fmt::Display for MailboxFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailboxFormat::Maildir => write!(f, "maildir"),
            MailboxFormat::MaildirPP => write!(f, "maildir++"),
            MailboxFormat::Mh => write!(f, "mh"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ArchiveConfig {
    directory: PathBuf,
    /// Minimum age in days before a message is archived.
    min_age_days: Option<u32>,
    /// Local mailbox storage format.
    #[serde(default)]
    pub format: MailboxFormat,
    /// Mailboxes to skip entirely.
    #[serde(default)]
    skip_mailboxes: Vec<String>,
    /// Mailbox rename mappings.
    #[serde(default)]
    rename_mailboxes: HashMap<String, String>,
}

impl ArchiveConfig {
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn min_age_days(&self) -> u32 {
        self.min_age_days.unwrap_or(30)
    }

    pub fn format(&self) -> MailboxFormat {
        self.format
    }

    pub fn skip_mailboxes(&self) -> &[String] {
        &self.skip_mailboxes
    }

    pub fn rename_mailboxes(&self) -> &HashMap<String, String> {
        &self.rename_mailboxes
    }
}

impl Config {
    pub async fn load(path: &Path) -> ah::Result<Self> {
        let content = fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        Ok(config)
    }
}
