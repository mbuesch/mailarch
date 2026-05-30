use crate::config::{Config, MailboxFormat};
use anyhow::{self as ah, Context as _, format_err as err};
use log::{debug, info};
use sha3::{Digest as _, Sha3_224};
use std::{
    collections::HashSet,
    fmt::Write as _,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::{
    fs,
    io::{AsyncReadExt as _, AsyncWriteExt as _},
};

/// Map an IMAP mailbox name to a local path under `archive_dir` for the given format.
///
/// - `Maildir` / `Mh`: Nested subdirectories -> `archive_dir/INBOX/Sent/`
/// - `MaildirPP`     : Flat dot-prefixed dir -> `archive_dir/.INBOX.Sent/`
///
/// Empty components, `.`, and `..` are dropped to prevent path traversal.
/// Any OS path separator embedded in a component is replaced with `_`.
pub fn mailbox_path(config: &Config, mailbox_name: &str, delimiter: char) -> PathBuf {
    let format = config.archive().format();
    let archive_dir = config.archive().directory();
    let rename_mailboxes = config.archive().rename_mailboxes();

    let mailbox_name = if let Some(rename) = rename_mailboxes.get(mailbox_name) {
        info!("Renaming mailbox {mailbox_name} -> {rename}");
        rename
    } else {
        mailbox_name
    };

    let components: Vec<String> = mailbox_name
        .split(delimiter)
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .map(|s| s.replace(['/', '\\'], "_"))
        .collect();

    match format {
        MailboxFormat::MaildirPP => {
            // Dots within component names must not conflict with the separator.
            let sanitised: Vec<String> = components
                .into_iter()
                .map(|s| s.replace('.', "_"))
                .collect();
            archive_dir.join(format!(".{}", sanitised.join(".")))
        }
        MailboxFormat::Maildir | MailboxFormat::Mh => {
            let rel: PathBuf = components.iter().collect();
            archive_dir.join(rel)
        }
    }
}

/// Append-only writer for Claws-Mail's `.claws_mark` file.
///
/// Binary layout (all fields little-endian u32):
/// ```text
/// header:  [VERSION]
/// records: [msgnum][perm_flags]  (one per message)
/// ```
pub struct ClawsMarkFile {
    path: PathBuf,
}

impl ClawsMarkFile {
    /// Mark-file format version.
    const VERSION: u32 = 2;

    /// Size of the file header in bytes.
    const HEADER_LEN: usize = 4;

    /// Size of one message record in bytes.
    const RECORD_LEN: usize = 8;

    /// Byte offset of the msgnum field within a record.
    const REC_OFF_MSGNUM: usize = 0;

    /// Byte offset of the `perm_flags` field within a record.
    const REC_OFF_FLAGS: usize = 4;

    // Message is new (never seen).
    const MSG_NEW_BIT: usize = 0;

    // Message is unread.
    const MSG_UNREAD_BIT: usize = 1;

    pub fn new(base: &Path) -> Self {
        Self {
            path: base.join(".claws_mark"),
        }
    }

    async fn verify_version(&self) -> ah::Result<()> {
        let mut f = fs::File::open(&self.path)
            .await
            .with_context(|| format!("Cannot open {} for version check", self.path.display()))?;
        let mut header = [0_u8; Self::HEADER_LEN];
        f.read_exact(&mut header).await.with_context(|| {
            format!(
                "{} is too short to contain a version header",
                self.path.display()
            )
        })?;
        let version = u32::from_le_bytes(header);
        if version != Self::VERSION {
            return Err(err!(
                "{}: unexpected mark-file version {} (expected {})",
                self.path.display(),
                version,
                Self::VERSION,
            ));
        }
        Ok(())
    }

    /// Append a "read" record for `msgnum`
    ///
    /// Creates the file with the version header if it does not yet exist.
    pub async fn mark_read(&self, msgnum: u64) -> ah::Result<()> {
        let msgnum = u32::try_from(msgnum).map_err(|_| err!("msgnum {msgnum} exceeds u32::MAX"))?;

        // Flags for an already-read, non-new message.
        // Neither FLAG_NEW nor FLAG_UNREAD
        let perm_flags: u32 = (0 << Self::MSG_NEW_BIT) | (0 << Self::MSG_UNREAD_BIT);

        let mut file = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .await
        {
            Ok(mut f) => {
                let mut header = [0_u8; Self::HEADER_LEN];
                header.copy_from_slice(&Self::VERSION.to_le_bytes());
                f.write_all(&header)
                    .await
                    .with_context(|| format!("Cannot write header to {}", self.path.display()))?;
                f
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                self.verify_version().await?;
                fs::OpenOptions::new()
                    .append(true)
                    .open(&self.path)
                    .await
                    .with_context(|| format!("Cannot open {}", self.path.display()))?
            }
            Err(e) => {
                return Err(e).with_context(|| format!("Cannot create {}", self.path.display()));
            }
        };

        let mut record = [0_u8; Self::RECORD_LEN];
        record[Self::REC_OFF_MSGNUM..Self::REC_OFF_MSGNUM + 4]
            .copy_from_slice(&msgnum.to_le_bytes());
        record[Self::REC_OFF_FLAGS..Self::REC_OFF_FLAGS + 4]
            .copy_from_slice(&perm_flags.to_le_bytes());
        file.write_all(&record)
            .await
            .with_context(|| format!("Cannot write mark record to {}", self.path.display()))?;

        Ok(())
    }
}

/// Unified mailbox writer supporting Maildir, Maildir++ and MH formats.
pub struct Maildir {
    base: PathBuf,
    format: MailboxFormat,
    dry_run: bool,
    mh_known: HashSet<String>,
    claws_mark: Option<ClawsMarkFile>,
}

impl Maildir {
    pub async fn open(base: &Path, format: MailboxFormat, dry_run: bool) -> ah::Result<Self> {
        let mut mh_known = HashSet::new();
        if !dry_run {
            match format {
                MailboxFormat::Maildir | MailboxFormat::MaildirPP => {
                    for subdir in &["cur", "new", "tmp"] {
                        let dir = base.join(subdir);
                        fs::create_dir_all(&dir).await.with_context(|| {
                            format!("Failed to create Maildir subdir {}", dir.display())
                        })?;
                    }
                }
                MailboxFormat::Mh => {
                    fs::create_dir_all(base).await.with_context(|| {
                        format!("Failed to create MH mailbox dir {}", base.display())
                    })?;
                    // Claws-Mail requires .mh_sequences in every MH folder.
                    let seq_file = base.join(".mh_sequences");
                    if !seq_file.exists() {
                        fs::write(&seq_file, "")
                            .await
                            .with_context(|| format!("Failed to create {}", seq_file.display()))?;
                    }
                    // Delete the Claws-Mail cache.
                    let cache_file = base.join(".claws_cache");
                    match fs::remove_file(&cache_file).await {
                        Ok(()) => {}
                        Err(e) if e.kind() == ErrorKind::NotFound => {}
                        Err(e) => {
                            log::warn!("Failed to remove {}: {e:#}", cache_file.display());
                        }
                    }
                    // Hash all existing messages to build the dedup set.
                    let mut rd = fs::read_dir(base)
                        .await
                        .with_context(|| format!("Cannot read {}", base.display()))?;
                    while let Some(entry) = rd.next_entry().await? {
                        // Only consider numeric filenames (MH messages).
                        if entry.file_name().to_string_lossy().parse::<u64>().is_ok() {
                            let raw = fs::read(entry.path()).await.with_context(|| {
                                format!("Cannot read {}", entry.path().display())
                            })?;
                            mh_known.insert(Self::message_hash(&raw));
                        }
                    }
                }
            }
        }
        let claws_mark = match format {
            MailboxFormat::Mh if !dry_run => Some(ClawsMarkFile::new(base)),
            _ => None,
        };
        Ok(Self {
            base: base.to_path_buf(),
            format,
            dry_run,
            mh_known,
            claws_mark,
        })
    }

    /// Hash a message for deduplication.
    fn message_hash(raw: &[u8]) -> String {
        let mut hasher = Sha3_224::new();
        hasher.update(raw);
        hasher
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut s, b| {
                write!(s, "{b:02x}").expect("Failed to write hash");
                s
            })
    }

    /// Store a message in Maildir/Maildir++ format.
    async fn store_maildir(&self, raw: &[u8], received_at: u64) -> ah::Result<StoreResult> {
        let hash = Self::message_hash(raw);
        let cur_path = self.base.join("cur");
        let new_path = self.base.join("new");

        for subdir in [&cur_path, &new_path] {
            let read_dir_result = fs::read_dir(subdir).await;
            let mut read_dir = match read_dir_result {
                Ok(read_dir) => read_dir,
                Err(e) if self.dry_run && e.kind() == ErrorKind::NotFound => {
                    continue;
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("Cannot read {}", subdir.display()));
                }
            };
            while let Some(entry) = read_dir.next_entry().await? {
                if entry.file_name().to_string_lossy().contains(&hash) {
                    return Ok(StoreResult::AlreadyExists { identical: true });
                }
            }
        }

        if self.dry_run {
            return Ok(StoreResult::Stored);
        }

        let filename = format!("{received_at}.{hash}:2,S");
        let cur_file_path = cur_path.join(&filename);
        let tmp_file = tempfile::Builder::new()
            .tempfile_in(self.base.join("tmp"))
            .context("Failed to create temp file in tmp/")?;
        fs::write(tmp_file.path(), raw)
            .await
            .with_context(|| format!("Failed to write to {}", tmp_file.path().display()))?;
        tmp_file
            .persist(&cur_file_path)
            .map_err(|e| e.error)
            .with_context(|| format!("Failed to move temp file -> {}", cur_file_path.display()))?;

        debug!("Stored message as {filename}");
        Ok(StoreResult::Stored)
    }

    /// Get the next MH sequence number.
    async fn mh_next_seq(base: &Path) -> ah::Result<u64> {
        let mut max = 0_u64;
        let mut rd = fs::read_dir(base)
            .await
            .with_context(|| format!("Cannot read {}", base.display()))?;
        while let Some(entry) = rd.next_entry().await? {
            if let Ok(n) = entry.file_name().to_string_lossy().parse::<u64>()
                && n > max
            {
                max = n;
            }
        }
        max.checked_add(1)
            .ok_or_else(|| err!("MH sequence number overflow"))
    }

    /// Store a message in MH format.
    async fn store_mh(&mut self, raw: &[u8]) -> ah::Result<StoreResult> {
        let hash = Self::message_hash(raw);

        if self.mh_known.contains(&hash) {
            return Ok(StoreResult::AlreadyExists { identical: true });
        }

        if self.dry_run {
            return Ok(StoreResult::Stored);
        }

        let seq = Self::mh_next_seq(&self.base).await?;
        let msg_path = self.base.join(seq.to_string());

        let tmp_file = tempfile::Builder::new()
            .tempfile_in(&self.base)
            .context("Failed to create temp file")?;
        fs::write(tmp_file.path(), raw)
            .await
            .with_context(|| format!("Failed to write to {}", tmp_file.path().display()))?;
        tmp_file
            .persist(&msg_path)
            .map_err(|e| e.error)
            .with_context(|| format!("Failed to rename temp file -> {}", msg_path.display()))?;

        self.mh_known.insert(hash);

        if let Some(ref mark) = self.claws_mark
            && let Err(e) = mark.mark_read(seq).await
        {
            log::warn!("Failed to update .claws_mark for MH #{seq}: {e:#}");
        }

        debug!("Stored message as MH #{seq}");
        Ok(StoreResult::Stored)
    }

    /// Store a message.
    pub async fn store(&mut self, raw: &[u8], received_at: u64) -> ah::Result<StoreResult> {
        match self.format {
            MailboxFormat::Maildir | MailboxFormat::MaildirPP => {
                self.store_maildir(raw, received_at).await
            }
            MailboxFormat::Mh => self.store_mh(raw).await,
        }
    }
}

#[derive(Debug)]
pub enum StoreResult {
    Stored,
    AlreadyExists { identical: bool },
}
