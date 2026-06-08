use crate::config::{Config, MailboxFormat};
use anyhow::{self as ah, Context as _, format_err as err};
use log::{debug, info, warn};
use redb::{
    Database, MultimapTableDefinition, ReadableDatabase as _, ReadableTable as _,
    ReadableTableMetadata as _, TableDefinition, TableError,
};
use sha3::{Digest as _, Sha3_224};
use std::{
    collections::HashSet,
    fmt::Write as _,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs,
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    task::spawn_blocking,
};

/// Message hash digest length in bytes.
const HASH_LEN: usize = 28;

/// Raw message hash digest.
type MsgHash = [u8; HASH_LEN];

/// Compute the SHA3-224 hash of a raw message.
fn compute_msg_hash(raw: &[u8]) -> MsgHash {
    let digest = Sha3_224::digest(raw);
    digest
        .as_slice()
        .try_into()
        .expect("Message hash digest has incorrect length")
}

/// Format a raw hash as a lowercase hex string.
fn hash_to_hex(hash: &MsgHash) -> String {
    hash.iter()
        .fold(String::with_capacity(HASH_LEN * 2), |mut s, b| {
            write!(s, "{b:02x}").expect("Write to String");
            s
        })
}

/// Per-mailbox message hash cache for MH-format deduplication.
///
/// Table `attr`:
/// `key (str)` -> `value (str)`
///
/// Table `id2h`:
/// `msgnum (u64)` -> `hash (28-byte SHA3-224)`
///
/// Table `h2id`:
/// `hash (28-byte SHA3-224)` -> `msgnum (u64)`
pub struct HashCache {
    db: Arc<Database>,
}

impl HashCache {
    const FILE_NAME: &str = ".mailarchdb";
    const VERSION: &str = "1";
    const TABLE_ATTR: TableDefinition<'_, &str, &str> = TableDefinition::new("attr");
    const TABLE_ID2H: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("id2h");
    const TABLE_H2ID: MultimapTableDefinition<'_, &[u8], u64> =
        MultimapTableDefinition::new("h2id");

    /// Load the cache, and sync with the directory if loading succeeded.
    ///
    /// If loading or syncing fails, delete the cache and rebuild from directory contents.
    pub async fn load_and_sync_lossy(base: &Path) -> ah::Result<Self> {
        match Self::load(base).await {
            Ok(mut cache) => {
                if let Err(e) = cache.sync_with_dir(base).await {
                    warn!(
                        "Failed to sync MH hash cache with directory ({}): {e:#}",
                        base.display()
                    );
                } else {
                    return Ok(cache);
                }
            }
            Err(e) => {
                warn!("Failed to load MH hash cache for {}: {e:#}", base.display());
            }
        }

        // Loading or syncing the cache failed, delete it and rebuild from directory contents.
        warn!(
            "Rebuilding MH hash cache for {} from directory contents",
            base.display()
        );
        fs::remove_file(base.join(Self::FILE_NAME))
            .await
            .context("Failed to remove corrupted MH hash cache")?;
        let mut cache = Self::load(base)
            .await
            .context("Failed to load MH hash cache after removing corrupted file")?;
        cache
            .sync_with_dir(base)
            .await
            .context("Failed to sync MH hash cache with directory")?;
        Ok(cache)
    }

    /// Open or create the database.
    pub async fn load(base: &Path) -> ah::Result<Self> {
        let db_path = base.join(Self::FILE_NAME);
        let db = spawn_blocking(move || -> ah::Result<Database> {
            let db = Database::create(&db_path).context("Cannot open hash DB")?;

            let txn = db.begin_write().context("Cannot begin write transaction")?;
            let do_init: bool;
            {
                let mut table_attr = txn
                    .open_table(Self::TABLE_ATTR)
                    .context("Cannot open attr table")?;
                let table_id2h = txn
                    .open_table(Self::TABLE_ID2H)
                    .context("Cannot open id2h table")?;
                let table_h2id = txn
                    .open_multimap_table(Self::TABLE_H2ID)
                    .context("Cannot open h2id table")?;

                // Check version, or initialise if not present.
                match table_attr.get("version") {
                    Ok(Some(v)) if v.value() != Self::VERSION => {
                        return Err(err!(
                            "Unsupported hash DB version {} (expected {})",
                            v.value(),
                            Self::VERSION
                        ));
                    }
                    Ok(Some(_)) => do_init = false,
                    Ok(None) => do_init = true,
                    Err(e) => return Err(e).context("Cannot query version in attr table"),
                }

                // Sanity check: id2h and h2id tables must have the same number of entries.
                if table_id2h.len().context("Cannot get id2h table length")?
                    != table_h2id.len().context("Cannot get h2id table length")?
                {
                    return Err(err!("id2h and h2id tables have different lengths"));
                }

                // Database is empty if version is not present.
                if do_init {
                    info!("Initialising new mailarch DB at {}", db_path.display());
                    table_attr
                        .insert("version", Self::VERSION)
                        .context("Cannot write version to attr table")?;
                }
            }
            if do_init {
                txn.commit().context("Cannot commit transaction")?;
            }
            Ok(db)
        })
        .await
        .context("spawn_blocking panicked")??;
        Ok(Self { db: Arc::new(db) })
    }

    /// Check whether `hash` is present.
    pub async fn contains_hash(&self, hash: &MsgHash) -> ah::Result<bool> {
        let db = Arc::clone(&self.db);
        let hash = *hash;
        spawn_blocking(move || -> ah::Result<bool> {
            let txn = db.begin_read().context("Cannot begin read transaction")?;
            let table = match txn.open_multimap_table(HashCache::TABLE_H2ID) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(e) => return Err(e).context("Cannot open h2id table"),
            };
            Ok(table
                .get(hash.as_slice())
                .context("Cannot query h2id table")?
                .next()
                .transpose()
                .context("Cannot read h2id value")?
                .is_some())
        })
        .await
        .context("spawn_blocking panicked")?
    }

    /// Insert `hash` for `msgnum`.
    pub async fn insert(&mut self, msgnum: u64, hash: MsgHash) -> ah::Result<()> {
        let db = Arc::clone(&self.db);
        spawn_blocking(move || -> ah::Result<()> {
            let txn = db.begin_write().context("Cannot begin write transaction")?;
            txn.open_table(HashCache::TABLE_ID2H)
                .context("Cannot open id2h table")?
                .insert(msgnum, hash.as_slice())
                .context("Cannot insert id2h entry")?;
            txn.open_multimap_table(HashCache::TABLE_H2ID)
                .context("Cannot open h2id table")?
                .insert(hash.as_slice(), msgnum)
                .context("Cannot insert h2id entry")?;
            txn.commit().context("Cannot commit transaction")?;
            Ok(())
        })
        .await
        .context("spawn_blocking panicked")?
    }

    /// Number of active entries.
    pub async fn len(&self) -> usize {
        let db = Arc::clone(&self.db);
        spawn_blocking(move || -> ah::Result<usize> {
            let txn = db.begin_read().context("Cannot begin read transaction")?;
            let table = match txn.open_table(HashCache::TABLE_ID2H) {
                Ok(t) => t,
                Err(TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(e) => return Err(e).context("Cannot open id2h table"),
            };
            Ok(
                usize::try_from(table.len().context("Cannot get table length")?)
                    .unwrap_or(usize::MAX),
            )
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(0)
    }

    /// Sync cache with directory `base`:
    /// Hash uncached files, drop deleted entries.
    pub async fn sync_with_dir(&mut self, base: &Path) -> ah::Result<()> {
        // Enumerate numeric filenames present on disk.
        let mut mailbox_msgnums: HashSet<u64> = HashSet::new();
        {
            let mut rd = fs::read_dir(base)
                .await
                .with_context(|| format!("Cannot read {}", base.display()))?;
            while let Some(e) = rd.next_entry().await? {
                if let Some(n) = e.file_name().to_str()
                    && let Ok(n) = n.parse::<u64>()
                {
                    mailbox_msgnums.insert(n);
                }
            }
        }

        // Read all cached msgnums from the database.
        let cached_msgnums: HashSet<u64> = spawn_blocking({
            let db = Arc::clone(&self.db);
            move || {
                let txn = db.begin_read().context("Cannot begin read transaction")?;
                let table = match txn.open_table(HashCache::TABLE_ID2H) {
                    Ok(t) => t,
                    Err(TableError::TableDoesNotExist(_)) => return Ok(HashSet::new()),
                    Err(e) => return Err(e).context("Cannot open id2h table"),
                };
                let len = usize::try_from(table.len().context("Cannot get table length")?)
                    .unwrap_or(usize::MAX);
                let mut msgnums = HashSet::with_capacity(len);
                for e in table.iter().context("Cannot iterate id2h table")? {
                    let (k, _) = e.context("Cannot read id2h entry")?;
                    msgnums.insert(k.value());
                }
                Ok(msgnums)
            }
        })
        .await
        .context("spawn_blocking panicked")??;

        // Delete stale entries (files removed from mailbox).
        let stale_msgnums: Vec<u64> = cached_msgnums
            .iter()
            .copied()
            .filter(|n| !mailbox_msgnums.contains(n))
            .collect();
        if !stale_msgnums.is_empty() {
            spawn_blocking({
                let db = Arc::clone(&self.db);
                move || -> ah::Result<()> {
                    let txn = db.begin_write().context("Cannot begin write transaction")?;
                    {
                        let mut table_id2h = txn
                            .open_table(HashCache::TABLE_ID2H)
                            .context("Cannot open id2h table")?;
                        let mut table_h2id = txn
                            .open_multimap_table(HashCache::TABLE_H2ID)
                            .context("Cannot open h2id table")?;
                        for msgnum in stale_msgnums {
                            let hash = table_id2h
                                .get(msgnum)
                                .context("Cannot query id2h table")?
                                .context("Stale msgnum not found in id2h table")?
                                .value()
                                .to_owned();
                            table_id2h
                                .remove(msgnum)
                                .context("Cannot remove id2h entry")?;
                            table_h2id
                                .remove(hash.as_slice(), msgnum)
                                .context("Cannot remove h2id entry")?;
                        }
                    }
                    txn.commit().context("Cannot commit transaction")?;
                    Ok(())
                }
            })
            .await
            .context("spawn_blocking panicked")??;
        }

        // Hash files not yet present in the cache.
        let mut new_hashes = Vec::with_capacity(mailbox_msgnums.len());
        for &n in &mailbox_msgnums {
            if cached_msgnums.contains(&n) {
                continue;
            }
            let path = base.join(n.to_string());
            match fs::read(&path).await {
                Ok(msg) => new_hashes.push((n, compute_msg_hash(&msg))),
                Err(e) => warn!("Cannot hash {}: {e:#}", path.display()),
            }
        }
        spawn_blocking({
            let db = Arc::clone(&self.db);
            move || -> ah::Result<()> {
                let txn = db.begin_write().context("Cannot begin write transaction")?;
                {
                    let mut table_id2h = txn
                        .open_table(HashCache::TABLE_ID2H)
                        .context("Cannot open id2h table")?;
                    let mut table_h2id = txn
                        .open_multimap_table(HashCache::TABLE_H2ID)
                        .context("Cannot open h2id table")?;
                    for (msgnum, hash) in new_hashes {
                        table_id2h
                            .insert(msgnum, hash.as_slice())
                            .context("Cannot insert id2h entry")?;
                        table_h2id
                            .insert(hash.as_slice(), msgnum)
                            .context("Cannot insert h2id entry")?;
                    }
                }
                txn.commit().context("Cannot commit transaction")?;
                Ok(())
            }
        })
        .await
        .context("spawn_blocking panicked")?
    }
}

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
    mh_cache: Option<HashCache>,
    claws_mark: Option<ClawsMarkFile>,
}

impl Maildir {
    pub async fn open(base: &Path, format: MailboxFormat, dry_run: bool) -> ah::Result<Self> {
        let mut mh_cache: Option<HashCache> = None;
        info!("Opening local {format} mailbox at {}", base.display());
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
                        info!("Creating .mh_sequences file {}", seq_file.display());
                        fs::write(&seq_file, "")
                            .await
                            .with_context(|| format!("Failed to create {}", seq_file.display()))?;
                    }
                    // Delete the Claws-Mail cache.
                    let cache_file = base.join(".claws_cache");
                    if cache_file.exists() {
                        info!("Removing Claws-Mail cache {}", cache_file.display());
                        match fs::remove_file(&cache_file).await {
                            Ok(()) => {}
                            Err(e) if e.kind() == ErrorKind::NotFound => {}
                            Err(e) => {
                                warn!("Failed to remove {}: {e:#}", cache_file.display());
                            }
                        }
                    }
                    info!("Loading MH hash cache for {}", base.display());
                    let cache = HashCache::load_and_sync_lossy(base)
                        .await
                        .context("Failed to load and sync MH hash cache")?;
                    info!("MH hash cache has {} entries", cache.len().await);
                    mh_cache = Some(cache);
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
            mh_cache,
            claws_mark,
        })
    }

    /// Store a message in Maildir/Maildir++ format.
    async fn store_maildir(&self, raw: &[u8], received_at: u64) -> ah::Result<StoreResult> {
        let hash = hash_to_hex(&compute_msg_hash(raw));
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
        let hash = compute_msg_hash(raw);

        if let Some(ref mut cache) = self.mh_cache
            && cache.contains_hash(&hash).await?
        {
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

        if let Some(ref mut cache) = self.mh_cache
            && let Err(e) = cache.insert(seq, hash).await
        {
            warn!("Failed to update hash cache for MH #{seq}: {e:#}");
        }

        if let Some(ref mark) = self.claws_mark
            && let Err(e) = mark.mark_read(seq).await
        {
            warn!("Failed to update .claws_mark for MH #{seq}: {e:#}");
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
