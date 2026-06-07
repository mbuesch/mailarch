use crate::Mode;
use anyhow::{self as ah, Context as _, format_err as err};
use nix::{
    errno::Errno,
    fcntl::{Flock, FlockArg},
};
use std::{path::Path, time::Duration};
use tokio::{
    fs::{self, OpenOptions},
    sync,
    time::{sleep, timeout},
};

const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// RAII exclusive advisory lock on a file.
/// Automatically released on drop.
pub struct Lock {
    _flock: Option<Flock<std::fs::File>>,
}

impl Lock {
    async fn acquire(
        path: &Path,
        mode: Mode,
        interrupt_rx: &mut sync::mpsc::Receiver<ah::Error>,
    ) -> ah::Result<Self> {
        let flock = if mode.is_dry_run() {
            None
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .with_context(|| err!("Lock dir create failed: {}", parent.display()))?;
            }

            timeout(LOCK_TIMEOUT, async move {
                let Ok(file) = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(path)
                    .await
                else {
                    return Err(err!("Cannot open lock file {}", path.display()));
                };
                let mut file = file.into_std().await;
                loop {
                    if let Ok(e) = interrupt_rx.try_recv() {
                        break Err(e);
                    }
                    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                        Ok(flock) => break Ok(Some(flock)),
                        Err((f, errno)) if errno == Errno::EWOULDBLOCK => {
                            file = f; // Locked by another process. Retry.
                        }
                        Err((_, _)) => {
                            break Err(err!("Cannot acquire lock {}", path.display()));
                        }
                    }
                    sleep(LOCK_RETRY_INTERVAL).await;
                }
            })
            .await
            .context(err!("Lock acquire timeout: {}", path.display()))??
        };

        Ok(Self { _flock: flock })
    }

    /// Lock a local mailbox directory.
    /// Lock file `.lock` sits in the mailbox directory.
    pub async fn acquire_mailbox(
        mailbox_dir: &Path,
        mode: Mode,
        interrupt_rx: &mut sync::mpsc::Receiver<ah::Error>,
    ) -> ah::Result<Self> {
        let mut lock_os = mailbox_dir.to_path_buf();
        lock_os.push(".lock");
        Self::acquire(&lock_os, mode, interrupt_rx).await
    }

    /// Lock an IMAP account access.
    ///
    /// This is not supposed to be perfect.
    /// It's only to catch common misuses like running two instances of mailarch at the same time by accident.
    /// We take a lock in the local mailbox base directory.
    pub async fn acquire_imap(
        mailbox_base_dir: &Path,
        mode: Mode,
        interrupt_rx: &mut sync::mpsc::Receiver<ah::Error>,
    ) -> ah::Result<Self> {
        Self::acquire(&mailbox_base_dir.join(".lock"), mode, interrupt_rx).await
    }
}
