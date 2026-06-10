use crate::{
    config::{Config, MailboxFormat},
    deferred::Deferred,
    imap_client::ImapClient,
    lock::Lock,
    maildir::{Maildir, StoreResult, mailbox_path},
};
use anyhow::{self as ah, Context as _, format_err as err};
use clap::Parser;
use log::{error, info, warn};
use std::{path::PathBuf, time::Duration};
use tokio::{
    runtime,
    signal::unix::{SignalKind, signal},
    sync, task,
};

mod config;
mod deferred;
mod imap_client;
mod lock;
mod maildir;

const WORKER_THREADS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    Copy,
    Move,
    DryRun,
}

impl Mode {
    #[must_use]
    pub fn prefix(self) -> &'static str {
        match self {
            Mode::DryRun => "[dry-run] ",
            Mode::Copy => "[copy] ",
            Mode::Move => "[move] ",
        }
    }

    #[must_use]
    pub fn is_copy(self) -> bool {
        self == Mode::Copy
    }

    #[must_use]
    pub fn is_move(self) -> bool {
        self == Mode::Move
    }

    #[must_use]
    pub fn is_dry_run(self) -> bool {
        self == Mode::DryRun
    }
}

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Archiving mode.
    ///
    /// copy: Archive messages to local maildir but keep them on the server.
    ///
    /// move: Archive messages to local maildir and delete them from the server.
    ///
    /// dry-run: Show what would be done without making any changes.
    mode: Mode,

    /// Path to the configuration file.
    #[arg(
        long,
        short,
        default_value = "/opt/mailarch/etc/mailarch/mailarch.conf"
    )]
    config: PathBuf,

    /// Number of messages to fetch in one batch when processing a mailbox.
    #[arg(long, short = 'C', default_value_t = 128, value_name = "NR_MESSAGES")]
    fetch_chunk_size: usize,
}

/// Archive messages in one already-selected mailbox. Returns the number of messages processed.
async fn process_mailbox(
    client: &mut ImapClient,
    name: &str,
    local_dir: &std::path::Path,
    min_age_days: u32,
    format: MailboxFormat,
    args: &Args,
    interrupt_rx: &mut sync::mpsc::Receiver<ah::Error>,
) -> ah::Result<usize> {
    let pfx = args.mode.prefix();

    let uids = match client.fetch_archivable_uids(min_age_days).await {
        Ok(u) => u,
        Err(e) => {
            error!("{pfx}Failed to search '{name}': {e:#}");
            return Ok(0);
        }
    };
    info!("{pfx}Found {} archivable message(s)", uids.len());

    if uids.is_empty() {
        return Ok(0);
    }

    let _mailbox_lock = Lock::acquire_mailbox(local_dir, args.mode, interrupt_rx)
        .await
        .with_context(|| err!("Cannot lock mailbox '{name}'"))?;

    // Open the maildir lazily in the background.
    let mut maildir = Deferred::new({
        let local_dir = local_dir.to_path_buf();
        let dry_run = args.mode.is_dry_run();
        async move { Maildir::open(&local_dir, format, dry_run).await }
    });

    let mut archived_uids = Vec::with_capacity(uids.len());
    for chunk in uids.chunks(args.fetch_chunk_size) {
        if let Ok(e) = interrupt_rx.try_recv() {
            return Err(e);
        }

        let messages = match client.fetch_messages(chunk).await {
            Ok(m) => m,
            Err(e) => {
                error!("{pfx}Failed to fetch messages from '{name}': {e:#}");
                continue;
            }
        };

        for msg in messages {
            if let Ok(e) = interrupt_rx.try_recv() {
                return Err(e);
            }

            let uid = msg.uid;

            let maildir = match maildir.as_mut().await? {
                Ok(m) => m,
                Err(e) => {
                    return Err(err!("{pfx}Cannot open local maildir for '{name}': {e:#}"));
                }
            };
            match maildir.store(&msg.raw, msg.received_at).await {
                Ok(StoreResult::Stored) => {
                    info!("{pfx}Archived UID {uid} from '{name}'");
                    archived_uids.push(uid);
                }
                Ok(StoreResult::AlreadyExists { identical: true }) => {
                    info!(
                        "{pfx}UID {uid} from '{name}' already archived.{}",
                        if args.mode.is_copy() {
                            ""
                        } else {
                            " Will delete from server."
                        }
                    );
                    archived_uids.push(uid);
                }
                Ok(StoreResult::AlreadyExists { identical: false }) => {
                    warn!(
                        "{pfx}UID {uid} from '{name}' exists locally but content differs. Keeping on server."
                    );
                }
                Err(e) => {
                    error!("{pfx}Failed to store UID {uid} locally from '{name}': {e:#}");
                }
            }
        }
    }

    // Only delete from server once we're confident the local copy is good.
    let mut processed = archived_uids.len();
    match args.mode {
        Mode::DryRun => {
            for uid in &archived_uids {
                info!("{pfx}Would delete UID {uid} from '{name}' on server");
            }
        }
        Mode::Copy => {
            if processed > 0 {
                info!(
                    "{pfx}'{name}': archived {} message(s), kept on server.",
                    archived_uids.len()
                );
            }
        }
        Mode::Move => match client.delete_messages(&archived_uids).await {
            Ok(()) => (),
            Err(e) => {
                error!("{pfx}Failed to delete messages from '{name}': {e:#}");
                processed = 0;
            }
        },
    }
    if processed > 0 {
        info!(
            "{pfx}'{name}': archived {}{processed} message(s).",
            if args.mode.is_move() {
                "and removed "
            } else {
                ""
            }
        );
    }
    Ok(processed)
}

async fn handle_client(
    config: &Config,
    args: &Args,
    client: &mut ImapClient,
    interrupt_rx: &mut sync::mpsc::Receiver<ah::Error>,
) -> ah::Result<()> {
    let pfx = args.mode.prefix();

    let mailboxes = client.list_mailboxes().await?;

    if mailboxes.is_empty() {
        info!("{pfx}No mailboxes found on server.");
        return Ok(());
    }
    info!("{pfx}Found {} mailbox(es) on server", mailboxes.len());

    let mut total = 0;

    for mailbox in &mailboxes {
        if let Ok(e) = interrupt_rx.try_recv() {
            return Err(e);
        }
        let name = &mailbox.name;

        if config.archive().skip_mailboxes().contains(name) {
            info!("{pfx}Skipping mailbox '{name}' (excluded in config)");
            continue;
        }

        info!("{pfx}Processing mailbox '{name}'");

        if let Err(e) = client.select_mailbox(name).await {
            error!("{pfx}Skipping '{name}': {e:#}");
            continue;
        }

        total += process_mailbox(
            client,
            name,
            &mailbox_path(config, name, mailbox.delimiter),
            config.archive().min_age_days(),
            config.archive().format(),
            args,
            interrupt_rx,
        )
        .await?;
    }

    info!(
        "{pfx}Done. {} {total} message(s) total.",
        if args.mode.is_copy() {
            "archived (kept on server)"
        } else {
            "archived and removed"
        }
    );

    Ok(())
}

async fn async_main() -> ah::Result<()> {
    let args = Args::parse();
    match args.mode {
        Mode::DryRun => info!("Dry-run mode enabled: No changes will be made."),
        Mode::Copy => info!(
            "Copy mode enabled: Messages will be archived locally but not deleted from server."
        ),
        Mode::Move => (),
    }

    let pfx = args.mode.prefix();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sighup = signal(SignalKind::hangup()).unwrap();
    let (exit_tx, mut exit_rx) = sync::mpsc::channel(1);
    let (interrupt_tx, mut interrupt_rx) = sync::mpsc::channel(1);

    let config = Config::load(&args.config).await?;

    task::spawn(async move {
        let _imap_lock =
            match Lock::acquire_imap(config.archive().directory(), args.mode, &mut interrupt_rx)
                .await
            {
                Ok(lock) => lock,
                Err(e) => {
                    exit_tx.send(Err(e)).await.expect("Exit code failed");
                    return;
                }
            };
        let result = match ImapClient::connect(config.imap()).await {
            Ok(mut client) => {
                info!(
                    "{pfx}Connected and authenticated to {}",
                    config.imap().host()
                );
                match handle_client(&config, &args, &mut client, &mut interrupt_rx).await {
                    Ok(()) => client.logout().await,
                    Err(e) => {
                        let _ = client.logout().await;
                        Err(e)
                    }
                }
            }
            Err(e) => Err(e),
        };
        exit_tx.send(result).await.expect("Exit code failed");
    });

    loop {
        tokio::select! {
            biased;
            code = exit_rx.recv() => {
                break code.unwrap_or_else(|| Err(err!("Unknown error code.")));
            }
            _ = sigint.recv() => {
                eprintln!("SIGINT: Interrupting...");
                let _ = interrupt_tx.send(err!("SIGINT: Interrupted.")).await;
            }
            _ = sigterm.recv() => {
                eprintln!("SIGTERM: Terminating...");
                let _ = interrupt_tx.send(err!("SIGTERM: Terminated.")).await;
            }
            _ = sighup.recv() => {
                eprintln!("SIGHUP: Reloading is not supported");
            }
        }
    }
}

fn main() -> ah::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::new()
            .filter_or("MAILARCH_LOG", "info")
            .write_style_or("MAILARCH_LOG_STYLE", "auto"),
    );

    runtime::Builder::new_multi_thread()
        .thread_keep_alive(Duration::from_secs(10))
        .max_blocking_threads(WORKER_THREADS * 16)
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
        .context("Tokio runtime builder")?
        .block_on(async_main())
}
