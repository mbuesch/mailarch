# mailarch

Archive emails from an
[IMAP server](https://en.wikipedia.org/wiki/Internet_Message_Access_Protocol)
to a local
[Maildir](https://en.wikipedia.org/wiki/Maildir).

## What it does

`mailarch` connects to an IMAP server, iterates over all mailboxes, and archives messages older than a configured number of days to a local Maildir directory.

It is designed for exactly one specific **use case**:

Automatically archive old emails in the background to a local Maildir without any human interaction.
The motivation is to keep the information on the remote server to a minimum, while still having access to all emails locally.
This can reduce the damage in case of a server compromise, because mail servers typically contain very sensitive information.

Three operating modes are supported:

- **copy**: Archive to local Maildir, keep messages on the server
- **move**: Archive to local Maildir, delete messages from the server
- **dry-run**: Show what would be done without making any changes

## What this tool is **not** about

This is not a generic IMAP client.
It cannot be used to do arbitrary operations on the server, and it is not designed for interactive use.

## Build & Install

```sh
./build.sh          # builds release binary
run0 ./install.sh   # installs to /opt/mailarch/
```

## Configuration

The default configuration file is installed at `/opt/mailarch/etc/mailarch/mailarch.conf`:

```toml
[imap]
host = "mail.example.com"
#port = 993
username = "user@example.com"
password = "secret"

[archive]
directory = "/home/user/mail/archive"
min_age_days = 30
# skip_mailboxes = [ "INBOX/Spam", "INBOX/Trash" ]
```

## Usage

```sh
mailarch [--config <path>] <copy|move|dry-run>
```

## systemd

A service and timer unit are provided to run `mailarch` automatically.

1. Copy the units:
   ```sh
   cp mailarch.service mailarch.timer /etc/systemd/system/
   ```

2. Edit `mailarch.service` to set the correct `User`/`Group` and the desired archiving mode (`copy` or `move`) in `ExecStart`.

3. Enable and start the timer:
   ```sh
   systemctl daemon-reload
   systemctl enable --now mailarch.timer
   ```

The timer runs once per day (and 5 minutes after boot).
Adjust `OnUnitActiveSec` in `mailarch.timer` to change the schedule.

## License

Copyright 2026 Michael Buesch <m@bues.ch> with the help of AI coding assistants.

MIT OR Apache-2.0
