#!/bin/sh

basedir="$(dirname "$(realpath "$0")")"

[ -f "$basedir/Cargo.toml" ] || die "basedir sanity check failed"
. "$basedir/scripts/lib.sh"

release="both"
while [ $# -ge 1 ]; do
    case "$1" in
        --debug|-d)
            release="debug"
            ;;
        --release|-r)
            release="release"
            ;;
        *)
            die "Invalid option: $1"
            ;;
    esac
    shift
done

cd "$basedir" || die "cd basedir failed."
export MAILARCH_CONF_PREFIX="/opt/mailarch"

# Debug build and test
if [ "$release" = "debug" -o "$release" = "both" ]; then
    cargo build || die "Cargo build (debug) failed."
    cargo test || die "Cargo test failed."
fi

# Release build
if [ "$release" = "release" -o "$release" = "both" ]; then
    if which cargo-auditable >/dev/null 2>&1; then
        cargo auditable build --release || die "Cargo build (release) failed."
        cargo audit --deny warnings bin \
            target/release/mailarch \
            || die "Cargo audit failed."
    else
        cargo build --release || die "Cargo build (release) failed."
    fi
fi
