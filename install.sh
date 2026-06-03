#!/bin/sh
# -*- coding: utf-8 -*-

basedir="$(dirname "$(realpath "$0")")"

. "$basedir/scripts/lib.sh"

entry_checks()
{
    [ -d "$target" ] || die "mailarch is not built! Run ./build.sh"
    [ "$(id -u)" = "0" ] || die "Must be root to install mailarch."
}

install_dirs()
{
    do_install \
        -o root -g root -m 0755 \
        -d /opt/mailarch/bin

    do_install \
        -o root -g root -m 0755 \
        -d /opt/mailarch/etc/mailarch
}

install_conf()
{
    if [ -e /opt/mailarch/etc/mailarch/mailarch.conf ]; then
        do_chown root:root /opt/mailarch/etc/mailarch/mailarch.conf
        do_chmod 0644 /opt/mailarch/etc/mailarch/mailarch.conf
    else
        do_install \
            -o root -g root -m 0644 \
            "$basedir/mailarch.conf" \
            /opt/mailarch/etc/mailarch/mailarch.conf
    fi
}

install_mailarch()
{
    do_install \
        -o root -g root -m 0755 \
        "$target/mailarch" \
        /opt/mailarch/bin/

    do_install \
        -o root -g root -m 0755 \
        "$basedir/claws-mail-archived" \
        /opt/mailarch/bin/
}

release="release"
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
target="$basedir/target/$release"

entry_checks
install_dirs
install_conf
install_mailarch
