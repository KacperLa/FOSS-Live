#!/bin/sh
# Public build artifacts only. Use installed libopus without changing host packages.
set -eu
cd "$(dirname "$0")"
if [ -z "${OPUS_LIB_DIR:-}" ] && ! pkg-config --exists opus 2>/dev/null; then
    opus_library=$(ldconfig -p 2>/dev/null | awk '/libopus\.so\.0 / { print $NF; exit }')
    if [ -z "$opus_library" ]; then
        echo 'Install libopus-dev and pkg-config (or provide OPUS_LIB_DIR).' >&2
        exit 1
    fi
    mkdir -p target/native/lib
    ln -sf "$opus_library" target/native/lib/libopus.so
    export OPUS_LIB_DIR="$PWD/target/native"
fi
if [ "$#" -gt 0 ]; then exec cargo "$@"; fi
source_bytes=$(wc -c < src/main.rs)
if [ "$source_bytes" -gt 200000 ]; then
    echo 'Source exceeds the conservative 200 KB context budget; simplify before adding more.' >&2
    exit 1
fi
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
