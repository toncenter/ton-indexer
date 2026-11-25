#!/bin/sh
# Shared by local builds, CMake, Docker and CI. Run the generator on the host.
set -eu
directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$directory/../../.."
. index/acton/catalog/catalog.lock
file=index/acton/catalog/catalog.json
digest() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"
    else shasum -a 256 "$1"
    fi | cut -d ' ' -f 1
}
if [ ! -f "$file" ] || [ "$(digest "$file")" != "$ABI_CATALOG_SHA256" ]; then
    temporary=$(mktemp "$file.XXXXXX")
    trap 'rm -f "$temporary"' EXIT HUP INT TERM
    curl --fail --silent --show-error --location --max-time 120 "$ABI_CATALOG_URL" -o "$temporary"
    [ "$(digest "$temporary")" = "$ABI_CATALOG_SHA256" ] || {
        echo "ABI catalog checksum mismatch" >&2
        exit 1
    }
    mv "$temporary" "$file"
    trap - EXIT HUP INT TERM
fi
env -u GOOS -u GOARCH CGO_ENABLED=0 go tool tolk-abi-to-go \
    --catalog "$file" --output-dir index/acton/catalog --package catalog "$@"
