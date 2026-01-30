#!/bin/sh

set -eu

TAG="$(git describe --tags | head -1)"
PKGNAME="$(./pkgname.sh)"
REPO="${REPO:-emqx/greptimedb-ingester-erlnif}"
URL="https://github.com/${REPO}/releases/download/$TAG/$PKGNAME"
NIF_SO_NAME="libgreptimedb_nif"
NIF_PATH="priv/${NIF_SO_NAME}.so"

mkdir -p _packages
echo "Attempting to download pre-built package from ${URL}"
if [ ! -f "_packages/${PKGNAME}" ]; then
    curl -f -L --no-progress-meter -o "_packages/${PKGNAME}" "${URL}"
fi

if [ ! -f "_packages/${PKGNAME}.sha256" ]; then
    curl -f -L --no-progress-meter -o "_packages/${PKGNAME}.sha256" "${URL}.sha256"
fi

if [ "$(uname -s)" = "Darwin" ]; then
    # macOS
    echo "$(cat "_packages/${PKGNAME}.sha256")  _packages/${PKGNAME}" | shasum -a 256 -c || exit 1
else
    # Linux and other Unix-like systems
    echo "$(cat "_packages/${PKGNAME}.sha256")  _packages/${PKGNAME}" | sha256sum -c || exit 1
fi

mkdir -p priv
gzip -c -d "_packages/${PKGNAME}" > "${NIF_PATH}"
