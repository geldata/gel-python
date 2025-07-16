#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

srv="https://packages.geldata.com"

curl -fL "${srv}/dist/$(uname -m)-unknown-linux-musl/gel-cli" \
    > "/usr/bin/gel"

chmod +x "/usr/bin/gel"

if command -v useradd >/dev/null 2>&1; then
    useradd --shell /bin/bash gel
else
    # musllinux/alpine doesn't have useradd
    adduser -s /bin/bash -D gel
fi

su -l gel -c "gel server install --version ${GEL_SERVER_VERSION}"
ln -s $(su -l gel -c "gel server info --latest --bin-path") \
    "/usr/bin/gel-server"

gel-server --version
