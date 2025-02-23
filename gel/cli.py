#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import (
    NoReturn,
    Tuple,
)

import os
import os.path
import pathlib
import platform
import shutil
import ssl
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request


PACKAGE_URL_PREFIX = "https://packages.edgedb.com/dist"
STRONG_CIPHERSUITES = ":".join([
    "TLS_AES_128_GCM_SHA256",
    "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_256_GCM_SHA384",
    "ECDHE-ECDSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-CHACHA20-POLY1305",
    "ECDHE-RSA-CHACHA20-POLY1305",
    "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384",
])


def _die(msg: str) -> NoReturn:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def _warn(msg: str) -> NoReturn:
    print(f"warning: {msg}", file=sys.stderr)


def _run_cli(path: str) -> NoReturn:
    cmd = [path] + sys.argv[1:]
    if os.name == "nt":
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    else:
        os.execv(path, cmd)


def _real_mac_machine(machine: str) -> str:
    import ctypes
    import ctypes.util

    def _sysctl(libc: ctypes.CDLL, name: str) -> str:
        size = ctypes.c_uint(0)
        libc.sysctlbyname(name, None, ctypes.byref(size), None, 0)
        buf = ctypes.create_string_buffer(size.value)
        libc.sysctlbyname(name, buf, ctypes.byref(size), None, 0)
        return buf.value

    libc_path = ctypes.util.find_library("c")
    if not libc_path:
        _die("could not find the C library")
    libc = ctypes.CDLL(libc_path)
    if machine == "i386":
        # check for 32-bit emulation on a 64-bit x86 machine
        if _sysctl(libc, "hw.optional.x86_64") == "1":
            machine = "x86_64"
    elif machine == "x86_64":
        # check for Rosetta
        if _sysctl(libc, "sysctl.proc_translated") == "1":
            machine = "aarch64"

    return machine


def _platform() -> Tuple[str, str]:
    uname = platform.uname()
    uname_sys = uname.system
    machine = uname.machine.lower()
    if (
        uname_sys == "Windows"
        or uname_sys.startswith("CYGWIN_NT")
        or uname_sys.startswith("MINGW64_NT")
        or uname_sys.startswith("MSYS_NT")
    ):
        os = "Windows"
    elif uname_sys == "Darwin":
        if machine == "i386" or machine == "x86_64":
            machine = _real_mac_machine(machine)
        os = "Darwin"
    elif uname_sys == "Linux":
        os = "Linux"
    else:
        _die(f"unsupported OS: {uname_sys}")

    if machine in ("x86-64", "x64", "amd64"):
        machine = "x86_64"
    elif machine == "arm64":
        machine = "aarch64"

    if machine not in ("x86_64", "aarch64") or (
        machine == "aarch64" and os not in ("Darwin", "Linux")
    ):
        _die(f"unsupported hardware architecture: {machine}")

    return os, machine


def _download(url: str, dest: pathlib.Path) -> None:
    if not url.lower().startswith("https://"):
        _die(f"unexpected insecure URL: {url}")

    # Create an SSL context with certificate verification enabled
    ssl_context = ssl.create_default_context()
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.set_ciphers(STRONG_CIPHERSUITES)

    try:
        # Open the URL with the SSL context
        with urllib.request.urlopen(url, context=ssl_context) as response:
            final_url = response.geturl()
            if not final_url.lower().startswith("https://"):
                _die("redirected to a non-HTTPS URL, download aborted.")

            if response.status != 200:
                raise RuntimeError(f"{response.status}")

            spinner_symbols = ['|', '/', '-', '\\']
            msg = "downloading Gel CLI"
            print(f"{msg}", end="\r")
            start = time.monotonic()

            with open(str(dest), mode="wb") as file:
                i = 0
                while True:
                    chunk = response.read(524288)
                    if not chunk:
                        break
                    file.write(chunk)
                    now = time.monotonic()
                    if now - start > 0.2:
                        print(f"\r{msg} {spinner_symbols[i]}", end="\r")
                        start = now
                        i = (i + 1) % len(spinner_symbols)

            # clear
            print(f"{' ' * (len(msg) + 2)}", end="\r")

    except Exception as e:
        _die(f"could not download Gel CLI: {e}")


def _get_binary_cache_dir(os_name) -> pathlib.Path:
    home = pathlib.Path.home()
    if os_name == 'Windows':
        localappdata = os.environ.get('LOCALAPPDATA', '')
        if localappdata:
            base_cache_dir = pathlib.Path(localappdata)
        else:
            base_cache_dir = home / 'AppData' / 'Local'
    elif os_name == 'Linux':
        xdg_cache_home = os.environ.get('XDG_CACHE_HOME', '')
        if xdg_cache_home:
            base_cache_dir = pathlib.Path(xdg_cache_home)
        else:
            base_cache_dir = home / '.cache'
    elif os_name == 'Darwin':
        base_cache_dir = home / 'Library' / 'Caches'
    else:
        _die(f"unsupported OS: {os_name}")

    cache_dir = base_cache_dir / "gel" / "bin"
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        _warn(f"could not create {cache_dir}: {e}")

        try:
            cache_dir = pathlib.Path(tempfile.mkdtemp(prefix="gel"))
        except Exception as e:
            _die(f"could not create temporary directory: {e}")

    return cache_dir


def _get_mountpoint(path: pathlib.Path) -> pathlib.Path:
    path = path.resolve()
    if os.path.ismount(str(path)):
        return path
    else:
        for p in path.parents:
            if os.path.ismount(str(p)):
                return p

        return p


def _install_cli(os_name: str, arch: str, path: pathlib.Path) -> str:
    triple = f"{arch}"
    ext = ""
    if os_name == "Windows":
        triple += "-pc-windows-msvc"
        ext = ".exe"
    elif os_name == "Darwin":
        triple += "-apple-darwin"
    elif os_name == "Linux":
        triple += "-unknown-linux-musl"
    else:
        _die(f"unexpected OS: {os}")

    url = f"{PACKAGE_URL_PREFIX}/{triple}/gel-cli{ext}"

    if path.exists() and not path.is_file():
        _die(f"{path} exists but is not a regular file, "
             f"please remove it and try again")

    _download(url, path)

    try:
        path.chmod(
            stat.S_IRWXU
            | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH,
        )
    except OSError as e:
        _die(f"could not max {path!r} executable: {e}")

    if not os.access(str(path), os.X_OK):
        _die(
            f"cannot execute {path!r} "
            f"(likely because {_get_mountpoint(path)} is mounted as noexec)"
        )


def main() -> NoReturn:
    dev_cli = shutil.which("gel-dev")
    if dev_cli:
        path = pathlib.Path(dev_cli)
    else:
        os, arch = _platform()
        cache_dir = _get_binary_cache_dir(os)
        path = cache_dir / "gel"
        if not path.exists():
            _install_cli(os, arch, path)

    _run_cli(path)


if __name__ == "__main__":
    main()
