# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import os
import pathlib
import sys

if sys.platform == "darwin":

    def config_dir() -> pathlib.Path:
        return (
            pathlib.Path.home() / "Library" / "Application Support" / "edgedb"
        )

    IS_WINDOWS = False

elif sys.platform == "win32":
    import ctypes
    from ctypes import windll

    def config_dir() -> pathlib.Path:
        path_buf = ctypes.create_unicode_buffer(255)
        csidl = 28  # CSIDL_LOCAL_APPDATA
        windll.shell32.SHGetFolderPathW(0, csidl, 0, 0, path_buf)
        return pathlib.Path(path_buf.value) / "EdgeDB" / "config"

    IS_WINDOWS = True

else:

    def config_dir() -> pathlib.Path:
        xdg_conf_dir = pathlib.Path(os.environ.get("XDG_CONFIG_HOME", "."))
        if not xdg_conf_dir.is_absolute():
            xdg_conf_dir = pathlib.Path.home() / ".config"
        return xdg_conf_dir / "edgedb"

    IS_WINDOWS = False


def old_config_dir() -> pathlib.Path:
    return pathlib.Path.home() / ".edgedb"


def search_config_dir(*suffix: str) -> pathlib.Path:
    rv = config_dir().joinpath(*suffix)
    if rv.exists():
        return rv

    fallback = old_config_dir().joinpath(*suffix)
    if fallback.exists():
        return fallback

    # None of the searched files exists, return the new path.
    return rv
