# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import functools
import os
import pathlib
import sys

if sys.platform == "darwin":

    def config_dir() -> pathlib.Path:
        return (
            pathlib.Path.home() / "Library" / "Application Support" / "edgedb"
        )

    def cache_dir() -> pathlib.Path:
        return pathlib.Path.home() / "Library" / "Caches" / "gel" / "python"

    IS_WINDOWS = False

elif sys.platform == "win32":
    import ctypes
    from ctypes import windll

    _CSIDL_LOCAL_APPDATA = 28

    @functools.cache
    def _app_data_dir() -> pathlib.Path:
        path_buf = ctypes.create_unicode_buffer(255)
        windll.shell32.SHGetFolderPathW(
            0, _CSIDL_LOCAL_APPDATA, 0, 0, path_buf
        )
        return pathlib.Path(path_buf.value)

    def config_dir() -> pathlib.Path:
        return _app_data_dir() / "EdgeDB" / "config"

    def cache_dir() -> pathlib.Path:
        return _app_data_dir() / "GelData" / "gel-python" / "cache"

    IS_WINDOWS = True

else:

    def config_dir() -> pathlib.Path:
        xdg_conf_dir = pathlib.Path(os.environ.get("XDG_CONFIG_HOME", "."))
        if not xdg_conf_dir.is_absolute():
            xdg_conf_dir = pathlib.Path.home() / ".config"
        return xdg_conf_dir / "edgedb"

    def cache_dir() -> pathlib.Path:
        xdg_cache_home = pathlib.Path(os.environ.get("XDG_CACHE_HOME", "."))
        if not xdg_cache_home.is_absolute():
            xdg_cache_home = pathlib.Path.home() / ".cache"
        return xdg_cache_home / "gel" / "python"

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
