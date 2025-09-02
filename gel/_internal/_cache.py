# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


"""Cache helpers"""

from typing import Any

import json
import pathlib

from gel._internal import _atomic
from gel._internal import _platform


_default_cache_dir = _platform.cache_dir()


def save(
    key: str,
    data: bytes | str,
    *,
    cache_dir: pathlib.Path | None = None,
    extra_key: str | None = None,
) -> bool:
    if cache_dir is None:
        cache_dir = _default_cache_dir
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return False

    if extra_key is not None:
        metadata = f"{key}.metadata"
        try:
            _atomic.atomic_write(cache_dir / metadata, extra_key)
        except OSError:
            return False

    try:
        _atomic.atomic_write(cache_dir / key, data)
    except OSError:
        return False

    return True


def save_json(
    key: str,
    data: Any,
    *,
    cache_dir: pathlib.Path | None = None,
    extra_key: str | None = None,
) -> bool:
    if cache_dir is None:
        cache_dir = _default_cache_dir
    return save(
        key,
        json.dumps(data),
        cache_dir=cache_dir,
        extra_key=extra_key,
    )


def _verify_cache(
    key: str,
    *,
    cache_dir: pathlib.Path,
    extra_key: str,
) -> bool:
    metadata = f"{key}.metadata"
    try:
        value = (cache_dir / metadata).read_text(encoding="utf8")
    except OSError:
        return False
    else:
        return value == extra_key


def load_text(
    key: str,
    *,
    cache_dir: pathlib.Path | None = None,
    extra_key: str | None = None,
) -> str | None:
    if cache_dir is None:
        cache_dir = _default_cache_dir
    if extra_key is not None and not _verify_cache(
        key, cache_dir=cache_dir, extra_key=extra_key
    ):
        return None
    try:
        return (cache_dir / key).read_text(encoding="utf8")
    except OSError:
        return None


def load_bytes(
    key: str,
    *,
    cache_dir: pathlib.Path | None = None,
    extra_key: str | None = None,
) -> bytes | None:
    if cache_dir is None:
        cache_dir = _default_cache_dir
    if extra_key is not None and not _verify_cache(
        key, cache_dir=cache_dir, extra_key=extra_key
    ):
        return None
    try:
        return (cache_dir / key).read_bytes()
    except OSError:
        return None


def load_json(
    key: str,
    *,
    cache_dir: pathlib.Path | None = None,
    extra_key: str | None = None,
) -> Any | None:
    if cache_dir is None:
        cache_dir = _default_cache_dir
    if extra_key is not None and not _verify_cache(
        key, cache_dir=cache_dir, extra_key=extra_key
    ):
        return None
    try:
        with open(cache_dir / key, encoding="utf8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None
