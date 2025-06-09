# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


"""Cache helpers"""

from typing import Any

import json

from gel._internal import _atomic
from gel._internal import _platform


def save(key: str, data: bytes | str) -> bool:
    cache_dir = _platform.cache_dir()

    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return False

    path = cache_dir / key

    try:
        _atomic.atomic_write(path, data)
    except OSError:
        return False

    return True


def save_json(key: str, data: Any) -> bool:
    return save(key, json.dumps(data))


def load_text(key: str) -> str | None:
    path = _platform.cache_dir() / key

    try:
        return path.read_text(encoding="utf8")
    except OSError:
        return None


def load_bytes(key: str) -> bytes | None:
    path = _platform.cache_dir() / key

    try:
        return path.read_bytes()
    except OSError:
        return None


def load_json(key: str) -> Any | None:
    path = _platform.cache_dir() / key

    try:
        with open(path, encoding="utf8") as f:
            return json.load(f)
    except (OSError, ValueError, TypeError):
        return None
