# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING

import contextlib
import importlib
import os
import sys

if TYPE_CHECKING:
    from collections.abc import Iterator


def _set_sys_path(entries: list[str]) -> None:
    if sys.path is None:
        # Might happen at shutdown
        return
    sys.path[:] = entries
    importlib.invalidate_caches()


@contextlib.contextmanager
def sys_path(*paths: os.PathLike[str] | str) -> Iterator[None]:
    """Modify sys.path by temporarily placing the given entry in front"""
    orig_sys_path = sys.path[:]
    entries = [os.fspath(path) for path in paths]
    paths_set = {*entries}
    _set_sys_path(
        [*entries, *(p for p in orig_sys_path if p not in paths_set)]
    )
    try:
        yield
    finally:
        _set_sys_path(orig_sys_path)
