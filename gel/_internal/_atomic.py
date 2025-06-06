# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

import contextlib
import os
import pathlib
import tempfile


def atomic_write(
    path: str | os.PathLike[str] | pathlib.Path,
    data: str | bytes,
    *,
    encoding: str = "utf8",
) -> None:
    """
    Atomically write 'data' to 'path'.
    If anything goes wrong, the original file is left untouched.
    encoding: only used when mode is 'w'.
    """
    path = pathlib.Path(path)
    dirpath = path.parent
    filename = path.name

    if (
        hasattr(os, "O_DIRECTORY")
        and os.replace in os.supports_dir_fd
        and os.remove in os.supports_dir_fd
    ):
        try:
            dir_fd = os.open(dirpath, os.O_DIRECTORY)
        except OSError:
            dir_fd = None
    else:
        dir_fd = None

    mode = "wb" if isinstance(data, bytes) else "w"

    try:
        fd, tmp_path = tempfile.mkstemp(prefix=filename, dir=dirpath)
        try:
            # Wrap fd to a file object
            if "b" in mode:
                f = os.fdopen(fd, mode)
            else:
                f = os.fdopen(fd, mode, encoding=encoding)
            try:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            finally:
                f.close()
            # Atomically replace the old file
            os.replace(tmp_path, path, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except Exception:
            with contextlib.suppress(OSError):
                os.remove(tmp_path, dir_fd=dir_fd)
            raise
    finally:
        if dir_fd is not None:
            os.close(dir_fd)
