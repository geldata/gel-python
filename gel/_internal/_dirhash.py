# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Directory hashing utilities.

This module provides functionality to compute hashes of directory trees based
on file contents. It's primarily used for generating content-based hashes that
can detect changes in directory structures and file contents across multiple
directories with specific file extensions.

The hashing is deterministic and uses SHA-1 for performance, making it
suitable for use cases where speed is more important than cryptographic
security (e.g., build system cache invalidation).
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import hashlib
import os
import pathlib

if TYPE_CHECKING:
    from collections.abc import Iterable


def dirhash(
    dirs: Iterable[tuple[str, str]],
    *,
    extra_files: Iterable[os.PathLike[str]] | None = None,
    extra_data: bytes | None = None,
) -> bytes:
    """Compute a hash digest of directory contents and additional data.

    This function recursively scans directories for files matching specific
    extensions and computes a SHA-1 hash of their combined contents. The
    hash includes file contents from matching files in the specified
    directories, optional extra files, and optional extra data.

    Args:
        dirs: An iterable of (directory_path, file_extension) tuples.
            Each tuple specifies a directory to scan and the file extension
            to filter for (e.g., ('.py', '.txt')). Only files ending with
            the specified extension will be included in the hash.
        extra_files: Optional iterable of additional file paths to include
            in the hash computation. These files are included regardless
            of their extension or location.
        extra_data: Optional bytes to include in the hash computation.
            This can be used to incorporate additional context or metadata
            into the hash.

    Returns:
        The SHA-1 digest as bytes representing the combined hash of all
        included file contents and extra data.

    Raises:
        OSError: If any of the specified directories or files cannot be
            accessed or read.
        FileNotFoundError: If any of the specified paths don't exist.

    Note:
        - Files are processed in sorted order by their resolved absolute
          paths to ensure deterministic hash computation.
        - The function uses SHA-1 for performance reasons, not cryptographic
          security.
        - Symbolic links are resolved to their targets before hashing.
    """

    def hash_dir(dirname: str, ext: str, paths: list[pathlib.Path]) -> None:
        with os.scandir(dirname) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith(ext):
                    paths.append(pathlib.Path(entry.path).resolve(strict=True))
                elif entry.is_dir():
                    hash_dir(entry.path, ext, paths)

    paths: list[pathlib.Path] = []
    for dirname, ext in dirs:
        hash_dir(dirname, ext, paths)

    if extra_files:
        paths.extend(
            pathlib.Path(extra_file).resolve(strict=True)
            for extra_file in extra_files
        )

    h = hashlib.sha1()  # noqa: S324 # sha1 is the fastest one

    for path in sorted(paths):
        h.update(path.read_bytes())

    if extra_data is not None:
        h.update(extra_data)

    return h.digest()
