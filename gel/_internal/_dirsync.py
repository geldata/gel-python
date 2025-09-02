# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Literal
from collections.abc import Iterable

import os
import shutil
import filecmp
from pathlib import Path

if TYPE_CHECKING:
    from collections.abc import Callable, Set as AbstractSet


class SyncError(Exception):
    pass


def dirsync(
    src: str
    | Path
    | os.PathLike[str]
    | Iterable[str | Path | os.PathLike[str]],
    dst: str | Path | os.PathLike[str],
    *,
    keep: AbstractSet[str | Path | os.PathLike[str]] = frozenset(),
    ignore: AbstractSet[str | Path | os.PathLike[str]] = frozenset(),
    method: Literal["copy", "move"] = "move",
) -> None:
    """
    Recursively make `dst` mirror `src`, batching all filesystem operations:
      1) mkdir for any dirs in src not in dst
      2) atomic copy/replace for files new or changed in src
      3) delete any files/dirs in dst not in src

    :param src: path to source directory (str, Path, or PathLike)
    :param dst: path to destination directory (str, Path, or PathLike)
    :param keep: set of paths to keep in destination directory even if they
                 don't exist in the source directory.
    :param ignore: set of paths to ignore completely.
    :param method: how to sync files ("copy" or "move", latter is default).
    """
    if isinstance(src, Iterable) and not isinstance(src, str):
        src_paths = [Path(src_path) for src_path in src]
    else:
        src_paths = [Path(src)]
    dst_path = Path(dst)
    _assert_safe_paths(src_paths, dst_path)

    create_dirs: set[Path] = set()
    copy_files: list[tuple[Path, Path]] = []
    remove_files: set[Path] = set()
    remove_dirs: set[Path] = set()

    keep_paths = frozenset(Path(p) for p in keep)
    ignore_paths = frozenset(Path(p) for p in ignore)

    # schedule base directory creation
    if not dst_path.exists():
        create_dirs.add(dst_path)

    # scan sources
    for src_path in src_paths:
        for root, dirs, files in os.walk(src_path):
            rel = Path(root).relative_to(src_path)
            if rel in ignore_paths:
                continue

            target_root = dst_path / rel

            if not target_root.exists():
                create_dirs.add(target_root)

            for d in dirs:
                td = target_root / d
                if not td.exists():
                    create_dirs.add(td)

            for f in files:
                src_file = Path(root) / f
                dst_file = target_root / f
                needs_copy = True
                if dst_file.exists():
                    try:
                        needs_copy = not filecmp.cmp(
                            src_file, dst_file, shallow=False
                        )
                    except OSError:
                        needs_copy = True
                if needs_copy:
                    copy_files.append((src_file, dst_file))

    # scan destination for removals
    for dirpath_str, dirs, files in os.walk(dst_path, topdown=False):
        dirpath = Path(dirpath_str)
        rel = dirpath.relative_to(dst_path)
        if rel in ignore_paths:
            continue

        src_dirs = [src_path / rel for src_path in src_paths]

        for f in files:
            dst_file = dirpath / f
            if not _exists_in_any(f, src_dirs) and (rel / f) not in keep_paths:
                remove_files.add(dst_file)

        for d in dirs:
            dst_dir = dirpath / d
            if not _exists_in_any(d, src_dirs) and (rel / d) not in keep_paths:
                remove_dirs.add(dst_dir)

    # execute creations
    for to_make in sorted(
        create_dirs, key=lambda p: len(p.relative_to(dst_path).parts)
    ):
        to_make.mkdir(parents=True, exist_ok=True)

    cp: Callable[[str, str], Any]
    if method == "move":
        cp = os.replace
    elif method == "copy":
        cp = shutil.copy2
    else:
        raise ValueError(f"unexpected copy mode: {method}")

    # execute copies/replaces atomically
    for src_file, dst_file in copy_files:
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        cp(src_file, dst_file)

    # execute file removals
    for to_del in sorted(
        remove_files,
        key=lambda p: len(p.relative_to(dst_path).parts),
        reverse=True,
    ):
        to_del.unlink()

    # execute directory removals
    for to_rmtree in sorted(
        remove_dirs,
        key=lambda p: len(p.relative_to(dst_path).parts),
        reverse=True,
    ):
        shutil.rmtree(to_rmtree)


def _exists_in_any(name: str, paths: Iterable[Path]) -> bool:
    return any((path / name).exists() for path in paths)


def _assert_safe_paths(srcs: list[Path], dst: Path) -> None:
    """Ensure source and destination are valid and not nested."""
    for src in srcs:
        if not src.exists() or not src.is_dir():
            raise SyncError(f"Source {src!r} must exist and be a directory.")
    if dst.exists() and not dst.is_dir():
        raise SyncError(f"Destination {dst!r} exists and is not a directory.")
    dst_resolved = dst.resolve()
    for src in srcs:
        src_resolved = src.resolve()
        if src_resolved == dst_resolved:
            raise SyncError(
                "Source and destination must be different directories."
            )
        if dst_resolved in src_resolved.parents:
            raise SyncError("Destination must not be inside source.")
        if src_resolved in dst_resolved.parents:
            raise SyncError("Source must not be inside destination.")
