# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

import difflib
import filecmp
import pathlib
import os


def unified_dir_diff(
    dir1: str | pathlib.Path | os.PathLike[str],
    dir2: str | pathlib.Path | os.PathLike[str],
    ignore: list[str] | None = None,
    hide: list[str] | None = None,
) -> list[str]:
    """
    Recursively compare dir1 and dir2 and print unified diffs
    for files that differ, plus listings of only-in-one-side files.

    Args:
        dir1: Path to the first directory.
        dir2: Path to the second directory.
        ignore: List of file/directory names to ignore.
        hide: List of file/directory names to hide.

    Returns:
        A list of unified diff lines.
    """

    dir1_path = pathlib.Path(dir1)
    dir2_path = pathlib.Path(dir2)
    diff: list[str] = []

    def _recurse(dcmp: filecmp.dircmp[str]) -> None:
        nonlocal diff

        # Report files only in dir1 or dir2
        diff.extend(
            f"Only in {dcmp.left}: {name}" for name in sorted(dcmp.left_only)
        )
        diff.extend(
            f"Only in {dcmp.right}: {name}" for name in sorted(dcmp.right_only)
        )

        # Unified diff for differing files
        for name in sorted(dcmp.diff_files):
            path1: str = os.path.join(dcmp.left, name)
            path2: str = os.path.join(dcmp.right, name)
            with (
                open(path1, encoding="utf-8", errors="replace") as f1,
                open(path2, encoding="utf-8", errors="replace") as f2,
            ):
                lines1 = [line.rstrip() for line in f1]
                lines2 = [line.rstrip() for line in f2]

            diff.extend(
                difflib.unified_diff(
                    lines1, lines2, fromfile=path1, tofile=path2, lineterm=""
                )
            )

        # Recurse into common subdirectories
        for sub_dcmp in dcmp.subdirs.values():
            _recurse(sub_dcmp)

    dcmp = filecmp.dircmp(dir1, dir2, ignore=ignore, hide=hide)
    for name in dcmp.common_files:
        f1 = dir1_path / pathlib.Path(name)
        f2 = dir2_path / pathlib.Path(name)
        if (
            not filecmp.cmp(f1, f2, shallow=False)
            and name not in dcmp.diff_files
        ):
            dcmp.diff_files.append(name)

    _recurse(dcmp)

    return diff
