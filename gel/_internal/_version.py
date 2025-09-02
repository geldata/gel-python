# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Packge version utilities."""

from __future__ import annotations
from typing import TYPE_CHECKING

import dataclasses
import functools
import importlib.metadata
import json
import pathlib
import re
import urllib.parse

from gel import _version as _gel_py_ver

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclasses.dataclass(kw_only=True, frozen=True)
class DirectURLOrigin:
    url: str
    editable: bool = False
    commit_id: str | None = None


@functools.cache
def get_direct_url_origin(
    dist_name: str,
    path: Sequence[str] | None = None,
) -> DirectURLOrigin | None:
    """Return PEP 660 Direct URL Origin metadata for package if present"""
    if path is not None:
        dists = importlib.metadata.distributions(name=dist_name, path=[*path])
    else:
        dists = importlib.metadata.distributions(name=dist_name)

    # Distribution finder will return a Distribution for
    # each matching distribution in sys.path even if they're
    # duplicate.  We try them in order until we find one that
    # has direct_url.json in it.
    for dist in dists:
        url_origin = _get_direct_url_origin(dist)
        if url_origin is not None:
            return url_origin

    return None


def _get_direct_url_origin(
    dist: importlib.metadata.Distribution,
) -> DirectURLOrigin | None:
    try:
        data = dist.read_text("direct_url.json")
    except OSError:
        return None
    if data is None:
        return None
    try:
        info = json.loads(data)
    except ValueError:
        return None
    if not isinstance(info, dict):
        return None

    url = info.get("url")
    if not url:
        # URL must be present, metadata is corrupt
        return None

    dir_info = info.get("dir_info")
    if isinstance(dir_info, dict):
        editable = dir_info.get("editable", False)
    else:
        editable = False
    vcs_info = info.get("vcs_info")
    if isinstance(vcs_info, dict):
        commit_id = vcs_info.get("commit_id")
    else:
        commit_id = None

    return DirectURLOrigin(
        url=url,
        editable=editable,
        commit_id=commit_id,
    )


def _is_revision_sha(s: str) -> bool:
    return bool(re.match(r"^\b[0-9a-f]{5,40}\b$", s))


def get_origin_source_dir(dist_name: str) -> pathlib.Path | None:
    url_origin = get_direct_url_origin(dist_name)
    if url_origin is None:
        return None

    try:
        dir_url = urllib.parse.urlparse(url_origin.url)
    except ValueError:
        return None

    if dir_url.scheme != "file":
        # Non-local URL?
        return None

    if not dir_url.path:
        # No path?
        return None

    return pathlib.Path(dir_url.path)


def get_origin_commit_id(dist_name: str) -> str | None:
    url_origin = get_direct_url_origin(dist_name)
    if url_origin is None:
        return None

    if url_origin.commit_id is not None:
        return url_origin.commit_id

    source_dir = get_origin_source_dir(dist_name)
    if source_dir is None:
        return None

    git_dir = source_dir / ".git"
    if not git_dir.exists():
        return None

    try:
        head = (git_dir / "HEAD").read_text().strip()
    except OSError:
        return None

    if not head:
        return None

    if m := re.match(r"ref:\s*(.*)", head):
        head_ref_path = m.group(1)
        head_ref = git_dir / pathlib.Path(head_ref_path)
        if not head_ref.is_relative_to(git_dir):
            # Huh?
            return None

        if head_ref.exists():
            try:
                commit_id = head_ref.read_text().strip()
            except OSError:
                return None
        else:
            # Check packed refs
            try:
                packed_refs = (git_dir / "packed-refs").read_text()
            except OSError:
                return None

            for line in packed_refs.splitlines():
                if line.startswith("#"):
                    continue
                sha, _, ref = line.partition(" ")
                ref = ref.strip()
                if not ref:
                    continue
                if ref == head_ref_path:
                    commit_id = sha
                    break
            else:
                return None
    else:
        commit_id = head

    if not _is_revision_sha(commit_id):
        return None
    else:
        return commit_id


@functools.cache
def get_project_version_key() -> str:
    ver_key = _gel_py_ver.__version__
    commit_id = get_origin_commit_id("gel")
    if commit_id:
        ver_key = f"{ver_key}.dev{commit_id[:9]}"
    return ver_key


@functools.cache
def get_project_source_root() -> pathlib.Path | None:
    return get_origin_source_dir("gel")
