# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TYPE_CHECKING, NamedTuple


from . import _query
from ._base import struct

if TYPE_CHECKING:
    from gel import abstract


class ServerVersion(NamedTuple):
    major: int
    minor: int


@struct
class BranchState:
    server_version: ServerVersion
    top_migration: str | None


def fetch_branch_state(
    db: abstract.ReadOnlyExecutor,
) -> BranchState:
    return db.query_required_single(_query.STATE)  # type: ignore [no-any-return]
