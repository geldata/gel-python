# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base object types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING


from gel._internal import _qb


if TYPE_CHECKING:

    from ._base import GelType


def assert_single(
    cls: type[GelType],
    *,
    message: _qb.ExprCompatible | str | None = None,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.Expr:
    kwargs: dict[str, _qb.ExprCompatible] = {}
    if message is None:
        pass
    elif isinstance(message, str):
        kwargs["message"] = _qb.StringLiteral(val=message)
    else:
        kwargs["message"] = _qb.edgeql_qb_expr(message)

    subj = _qb.edgeql_qb_expr(cls if __operand__ is None else __operand__)
    return _qb.FuncCall(
        fname="std::assert_single",
        args=[subj],
        kwargs=kwargs,
        type_=subj.type,
    )
