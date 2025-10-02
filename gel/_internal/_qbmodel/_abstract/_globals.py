# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base definitions for schema globals"""

from __future__ import annotations
from typing_extensions import (
    TypeVar,
)


from gel._internal import _qb

from ._base import GelType


_T = TypeVar("_T", bound=GelType)


class Global(_qb.GelSchemaMetadata):
    @classmethod
    def global_(cls, tp: type[_T]) -> type[_T]:
        return _qb.AnnotatedGlobal(  # type: ignore [return-value]
            tp,
            _qb.Global(
                name=cls.__gel_reflection__.name,
                type_=tp.__gel_reflection__.type_name,
            ),
        )
