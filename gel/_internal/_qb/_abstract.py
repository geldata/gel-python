# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Abstract type definitions for the EdgeQL query builder"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any
from typing_extensions import Self

import abc

if TYPE_CHECKING:
    from gel._internal import _edgeql
    from gel._internal import _reflection


class Expr(abc.ABC):
    @abc.abstractproperty
    def precedence(self) -> _edgeql.Precedence: ...

    @abc.abstractproperty
    def type(self) -> _reflection.SchemaPath: ...

    @abc.abstractmethod
    def __edgeql_expr__(self) -> str: ...

    def __edgeql_qb_expr__(self) -> Self:
        return self


class AbstractDescriptor:
    pass


class AbstractFieldDescriptor(AbstractDescriptor):
    def get(self, owner: Any) -> Any:
        raise NotImplementedError(f"{type(self)}.get")
