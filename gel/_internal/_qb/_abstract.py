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


class Expr(abc.ABC):
    @abc.abstractproperty
    def precedence(self) -> _edgeql.Precedence: ...

    @abc.abstractmethod
    def __edgeql_expr__(self) -> str: ...

    def __edgeql_qb_expr__(self) -> Self:
        return self


class AbstractDescriptor:
    pass


class AbstractFieldDescriptor(abc.ABC, AbstractDescriptor):
    @abc.abstractmethod
    def get(self, owner: Any) -> Any: ...
