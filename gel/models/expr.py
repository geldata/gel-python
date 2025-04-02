# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import Callable, TypeVar, Union

from gel._internal.typing_hacks import parametric
from . import types


T = TypeVar("T", bound=Union[types.AnyType, type], covariant=True)


class Expression(parametric.SingleParametricType[T]):
    pass


class SelectExpression(Expression[T]):
    def __init__(self, expr: Expression[T]) -> None:
        self._expr = expr

    def order_by(
        self,
        cb: Callable[[type[T]], Expression[T]],
    ) -> SelectOrderExpression[T]:
        return SelectOrderExpression(self, cb(self.type))


class SelectOrderExpression(Expression[T]):
    def __init__(self, expr: Expression[T], ordering: Expression[T]) -> None:
        self._expr = expr
        self._ordering = ordering


def select(expr: Expression[T], /) -> SelectExpression[T]:
    return SelectExpression(expr)
