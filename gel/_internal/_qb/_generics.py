# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Typing generics for the EdgeQL query builder."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    get_args,
)


import functools

from gel._internal import _typing_inspect
from gel._internal import _utils

from ._abstract import AbstractFieldDescriptor
from ._expressions import InfixOp, Path
from ._protocols import TypeClassProto, edgeql_qb_expr, is_exprmethod
from ._render import toplevel_edgeql

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._abstract import Expr


OP_OVERLOADS = frozenset(
    {
        "__add__",
        "__and__",
        "__divmod__",
        "__eq__",
        "__floordiv__",
        "__ge__",
        "__gt__",
        "__le__",
        "__lshift__",
        "__lt__",
        "__matmul__",
        "__mod__",
        "__mul__",
        "__ne__",
        "__or__",
        "__pow__",
        "__rshift__",
        "__sub__",
        "__truediv__",
        "__xor__",
    }
)
"""Operators that are overloaded on types"""


class BaseAliasMeta(type):
    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
    ) -> BaseAliasMeta:
        for op in OP_OVERLOADS:
            namespace.setdefault(
                op,
                lambda self, other, op=op: self.__infix_op__(op, other),
            )

        return super().__new__(mcls, name, bases, namespace)


class BaseAlias(metaclass=BaseAliasMeta):
    def __init__(self, origin: type[TypeClassProto], metadata: Expr) -> None:
        self.__gel_origin__ = origin
        self.__gel_metadata__ = metadata

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__gel_origin__(*args, **kwargs)

    def __mro_entries__(self, bases: tuple[type, ...]) -> tuple[type, ...]:
        return (self.__gel_origin__,)

    def __dir__(self) -> list[str]:
        return dir(self.__gel_origin__)

    def __instancecheck__(self, obj: object) -> bool:
        return isinstance(obj, self.__gel_origin__)

    def __subclasscheck__(self, cls: type) -> bool:
        return issubclass(cls, self.__gel_origin__)  # pyright: ignore [reportGeneralTypeIssues]

    def __repr__(self) -> str:
        origin = _utils.type_repr(self.__gel_origin__)
        metadata = repr(self.__gel_metadata__)
        return f"{_utils.type_repr(type(self))}[{origin}, {metadata}]"

    def __getattr__(self, attr: str) -> Any:
        if "__gel_origin__" in self.__dict__ and not _utils.is_dunder(attr):
            attrval = getattr(self.__gel_origin__, attr)
            if is_exprmethod(attrval):

                @functools.wraps(attrval)
                def wrapper(*args: Any, **kwargs: Any) -> Any:
                    return attrval(*args, __operand__=self, **kwargs)

                return wrapper
            else:
                return attrval
        else:
            raise AttributeError(attr)

    def __edgeql_qb_expr__(self) -> Expr:
        return self.__gel_metadata__

    def __infix_op__(self, op: str, operand: Any) -> Any:
        if op == "__eq__" and operand is self:
            return True

        this_operand = self.__gel_origin__
        other_operand = operand
        if isinstance(operand, BaseAlias):
            other_operand = operand.__gel_origin__

        type_class = this_operand.__gel_type_class__
        op_impl = getattr(type_class, op, None)
        if op_impl is None:
            t1 = _utils.type_repr(this_operand)
            t2 = _utils.type_repr(other_operand)
            raise TypeError(
                f"operation not supported between instances of {t1} and {t2}"
            )

        expr = op_impl(this_operand, other_operand)
        assert isinstance(expr, ExprAlias)
        metadata = expr.__gel_metadata__
        assert isinstance(metadata, InfixOp)
        metadata.lexpr = self.__edgeql_qb_expr__()
        if hasattr(operand, "__edgeql_qb_expr__"):
            metadata.rexpr = edgeql_qb_expr(operand)

        return expr

    def __edgeql__(self) -> tuple[type, str]:
        return self.__gel_origin__, toplevel_edgeql(self)


class PathAlias(BaseAlias):
    def __init__(self, origin: type, metadata: Path) -> None:
        assert isinstance(origin, type)
        self.__gel_origin__ = origin
        self.__gel_metadata__ = metadata
        if _typing_inspect.is_generic_alias(origin):
            real_origin = get_args(origin)[0]
        else:
            real_origin = origin
        proxied_dunders: Iterable[str] = (
            getattr(real_origin, "__gel_proxied_dunders__", ()) or ()
        )
        self.__gel_proxied_dunders__ = frozenset(proxied_dunders)

    def __getattr__(self, attr: str) -> Any:
        if not _utils.is_dunder(attr) or attr in self.__gel_proxied_dunders__:
            origin = self.__gel_origin__
            descriptor = _utils.maybe_get_descriptor(
                origin, attr, of_type=AbstractFieldDescriptor
            )
            if descriptor is not None:
                return descriptor.get(self)
            else:
                return getattr(origin, attr)
        else:
            raise AttributeError(attr)

    def __setattr__(self, attr: str, val: Any) -> None:
        if _utils.is_dunder(attr):
            super().__setattr__(attr, val)
        else:
            setattr(self.__gel_origin__, attr, val)


def AnnotatedPath(origin: type, metadata: Path) -> PathAlias:  # noqa: N802
    return PathAlias(origin, metadata)


class ExprAlias(BaseAlias):
    def __bool__(self) -> bool:
        return False


def AnnotatedExpr(origin: type[Any], metadata: Expr) -> ExprAlias:  # noqa: N802
    return ExprAlias(origin, metadata)
