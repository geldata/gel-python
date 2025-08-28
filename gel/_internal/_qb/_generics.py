# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Typing generics for the EdgeQL query builder."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    TypedDict,
    NoReturn,
    get_args,
)
from typing_extensions import Never


import contextvars
import dataclasses
import enum
import functools
import inspect
import types

from gel._internal import _dis_bool
from gel._internal import _edgeql
from gel._internal import _namespace
from gel._internal import _typing_inspect
from gel._internal import _utils

from ._abstract import AbstractDescriptor, AbstractFieldDescriptor
from ._expressions import (
    BinaryOp,
    Global,
    Path,
    Variable,
    get_object_type_splat,
    toplevel_edgeql,
)
from ._protocols import (
    TypeClassProto,
    assert_edgeql_qb_expr,
    edgeql_qb_expr,
    is_exprmethod,
)
from ._reflection import GelTypeMetadata

if TYPE_CHECKING:
    from collections.abc import Iterable

    from gel._internal._schemapath import SchemaPath
    from ._abstract import Expr


_OP_OVERLOADS = frozenset(
    {
        "__add__",
        "__and__",
        "__contains__",
        "__divmod__",
        "__eq__",
        "__floordiv__",
        "__ge__",
        "__getitem__",
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

_SWAPPED_OP_OVERLOADS = frozenset(
    {
        "__radd__",
        "__rand__",
        "__rfloordiv__",
        "__rlshift__",
        "__rmatmul__",
        "__rmod__",
        "__rmul__",
        "__ror__",
        "__rpow__",
        "__rrshift__",
        "__rsub__",
        "__rtruediv__",
        "__rxor__",
    }
)
"""Operators that are overloaded on types (swapped versions)"""


class _BinaryOpKwargs(TypedDict, total=False):
    lexpr: Expr
    rexpr: Expr
    op: _edgeql.Token
    type_: SchemaPath


SPECIAL_EXPR_METHODS = frozenset(
    {
        "__gel_assert_single__",
    }
)

OPERAND_IS_ALIAS: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "GEL_EXPR_OPERAND_IS_EXPR", default=False
)


class _Op(enum.Enum):
    IS_NONE = enum.auto()
    IS_NOT_NONE = enum.auto()
    IN_NOT_IN = enum.auto()


_OP_ERROR_MAP: dict[_Op | _dis_bool.BoolOp, str] = {
    _dis_bool.BoolOp.UNKNOWN: (
        "Boolean conversion not supported; use std functions instead"
    ),
    _dis_bool.BoolOp.EXPLICIT_BOOL: (
        "Boolean conversion with bool() is not supported; "
        "use std.exists(...) instead"
    ),
    _dis_bool.BoolOp.IMPLICIT_BOOL: (
        "Boolean conversion is not supported; use std.if_(...) for "
        "conditionals or std.exists(...) for existence checks"
    ),
    _dis_bool.BoolOp.NOT: (
        "Boolean 'not' operation is not supported; use std.not_(...) instead"
    ),
    _dis_bool.BoolOp.AND: (
        "Boolean 'and' operation is not supported; use std.and_(...) instead"
    ),
    _dis_bool.BoolOp.OR: (
        "Boolean 'or' operation is not supported; use std.or_(...) instead"
    ),
    _Op.IS_NONE: (
        "Comparison with None is not supported; "
        "use std.not_(std.exists(...)) instead"
    ),
    _Op.IS_NOT_NONE: (
        "Comparison with None is not supported; use std.exists(...) instead"
    ),
    _Op.IN_NOT_IN: (
        "'in/not in' operation is not supported; use std.in_(...) "
        "or std.not_in(...) instead"
    ),
}


def _raise_op_error(op: _Op | _dis_bool.BoolOp) -> NoReturn:
    __tracebackhide__ = True
    msg = _OP_ERROR_MAP.get(op)
    if msg is None:
        # This should not happen, _OP_ERROR_MAP should be exhaustive
        msg = (
            f"unsupported query builder operator: {op}; "
            f"if you see this error, please open an issue "
            f"here: https://github.com/geldata/gel-python/issues/new"
            f"?template=bug_report.md"
        )
        raise AssertionError(msg)
    # incorrect query builder expression
    raise TypeError(msg)


class CheckedBoolOpType:
    def __bool__(self) -> Never:  # noqa: PLE0304
        _raise_op_error(_dis_bool.guess_bool_op())


class CheckedContainsOpType:
    def __contains__(self, other: Any) -> Never:
        _raise_op_error(_Op.IN_NOT_IN)


class CheckedIsNoneType:
    def __gel_is__(self, other: Any) -> bool:
        if other is None:
            _raise_op_error(_Op.IS_NONE)
        else:
            return self is other

    def __gel_is_not__(self, other: Any) -> bool:
        if other is None:
            _raise_op_error(_Op.IS_NOT_NONE)
        else:
            return self is not other


class BaseAliasMeta(type):
    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
    ) -> BaseAliasMeta:
        for op in _OP_OVERLOADS:
            namespace.setdefault(
                op,
                lambda self, other, op=op: self.__infix_op__(op, other),
            )

        for op in _SWAPPED_OP_OVERLOADS:
            namespace.setdefault(
                op,
                lambda self, other, op=op: self.__infix_op__(
                    op, other, swapped=True
                ),
            )

        return super().__new__(mcls, name, bases, namespace)


class BaseAlias(
    CheckedBoolOpType,
    CheckedContainsOpType,
    CheckedIsNoneType,
    metaclass=BaseAliasMeta,
):
    def __init__(self, origin: type[TypeClassProto], metadata: Expr) -> None:
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
        if attr == "link":
            # `default.MyType.linkname.link()` is a very common operation:
            # it can be called millions of times when loading data and
            # the descriptor lookup with the subsequent `.get()` slow down
            # instance creation ~40%.
            #
            # Given that "link" is a reserved EdgeQL keyword, we can just
            # resolve it to `ProxyModel.link()` as fast as possible.
            try:
                method = type.__getattribute__(self.__gel_origin__, "link")
            except AttributeError:
                # Alright, let's try the descriptor lookup.
                pass
            else:
                if isinstance(method, types.MethodType):
                    # Got it -- cache it so it's even faster next time.
                    object.__setattr__(self, "link", method)
                    return method

        if (
            not _namespace.is_dunder(attr)
            or attr in self.__gel_proxied_dunders__
            or attr in SPECIAL_EXPR_METHODS
        ):
            origin = self.__gel_origin__
            descriptor = inspect.getattr_static(origin, attr, None)
            if isinstance(descriptor, AbstractFieldDescriptor):
                return descriptor.get(origin, self)
            else:
                attrval = getattr(origin, attr)
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

    def __infix_op__(
        self,
        op: str,
        operand: Any,
        *,
        swapped: bool = False,
    ) -> Any:
        if op == "__eq__" and operand is self:
            return True

        # Check for None comparison and raise appropriate error
        if operand is None and op in {"__eq__", "__ne__"}:
            _raise_op_error(_Op.IS_NONE if op == "__eq__" else _Op.IS_NOT_NONE)

        this_operand = self.__gel_origin__
        other_operand = operand
        if isinstance(operand, BaseAlias):
            other_operand = operand.__gel_origin__
            operand_is_alias = True
        else:
            operand_is_alias = False

        type_class = this_operand.__gel_type_class__
        op_impl = getattr(type_class, op, None)
        if op_impl is None:
            t1 = _utils.type_repr(this_operand)
            t2 = _utils.type_repr(other_operand)
            raise TypeError(
                f"operation not supported between instances of {t1} and {t2}"
            )

        cvar_token = OPERAND_IS_ALIAS.set(operand_is_alias)
        try:
            expr = op_impl(this_operand, other_operand)
        finally:
            OPERAND_IS_ALIAS.reset(cvar_token)
        assert isinstance(expr, ExprAlias)
        metadata = expr.__gel_metadata__
        assert isinstance(metadata, BinaryOp)
        self_expr = edgeql_qb_expr(self)

        if isinstance(operand, BaseAlias):
            other_expr = assert_edgeql_qb_expr(operand)
        else:
            other_expr = metadata.lexpr if swapped else metadata.rexpr

        replacements: _BinaryOpKwargs = {}
        if swapped:
            replacements["rexpr"] = self_expr
            replacements["lexpr"] = other_expr
        else:
            replacements["lexpr"] = self_expr
            replacements["rexpr"] = other_expr

        expr.__gel_metadata__ = dataclasses.replace(metadata, **replacements)

        return expr

    def __edgeql__(self) -> tuple[type, tuple[str, dict[str, object]]]:
        type_ = self.__gel_origin__
        if issubclass(type_, GelTypeMetadata):
            splat_cb = functools.partial(get_object_type_splat, type_)
        else:
            splat_cb = None
        return type_, toplevel_edgeql(self, splat_cb=splat_cb)


class PathAlias(BaseAlias):
    pass


def AnnotatedPath(origin: type, metadata: Path) -> PathAlias:  # noqa: N802
    return PathAlias(origin, metadata)


class ExprAlias(BaseAlias):
    pass


def AnnotatedExpr(origin: type[Any], metadata: Expr) -> ExprAlias:  # noqa: N802
    return ExprAlias(origin, metadata)


class SortAlias(BaseAlias):
    pass


class VarAlias(BaseAlias):
    pass


def AnnotatedVar(origin: type[Any], metadata: Variable) -> VarAlias:  # noqa: N802
    return VarAlias(origin, metadata)


class GlobalAlias(BaseAlias):
    pass


def AnnotatedGlobal(origin: type[Any], metadata: Global) -> GlobalAlias:  # noqa: N802
    return GlobalAlias(origin, metadata)


def is_pointer_descriptor(v: Any) -> bool:
    return isinstance(v, (AbstractDescriptor, PathAlias)) or (
        _typing_inspect.is_annotated(v)
        and isinstance(v.__origin__, AbstractDescriptor)
    )
