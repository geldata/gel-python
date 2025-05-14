# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Protocol,
    TypeGuard,
    TypeVar,
    cast,
    final,
    overload,
)

from typing_extensions import (
    ParamSpec,
    Self,
    TypeAliasType,
    TypeVarTuple,
    Unpack,
)

import abc
import builtins
import dataclasses
import decimal
import functools
import sys
import textwrap
import typing
import uuid
import warnings
import weakref

import pydantic
import pydantic.fields
import pydantic_core
from pydantic import ConfigDict as ConfigDict
from pydantic import Field
from pydantic import PrivateAttr as PrivateAttr
from pydantic import computed_field as computed_field
from pydantic import field_serializer as field_serializer
from pydantic_core import core_schema as pydantic_schema

from pydantic._internal import _model_construction  # noqa: PLC2701
from pydantic._internal import _namespace_utils  # noqa: PLC2701

from gel.datatypes import range as range_t
from gel._internal import _edgeql
from gel._internal import _typing_eval
from gel._internal import _typing_inspect
from gel._internal import _typing_dispatch
from gel._internal import _typing_parametric as parametric
from gel._internal import _polyfills
from gel._internal._hybridmethod import hybridmethod

from gel._internal._reflection import SchemaPath as SchemaPath  # noqa: TC001

from . import lists
from . import unsetid

if TYPE_CHECKING:
    import enum

    from collections.abc import (
        Callable,
        Sequence,
    )


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


@final
class UnspecifiedType:
    """A type used as a sentinel for unspecified values."""


Unspecified = UnspecifiedType()


class InstanceSupportsEdgeQL(Protocol):
    def __edgeql__(self) -> str: ...


class TypeSupportsEdgeQL(Protocol):
    @classmethod
    def __edgeql__(cls) -> str: ...


SupportsEdgeQL = TypeAliasType(
    "SupportsEdgeQL",
    InstanceSupportsEdgeQL | type[TypeSupportsEdgeQL],
)


dispatch_overload = _typing_dispatch.dispatch_overload


class GelClassVar:
    pass


def _is_dunder(attr: str) -> bool:
    return attr.startswith("__") and attr.endswith("__")


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


PROXIED_DUNDERS = frozenset(
    {
        "__linkprops__",
    }
)


def _module_ns_of(obj: object) -> dict[str, Any]:
    """Return the namespace of the module where *obj* is defined."""
    module_name = getattr(obj, "__module__", None)
    if module_name:
        module = sys.modules.get(module_name)
        if module is not None:
            return module.__dict__

    return {}


def _type_repr(t: type[Any]) -> str:
    if isinstance(t, type):
        if t.__module__ == "builtins":
            return t.__qualname__
        else:
            return f"{t.__module__}.{t.__qualname__}"
    else:
        return repr(t)


def _get_field_descriptor(cls: type, name: str) -> GelFieldDescriptor | None:
    for ancestor in cls.__mro__:
        desc = ancestor.__dict__.get(name, Unspecified)
        if desc is not Unspecified and isinstance(desc, GelFieldDescriptor):
            return desc

    return None


class _BaseAliasMeta(type):
    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
    ) -> _BaseAliasMeta:
        for op in OP_OVERLOADS:
            namespace.setdefault(
                op,
                lambda self, other, op=op: self.__infix_op__(op, other),
            )

        return super().__new__(mcls, name, bases, namespace)


class _BaseAlias(metaclass=_BaseAliasMeta):
    def __init__(self, origin: type[GelType]) -> None:
        self.__gel_origin__ = origin

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__gel_origin__(*args, **kwargs)

    def __mro_entries__(self, bases: tuple[type, ...]) -> tuple[type, ...]:
        return (self.__gel_origin__,)

    def __dir__(self) -> list[str]:
        return dir(self.__gel_origin__)

    def __instancecheck__(self, obj: object) -> bool:
        return isinstance(obj, self.__gel_origin__)

    def __subclasscheck__(self, cls: type) -> bool:
        return issubclass(cls, self.__gel_origin__)

    def __getattr__(self, attr: str) -> Any:
        if "__gel_origin__" in self.__dict__ and not _is_dunder(attr):
            attrval = getattr(self.__gel_origin__, attr)
            if _is_gel_expr_method(attrval):

                @functools.wraps(attrval)
                def wrapper(*args: Any, **kwargs: Any) -> Any:
                    return attrval(*args, __operand__=self, **kwargs)

                return wrapper
            else:
                return attrval
        else:
            raise AttributeError(attr)

    def __edgeql_qb_expr__(self) -> Expr:
        raise NotImplementedError(
            f"{self.__class__.__name__}.__edgeql_qb_expr__"
        )

    def __infix_op__(self, op: str, operand: Any) -> Any:
        if op == "__eq__" and operand is self:
            return True

        this_operand = self.__gel_origin__
        other_operand = operand
        if isinstance(operand, _BaseAlias):
            other_operand = operand.__gel_origin__

        meta_impl = this_operand.__type_meta_impl__
        op_impl = getattr(meta_impl, op, None)
        if op_impl is None:
            t1 = _type_repr(this_operand)
            t2 = _type_repr(other_operand)
            raise TypeError(
                f"operation not supported between instances of {t1} and {t2}"
            )

        expr = op_impl(this_operand, other_operand)
        assert isinstance(expr, _ExprAlias)
        metadata = expr.__gel_metadata__
        assert isinstance(metadata, InfixOp)
        metadata.lexpr = self.__edgeql_qb_expr__()
        if hasattr(operand, "__edgeql_qb_expr__"):
            metadata.rexpr = operand.__edgeql_qb_expr__()
        else:
            raise NotImplementedError(
                f"unsupported infix iop type: {operand!r}"
            )
        return expr


class _PathAlias(_BaseAlias):
    def __init__(self, origin: type[GelType], metadata: Path) -> None:
        super().__init__(origin)
        self.__gel_metadata__ = metadata

    def __edgeql_qb_expr__(self) -> Expr:
        return self.__gel_metadata__

    def __getattr__(self, attr: str) -> Any:
        if "__gel_origin__" in self.__dict__ and (
            not _is_dunder(attr) or attr in PROXIED_DUNDERS
        ):
            origin = self.__gel_origin__
            descriptor = _get_field_descriptor(origin, attr)
            if descriptor is not None:
                return descriptor.get(self)
            else:
                return getattr(origin, attr)
        else:
            raise AttributeError(attr)

    def __setattr__(self, attr: str, val: Any) -> None:
        if _is_dunder(attr):
            super().__setattr__(attr, val)
        else:
            setattr(self.__gel_origin__, attr, val)

    def __repr__(self) -> str:
        origin = _type_repr(self.__gel_origin__)
        metadata = repr(self.__gel_metadata__)
        return f"gel.models.PathAlias[{origin}, {metadata}]"


def AnnotatedPath(origin: type, metadata: Path) -> _PathAlias:  # noqa: N802
    return _PathAlias(origin, metadata)


class _ExprAlias(_BaseAlias):
    def __init__(self, origin: type[GelType], metadata: Expr) -> None:
        super().__init__(origin)
        self.__gel_metadata__ = metadata

    def __edgeql_qb_expr__(self) -> Expr:
        return self.__gel_metadata__

    def __repr__(self) -> str:
        origin = _type_repr(self.__gel_origin__)
        metadata = repr(self.__gel_metadata__)
        return f"gel.models.ExprAlias[{origin}, {metadata}]"

    def __bool__(self) -> bool:
        return False

    def __edgeql__(self) -> str:
        return self.__gel_metadata__.__edgeql__()


def AnnotatedExpr(origin: type[GelType], metadata: Expr) -> _ExprAlias:  # noqa: N802
    return _ExprAlias(origin, metadata)


PathSource = TypeAliasType("PathSource", "Symbol | SchemaSet | Path")


class GelFieldDescriptor(GelClassVar):
    __slots__ = (
        "__gel_annotation__",
        "__gel_name__",
        "__gel_origin__",
        "__gel_resolved_type__",
    )

    @classmethod
    def _from_pydantic_field(
        cls,
        origin: type,
        name: str,
        field: pydantic.fields.FieldInfo,
    ) -> Self:
        if field.annotation is None:
            raise AssertionError(
                f"unexpected unnannotated model field: {name}"
            )
        return cls(origin, name, field.annotation)

    def __init__(self, origin: type, name: str, annotation: type[Any]) -> None:
        self.__gel_origin__ = origin
        self.__gel_name__ = name
        self.__gel_annotation__ = annotation
        self.__gel_resolved_type__ = None

    def __repr__(self) -> str:
        qualname = f"{self.__gel_origin__.__qualname__}.{self.__gel_name__}"
        anno = self.__gel_annotation__
        return f"<{self.__class__.__name__} {qualname}: {anno}>"

    def __set_name__(self, owner: Any, name: str) -> None:
        self.__gel_name__ = name

    def _try_resolve_type(self) -> Any:
        origin = self.__gel_origin__
        globalns = _module_ns_of(origin)
        ns = _namespace_utils.NsResolver(
            parent_namespace=getattr(
                origin,
                "__pydantic_parent_namespace__",
                None,
            ),
        )
        with ns.push(origin):
            globalns, localns = ns.types_namespace

        t = _typing_eval.try_resolve_type(
            self.__gel_annotation__,
            owner=origin,
            globals=globalns,
            locals=localns,
        )
        if (
            t is not None
            and _typing_inspect.is_generic_alias(t)
            and issubclass(typing.get_origin(t), BasePointer)
        ):
            t = typing.get_args(t)[0]
            if not isinstance(
                t, type
            ) and not _typing_inspect.is_generic_alias(t):
                raise AssertionError(
                    f"BasePointer type argument is not a type: {t}"
                )

        return t

    def get(
        self,
        owner: type[Any] | _PathAlias,
    ) -> Any:
        t = self.__gel_resolved_type__
        if t is None:
            t = self._try_resolve_type()
            if t is not None:
                self.__gel_resolved_type__ = t

        if t is None:
            return self
        else:
            source: PathSource
            if isinstance(owner, _PathAlias):
                source = owner.__gel_metadata__
            elif isinstance(owner, type) and issubclass(
                owner, GelModelMetadata
            ):
                source = SchemaSet(name=owner.__reflection__.name)
            else:
                return t
            metadata = Path(
                source=source,
                name=self.__gel_name__,
                is_lprop=False,
            )
            return AnnotatedPath(t, metadata)

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
    ) -> Any:
        if instance is not None:
            return self
        else:
            assert owner is not None
            return self.get(owner)


class Exclusive:
    pass


class GelModelMetadata:
    class __reflection__:  # noqa: N801
        id: ClassVar[uuid.UUID]
        name: ClassVar[SchemaPath]


class ExprCompatibleInstance(Protocol):
    def __edgeql_qb_expr__(self) -> Expr: ...


class ExprCompatibleType(Protocol):
    @classmethod
    def __edgeql__(cls) -> str: ...


ExprCompatible = TypeAliasType(
    "ExprCompatible",
    ExprCompatibleInstance | type[ExprCompatibleType],
)


def edgeql(source: SupportsEdgeQL | ExprCompatible) -> str:
    try:
        __edgeql__ = source.__edgeql__  # type: ignore [union-attr]
    except AttributeError:
        try:
            __edgeql_qb_expr__ = source.__edgeql_qb_expr__  # type: ignore [union-attr]
        except AttributeError:
            raise TypeError(
                f"{type(source)} does not support __edgeql__ protocol"
            ) from None
        else:
            expr = __edgeql_qb_expr__()
            __edgeql__ = expr.__edgeql__

    if not callable(__edgeql__):
        raise TypeError(f"{type(source)}.__edgeql__ is not callable")

    value = __edgeql__()
    if not isinstance(value, str):
        raise ValueError("{type(source)}.__edgeql__()")
    return value


def _as_expr(x: ExprCompatible) -> Expr:
    if isinstance(x, Expr):
        return x

    as_expr = getattr(x, "__edgeql_qb_expr__", None)
    if as_expr is None:
        raise TypeError(
            f"{_type_repr(type(x))} cannot be converted to an Expr"
        )
    expr = as_expr()
    if not isinstance(expr, Expr):
        raise ValueError(
            f"{_type_repr(type(x))}.__edgeql_qb_expr__ did not "
            f"return an Expr instance"
        )
    return expr


class Expr(abc.ABC):
    @abc.abstractproperty
    def precedence(self) -> _edgeql.Precedence: ...

    @abc.abstractmethod
    def __edgeql__(self) -> str: ...

    def __edgeql_qb_expr__(self) -> Self:
        return self


class ExprPlaceholder(Expr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        raise TypeError("unreplaced ExprPlaceholder")

    def __edgeql__(self) -> str:
        raise TypeError("unreplaced ExprPlaceholder")


class IdentLikeExpr(Expr):
    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.IDENT]


class Symbol(IdentLikeExpr):
    pass


class PathPrefix(Symbol):
    def __edgeql__(self) -> str:
        return ""


@dataclasses.dataclass(frozen=True)
class SchemaSet(IdentLikeExpr):
    name: SchemaPath

    def __edgeql__(self) -> str:
        return "::".join(self.name.parts)


class Literal(IdentLikeExpr):
    pass


@dataclasses.dataclass(frozen=True)
class BoolLiteral(Literal):
    val: bool

    def __edgeql__(self) -> str:
        return "true" if self.val else "false"


@dataclasses.dataclass(frozen=True)
class IntLiteral(Literal):
    val: int

    def __edgeql__(self) -> str:
        return str(self.val)


@dataclasses.dataclass(frozen=True)
class FloatLiteral(Literal):
    val: float

    def __edgeql__(self) -> str:
        return str(self.val)


@dataclasses.dataclass(frozen=True)
class BigIntLiteral(Literal):
    val: int

    def __edgeql__(self) -> str:
        return f"n{self.val}"


@dataclasses.dataclass(frozen=True)
class DecimalLiteral(Literal):
    val: decimal.Decimal

    def __edgeql__(self) -> str:
        return f"n{self.val}"


@dataclasses.dataclass(frozen=True)
class BytesLiteral(Literal):
    val: bytes

    def __edgeql__(self) -> str:
        v = _edgeql.quote_literal(repr(self.val)[2:-1])
        return f"b{v}"


@dataclasses.dataclass(frozen=True)
class StringLiteral(Literal):
    val: str

    def __edgeql__(self) -> str:
        return _edgeql.quote_literal(self.val)


@dataclasses.dataclass(kw_only=True)
class Path(Expr):
    source: Symbol | SchemaSet | Path
    name: str
    is_lprop: bool

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.PATH]

    def __edgeql__(self) -> str:
        steps = []
        current: PathSource = self
        while isinstance(current, Path):
            steps.append(current.name)
            current = current.source

        if not isinstance(current, (SchemaSet, Symbol)):
            raise AssertionError(
                "Path does not start with a SourceSet or a Symbol"
            )
        steps.append(current.__edgeql__())

        return ".".join(reversed(steps))


@dataclasses.dataclass(kw_only=True)
class Op(Expr):
    op: _edgeql.Token

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[self.op]


@dataclasses.dataclass(kw_only=True)
class PrefixOp(Op):
    expr: Expr

    def __init__(
        self,
        *,
        expr: ExprCompatible,
        op: _edgeql.Token | str,
    ) -> None:
        self.expr = _as_expr(expr)
        if isinstance(op, str):
            op = _edgeql.Token.from_str(op)
        self.op = op

    def __edgeql__(self) -> str:
        return f"{self.op} {edgeql(self.expr)}"


@dataclasses.dataclass(kw_only=True)
class InfixOp(Op):
    lexpr: Expr
    rexpr: Expr

    def __init__(
        self,
        *,
        lexpr: ExprCompatible,
        rexpr: ExprCompatible,
        op: _edgeql.Token | str,
    ) -> None:
        self.lexpr = _as_expr(lexpr)
        self.rexpr = _as_expr(rexpr)
        if isinstance(op, str):
            op = _edgeql.Token.from_str(op)
        self.op = op

    def __edgeql__(self) -> str:
        left = edgeql(self.lexpr)
        if self._need_left_parens():
            left = f"({left})"
        right = edgeql(self.rexpr)
        if self._need_right_parens():
            right = f"({right})"
        return f"{left} {self.op} {right}"

    def _need_left_parens(self) -> bool:
        lexpr = self.lexpr
        left_prec = lexpr.precedence.value
        self_prec = self.precedence.value
        self_assoc = self.precedence.assoc

        return left_prec < self_prec or (
            left_prec == self_prec
            and self_assoc is not _edgeql.Assoc.RIGHT
            and (not isinstance(lexpr, InfixOp) or lexpr.op != self.op)
        )

    def _need_right_parens(self) -> bool:
        rexpr = self.rexpr
        right_prec = rexpr.precedence.value
        self_prec = self.precedence.value
        self_assoc = self.precedence.assoc

        return right_prec < self_prec or (
            right_prec == self_prec and self_assoc is _edgeql.Assoc.RIGHT
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class FuncCall(Expr):
    fname: str
    args: list[Expr]
    kwargs: dict[str, Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Operation.CALL]

    def __edgeql__(self) -> str:
        args = ", ".join(
            [
                *(edgeql(arg) for arg in self.args),
                *(f"{n} := {edgeql(v)}" for n, v in self.kwargs.items()),
            ]
        )

        return f"{self.fname}({args})"


@dataclasses.dataclass(frozen=True, kw_only=True)
class Filter(Expr):
    expr: Any
    filters: list[Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.FILTER]

    def __edgeql__(self) -> str:
        fexpr = self.filters[0]
        for item in self.filters[1:]:
            fexpr = InfixOp(lexpr=fexpr, op=_edgeql.Token.AND, rexpr=item)
        fexpr = InfixOp(lexpr=self.expr, op=_edgeql.Token.FILTER, rexpr=fexpr)
        return edgeql(fexpr)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Shape(Expr):
    expr: Any
    elements: dict[str, Expr]

    @property
    def precedence(self) -> _edgeql.Precedence:
        return _edgeql.PRECEDENCE[_edgeql.Token.LBRACE]

    def __edgeql__(self) -> str:
        els = (f"{n} := {edgeql(ex)}" for n, ex in self.elements.items())
        shape = "{\n" + textwrap.indent("\n".join(els), "  ") + "\n}"
        return f"{edgeql(self.expr)} {shape}"


GelType_T = TypeVar("GelType_T", bound="GelType")


if TYPE_CHECKING:

    class GelTypeMeta(type):
        def __edgeql_qb_expr__(cls) -> Expr: ...

    class GelType(GelClassVar, metaclass=GelTypeMeta):
        __type_meta_impl__: ClassVar[type]

        def __edgeql_qb_expr__(self) -> Expr: ...

        @staticmethod
        def __edgeql__() -> str: ...
else:
    GelTypeMeta = type

    class GelType(GelClassVar):
        __type_meta_impl__: ClassVar[type]

        @hybridmethod
        def __edgeql_qb_expr__(self) -> Expr:
            if isinstance(self, type):
                return ExprPlaceholder()
            else:
                return self.__edgeql_literal__()

        def __edgeql_literal__(self) -> Literal:
            raise NotImplementedError(
                f"{type(self).__name__}.__edgeql_literal__"
            )


class GelPrimitiveType(GelType):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[Self]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> Self: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[Self] | Self: ...


class BaseScalar(GelPrimitiveType):
    pass


if TYPE_CHECKING:
    from typing import NamedTupleMeta  # type: ignore [attr-defined]

    class AnyTupleMeta(NamedTupleMeta, GelTypeMeta):  # type: ignore [misc]
        ...
else:
    AnyTupleMeta = type(GelPrimitiveType)


class AnyTuple(GelPrimitiveType, metaclass=AnyTupleMeta):
    pass


if TYPE_CHECKING:

    class AnyEnumMeta(enum.EnumMeta, GelTypeMeta):
        pass
else:
    AnyEnumMeta = type(_polyfills.StrEnum)


class AnyEnum(BaseScalar, _polyfills.StrEnum, metaclass=AnyEnumMeta):
    pass


if TYPE_CHECKING:

    class _ArrayMeta(GelTypeMeta, typing._ProtocolMeta):
        pass
else:
    _ArrayMeta = type(list)


class Array(list[T], GelPrimitiveType, metaclass=_ArrayMeta):
    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: Array[T] | Sequence[T]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return pydantic_schema.list_schema(
                items_schema=handler.generate_schema(item_type),
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)


if TYPE_CHECKING:

    class _TupleMeta(GelTypeMeta, typing._ProtocolMeta):
        pass
else:
    _TupleMeta = type(tuple)


Ts = TypeVarTuple("Ts")


class Tuple(tuple[Unpack[Ts]], GelPrimitiveType, metaclass=_TupleMeta):
    __slots__ = ()

    if TYPE_CHECKING:

        def __set__(self, obj: Any, value: Tuple[T] | Sequence[T]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return pydantic_schema.tuple_schema(
                items_schema=[handler.generate_schema(arg) for arg in args],
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    tuple,
                ),
            )
        else:
            return handler.generate_schema(source_type)


def _get_range_pydantic_schema(
    source_type: Any,
    handler: pydantic.GetCoreSchemaHandler,
) -> pydantic_schema.ModelFieldsSchema:
    args = typing.get_args(source_type)
    item_schema = handler.generate_schema(args[0])
    opt_item_schema = pydantic_schema.nullable_schema(item_schema)
    item_field_schema = pydantic_schema.model_field(opt_item_schema)
    bool_schema = pydantic_schema.bool_schema()
    bool_field_schema = pydantic_schema.model_field(bool_schema)
    return pydantic_schema.model_fields_schema(
        {
            "lower": item_field_schema,
            "upper": item_field_schema,
            "inc_lower": bool_field_schema,
            "inc_upper": bool_field_schema,
            "empty": bool_field_schema,
        }
    )


if TYPE_CHECKING:

    class _RangeMeta(GelTypeMeta):
        pass
else:
    _RangeMeta = type


class Range(range_t.Range[T], GelPrimitiveType, metaclass=_RangeMeta):
    if TYPE_CHECKING:

        def __set__(
            self, obj: Any, value: Range[T] | range_t.Range[T]
        ) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            return _get_range_pydantic_schema(source_type, handler)
        else:
            return handler.generate_schema(source_type)


if TYPE_CHECKING:

    class _MultiRangeMeta(GelTypeMeta):
        pass
else:
    _MultiRangeMeta = type


class MultiRange(
    GelPrimitiveType,
    Generic[T],
    metaclass=_MultiRangeMeta,
):
    if TYPE_CHECKING:

        def __set__(
            self,
            obj: Any,
            value: MultiRange[T] | range_t.MultiRange[T],
        ) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            range_schema = _get_range_pydantic_schema(source_type, handler)
            return pydantic_schema.list_schema(range_schema)
        else:
            return handler.generate_schema(source_type)


class PyTypeScalar(parametric.SingleParametricType[T_co]):
    if TYPE_CHECKING:

        def __init__(self, val: Any) -> None: ...
        def __set__(self, obj: Any, value: T_co) -> None: ...  # type: ignore [misc]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        return pydantic_core.core_schema.no_info_after_validator_function(
            cls.type,
            handler(cls.type),
        )

    def __edgeql_literal__(self) -> Literal:
        cls = type(self)
        match cls.type:
            case builtins.bool:
                return BoolLiteral(bool(self))
            case builtins.int:
                return IntLiteral(self)  # type: ignore [arg-type]
            case builtins.float:
                return FloatLiteral(self)  # type: ignore [arg-type]
            case builtins.str:
                return StringLiteral(self)  # type: ignore [arg-type]
            case decimal.Decimal:
                return DecimalLiteral(self)  # type: ignore [arg-type]

        raise NotImplementedError(f"{cls}.__edgeql_literal__")


class GelModelMeta(_model_construction.ModelMetaclass, GelTypeMeta):
    __gel_class_registry__: ClassVar[
        weakref.WeakValueDictionary[uuid.UUID, type[Any]]
    ] = weakref.WeakValueDictionary()

    def __new__(
        mcls,
        name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        *,
        __gel_type_id__: uuid.UUID | None = None,
        **kwargs: Any,
    ) -> GelModelMeta:
        with warnings.catch_warnings():
            # Make pydantic shut up about attribute redefinition.
            warnings.filterwarnings(
                "ignore",
                message=r".*shadows an attribute in parent.*",
            )
            cls = cast(
                "type[pydantic.BaseModel]",
                super().__new__(mcls, name, bases, namespace, **kwargs),
            )

        for fname, field in cls.__pydantic_fields__.items():
            if fname in cls.__annotations__:
                col = GelFieldDescriptor._from_pydantic_field(
                    cls, fname, field
                )
                setattr(cls, fname, col)

        if __gel_type_id__ is not None:
            mcls.__gel_class_registry__[__gel_type_id__] = cls

        return cls  # type: ignore [return-value]

    def __setattr__(cls, name: str, value: Any, /) -> None:
        if name == "__pydantic_fields__":
            fields: dict[str, pydantic.fields.FieldInfo] = value
            for field in fields.values():
                fdef = field.default
                if isinstance(fdef, (GelClassVar, _PathAlias)) or (
                    _typing_inspect.is_annotated(fdef)
                    and isinstance(fdef.__origin__, GelClassVar)
                ):
                    field.default = pydantic_core.PydanticUndefined

        super().__setattr__(name, value)

    @classmethod
    def get_class_by_id(cls, tid: uuid.UUID) -> type[GelModel]:
        try:
            return cls.__gel_class_registry__[tid]
        except KeyError:
            raise LookupError(
                f"cannot find GelModel for object type id {tid}"
            ) from None


P = ParamSpec("P")
R = TypeVar("R")


def exprmethod(func: Callable[P, R]) -> Callable[P, R]:
    actual_func: Callable[P, R] = getattr(func, "__func__", func)
    actual_func.__gel_expr_method__ = True  # type: ignore [attr-defined]
    return func


def _is_gel_expr_method(obj: Any) -> TypeGuard[Callable[..., Any]]:
    if hasattr(obj, "__gel_expr_method__"):
        return True
    func = getattr(obj, "__func__", None)
    if func is not None:
        return hasattr(func, "__gel_expr_method__")
    return False


class GelModel(
    pydantic.BaseModel,
    GelModelMetadata,
    GelType,
    metaclass=GelModelMeta,
):
    model_config = pydantic.ConfigDict(
        json_encoders={uuid.UUID: str},
        validate_assignment=True,
        defer_build=True,
    )

    _p__id__: uuid.UUID = PrivateAttr()

    def __init__(self, /, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._p__id: uuid.UUID = unsetid.UNSET_UUID
        self._p____type__ = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GelModel):
            return NotImplemented

        if self._p__id is None or other._p__id is None:
            return False
        else:
            return self._p__id == other._p__id

    def __hash__(self) -> int:
        if self._p__id is unsetid.UNSET_UUID:
            raise TypeError("Model instances without id value are unhashable")

        return hash(self._p__id)

    if TYPE_CHECKING:

        @classmethod
        def select(cls, /, **kwargs: bool | type[GelType]) -> type[Self]: ...
    else:

        @exprmethod
        @classmethod
        def select(
            cls,
            /,
            __operand__: _ExprAlias | None = None,
            **kwargs: bool | type[GelType],
        ) -> type[Self]:
            shape = {}

            for ptrname, kwarg in kwargs.items():
                if isinstance(kwarg, bool):
                    ptr = getattr(cls, ptrname, Unspecified)
                    if ptr is Unspecified and isinstance(kwarg, bool):
                        sn = cls.__reflection__.name.as_schema_name()
                        msg = f"{ptrname} is not a valid {sn} property"
                        raise AttributeError(msg)
                    if not isinstance(ptr, _PathAlias):
                        raise AssertionError(
                            f"expected {cls.__name__}.{ptrname} "
                            f"to be a PathAlias"
                        )

                    if kwarg:
                        ptr.__gel_metadata__.source = PathPrefix()
                        shape[ptrname] = ptr
                else:
                    shape[ptrname] = kwarg

            operand: type[Self] | Expr

            if __operand__ is not None:
                operand = __operand__.__gel_metadata__
            else:
                operand = cls

            expr = Shape(expr=operand, elements=shape)

            return AnnotatedExpr(  # type: ignore [return-value]
                cls,
                expr,
            )

    if TYPE_CHECKING:

        @classmethod
        def filter(
            cls,
            /,
            *exprs: Any,
            **properties: Any,
        ) -> type[Self]: ...
    else:

        @exprmethod
        @classmethod
        def filter(
            cls,
            /,
            *exprs: Any,
            __operand__: _ExprAlias | None = None,
            **properties: Any,
        ) -> type[Self]:
            all_exprs = list(exprs)

            for propname, value in properties.items():
                prop = getattr(cls, propname, Unspecified)
                if prop is Unspecified:
                    sn = cls.__reflection__.name.as_schema_name()
                    msg = f"{propname} is not a valid {sn} property"
                    raise AttributeError(msg)
                if not isinstance(prop, _PathAlias):
                    raise AssertionError(
                        f"expected {cls.__name__}.{propname} to be a PathAlias"
                    )
                prop.__gel_metadata__.source = PathPrefix()
                prop_comp = prop == value
                if not isinstance(prop_comp, _ExprAlias):
                    raise AssertionError(
                        f"comparing {prop} to {value} did not produce "
                        "a Gel expression type"
                    )
                all_exprs.append(prop_comp)

            operand: type[Self] | Expr

            if __operand__ is not None:
                operand = __operand__.__gel_metadata__
            else:
                operand = cls

            if isinstance(operand, Filter):
                expr = Filter(
                    expr=operand.expr, filters=operand.filters + all_exprs
                )
            else:
                expr = Filter(expr=operand, filters=all_exprs)

            return AnnotatedExpr(  # type: ignore [return-value]
                cls,
                expr,
            )

    @classmethod
    def __edgeql__(cls) -> str:  # pyright: ignore [reportIncompatibleMethodOverride]
        return cls.__reflection__.name.as_schema_name()


class LinkPropsDescriptor(Generic[T_co]):
    @overload
    def __get__(self, obj: None, owner: type[Any]) -> type[T_co]: ...

    @overload
    def __get__(self, obj: object, owner: type[Any] | None = None) -> T_co: ...

    def __get__(
        self, obj: Any, owner: type[Any] | None = None
    ) -> T_co | type[T_co]:
        if obj is None:
            assert owner is not None
            return owner.__lprops__  # type: ignore [no-any-return]
        else:
            return obj._p__lprops__  # type: ignore [no-any-return]


class GelLinkModel(pydantic.BaseModel, metaclass=GelModelMeta):
    model_config = pydantic.ConfigDict(
        validate_assignment=True,
        defer_build=True,
    )

    @classmethod
    def __descriptor__(cls) -> LinkPropsDescriptor[Self]:
        return LinkPropsDescriptor()


MT = TypeVar("MT", bound=GelModel, covariant=True)


class ProxyModel(GelModel, Generic[MT]):
    __proxy_of__: ClassVar[type[MT]]  # type: ignore [misc]
    _p__obj__: MT

    def __init__(self, obj: MT, /) -> None:
        object.__setattr__(self, "_p__obj__", obj)

    def __getattribute__(self, name: str) -> Any:
        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields or name == "_p__id":
            base = object.__getattribute__(self, "_p__obj__")
            return getattr(base, name)
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        model_fields = type(self).__proxy_of__.model_fields
        if name in model_fields:
            # writing to a field: mutate the  wrapped model
            base = object.__getattribute__(self, "_p__obj__")
            setattr(base, name, value)
        else:
            # writing anything else (including _proxied) is normal
            super().__setattr__(name, value)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        generic_meta = cls.__pydantic_generic_metadata__
        if generic_meta["origin"] is ProxyModel and generic_meta["args"]:
            cls.__proxy_of__ = generic_meta["args"][0]

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if cls.__name__ == "ProxyModel" or cls.__name__.startswith(
            "ProxyModel[",
        ):
            return handler(source_type)
        else:
            return pydantic_schema.no_info_before_validator_function(
                cls,
                schema=handler.generate_schema(cls.__proxy_of__),
            )


#
# Metaclass for type __links__ namespaces.  Facilitates
# proper forward type resolution by raising a NameError
# instead of AttributeError when resolving names in its
# namespace, thus not confusing users of typing._eval_type
#
class LinkClassNamespaceMeta(type):
    def __getattr__(cls, name: str) -> Any:
        if name == "__isabstractmethod__":
            return False

        raise NameError(name)


class LinkClassNamespace(metaclass=LinkClassNamespaceMeta):
    pass


BT_co = TypeVar("BT_co", covariant=True)


class BasePointer(GelClassVar, Generic[T_co, BT_co]):
    if TYPE_CHECKING:

        def __get__(self, obj: None, objtype: type[Any]) -> type[T_co]: ...


class OptionalPointer(BasePointer[T_co, BT_co]):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[T_co]: ...

        @overload
        def __get__(self, obj: object, objtype: Any = None) -> T_co | None: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[T_co] | T_co | None: ...

        def __set__(self, obj: Any, value: BT_co | None) -> None: ...


ST = TypeVar("ST", bound=GelPrimitiveType, covariant=True)


class _OptionalProperty(OptionalPointer[ST, BT_co]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            return pydantic_schema.nullable_schema(
                handler.generate_schema(args[0])
            )
        else:
            return handler.generate_schema(source_type)


OptionalProperty = TypeAliasType(
    "OptionalProperty",
    "Annotated[_OptionalProperty[ST, BT_co], Field(default=None)]",
    type_params=(ST, BT_co),
)


BMT = TypeVar("BMT", bound=GelModel, covariant=True)
PT = TypeVar("PT", bound=ProxyModel[GelModel], covariant=True)


class _OptionalLink(OptionalPointer[MT, BMT]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            if issubclass(args[0], ProxyModel):
                return pydantic_schema.no_info_before_validator_function(
                    functools.partial(cls._validate, generic_args=args),
                    schema=handler.generate_schema(args[0]),
                )
            else:
                return handler.generate_schema(args[0])
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> MT:
        mt, bmt = generic_args
        if isinstance(value, mt):
            return value  # type: ignore [no-any-return]
        elif isinstance(value, bmt):
            return mt(value)  # type: ignore [no-any-return]
        else:
            raise TypeError(
                f"could not convert {type(value)} to {mt.__name__}"
            )


OptionalLink = TypeAliasType(
    "OptionalLink",
    "Annotated[_OptionalLink[MT, MT], Field(default=None)]",
    type_params=(MT,),
)

OptionalLinkWithProps = TypeAliasType(
    "OptionalLinkWithProps",
    "Annotated[_OptionalLink[PT, MT], Field(default=None)]",
    type_params=(PT, MT),
)


class _UpcastingDistinctList(lists.DistinctList[MT], Generic[MT, BMT]):
    @classmethod
    def _check_value(cls, value: Any) -> MT:
        t = cls.type
        if isinstance(value, t):
            return value
        elif issubclass(t, ProxyModel) and isinstance(value, t.__proxy_of__):
            return t(value)  # type: ignore [return-value]

        raise ValueError(
            f"{cls!r} accepts only values of type {cls.type!r}, "
            f"got {type(value)!r}",
        )


class _MultiLinkMeta(type):
    _list_type: type[lists.DistinctList[GelModel | ProxyModel[GelModel]]]


class _MultiLink(BasePointer[MT, BMT], metaclass=_MultiLinkMeta):
    if TYPE_CHECKING:

        @overload
        def __get__(self, obj: None, objtype: type[Any]) -> type[MT]: ...

        @overload
        def __get__(
            self, obj: object, objtype: Any = None
        ) -> lists.DistinctList[MT]: ...

        def __get__(
            self,
            obj: Any,
            objtype: Any = None,
        ) -> type[MT] | lists.DistinctList[MT] | None: ...

        def __set__(self, obj: Any, value: Sequence[MT | BMT]) -> None: ...

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: pydantic.GetCoreSchemaHandler,
    ) -> pydantic_core.CoreSchema:
        if _typing_inspect.is_generic_alias(source_type):
            args = typing.get_args(source_type)
            item_type = args[0]
            return pydantic_schema.no_info_before_validator_function(
                functools.partial(cls._validate, generic_args=args),
                schema=pydantic_schema.list_schema(
                    items_schema=handler.generate_schema(item_type),
                ),
                serialization=pydantic_schema.plain_serializer_function_ser_schema(
                    list,
                ),
            )
        else:
            return handler.generate_schema(source_type)

    @classmethod
    def _validate(
        cls,
        value: Any,
        generic_args: tuple[type[Any], type[Any]],
    ) -> lists.DistinctList[MT]:
        lt: type[_UpcastingDistinctList[MT, BMT]] = _UpcastingDistinctList[
            generic_args[0],  # type: ignore [valid-type]
            generic_args[1],  # type: ignore [valid-type]
        ]
        if isinstance(value, lt):
            return value
        elif isinstance(value, (list, lists.DistinctList)):
            return lt(value)
        else:
            raise TypeError(
                f"could not convert {type(value)} to {cls.__name__}"
            )


MultiLink = TypeAliasType(
    "MultiLink",
    Annotated[
        _MultiLink[MT, MT],
        Field(default_factory=lists.DistinctList[GelModel]),
    ],
    type_params=(MT,),
)

MultiLinkWithProps = TypeAliasType(
    "MultiLinkWithProps",
    Annotated[
        _MultiLink[PT, MT],
        Field(default_factory=lists.DistinctList[ProxyModel[GelModel]]),
    ],
    type_params=(PT, MT),
)


class LazyClassProperty(Generic[T]):
    def __init__(
        self, meth: Callable[[type[Any]], T] | classmethod[Any, Any, T], /
    ) -> None:
        if isinstance(meth, classmethod):
            self._func = meth.__func__
        else:
            raise TypeError(
                f"{self.__class__.__name__} must be used to "
                f"decorate classmethods"
            )

    def __set_name__(self, owned: type[Any], name: str) -> None:
        self._name = name

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> T:
        if owner is None:
            raise RuntimeError(
                f"{self.__class__.__name__} called on an instance not a class"
            )
        value = self._func(owner)
        setattr(owner, self._name, value)
        return value


class LazyLinkClassDef:
    def __init__(self, name: str) -> None:
        self._recursion_guard = False
        self._name = name

    def _define(self, name: str) -> type[Any]:
        raise NotImplementedError

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = name

    def __get__(
        self,
        instance: object | None,
        owner: type[Any] | None = None,
    ) -> Any:
        if instance is not None:
            raise AssertionError(
                "unexpected lazy class def access on containing "
                "class instance (not class)"
            )

        assert owner is not None

        fqname = f"{owner.__qualname__}.{self._name}"
        if self._recursion_guard:
            raise NameError(f"recursion while resolving {fqname}")

        self._recursion_guard = True

        try:
            defined = self._define(self._name)
        except AttributeError as e:
            raise NameError(f"cannot define {fqname} yet") from e
        finally:
            self._recursion_guard = False

        setattr(owner, self._name, defined)
        return defined
