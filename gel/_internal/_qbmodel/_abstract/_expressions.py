# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base object types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload
from typing_extensions import TypeAliasType

import dataclasses
import functools

from gel._internal import _qb
from gel._internal._utils import Unspecified

from ._base import AbstractGelModel
from ._descriptors import AbstractGelProxyModel

from ._primitive import (
    GelPrimitiveType,
    PyConstType,
    PyTypeScalar,
    get_literal_for_scalar,
    get_literal_for_value,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._base import GelType


_T = TypeVar("_T")


def _get_prefixed_ptr(
    cls: type[GelType],
    ptrname: str,
    scope: _qb.Scope,
) -> tuple[_qb.PathAlias, _qb.Path]:
    this_type = cls.__gel_reflection__.name

    ptr = getattr(cls, ptrname, Unspecified)
    if ptr is Unspecified:
        sn = str(this_type)
        msg = f"{ptrname} is not a valid {sn} property"
        raise AttributeError(msg)
    if not isinstance(ptr, _qb.PathAlias):
        raise AssertionError(
            f"expected {cls.__name__}.{ptrname} to be a PathAlias"
        )

    expr = _qb.edgeql_qb_expr(ptr)
    assert isinstance(expr, _qb.Path)
    prefix = _qb.PathPrefix(type_=this_type, scope=scope)
    expr = dataclasses.replace(expr, source=prefix)
    return ptr, expr


def _select_stmt_context(
    cls: type[GelType],
    operand: _qb.ExprAlias | None = None,
    *,
    new_stmt_if: Callable[[_qb.SelectStmt], bool] | None = None,
) -> tuple[_qb.SelectStmt, _qb.PathAlias]:
    if issubclass(cls, AbstractGelModel):
        splat_cb = functools.partial(_qb.get_object_type_splat, cls)
    else:
        splat_cb = None
    stmt = _qb.SelectStmt.wrap(
        _qb.edgeql_qb_expr(cls if operand is None else operand),
        new_stmt_if=new_stmt_if,
        splat_cb=splat_cb,
    )
    return stmt, _qb.PathAlias(cls, stmt.path_prefix)


_Value = TypeAliasType(
    "_Value", _qb.ExprClosure | _qb.ExprCompatible | PyConstType
)


def _val_to_expr(
    val: _Value,
    scope_var: _qb.PathAlias,
    conv: Callable[[PyConstType], GelPrimitiveType],
) -> _qb.Expr:
    expr = conv(val) if isinstance(val, PyConstType) else val
    return _qb.edgeql_qb_expr(expr, var=scope_var)


def _const_to_expr(
    cls: type[GelType],
    pname: str,
    val: Any,
) -> _qb.Literal | _qb.CastOp:
    ptype: type[PyTypeScalar[PyConstType]] | None = getattr(cls, pname, None)
    if ptype is None:
        return get_literal_for_value(val)
    else:
        return get_literal_for_scalar(ptype, val)


def select(
    cls: type[AbstractGelModel],
    /,
    *elements: _qb.ShapeElement | Literal["*"],
    __operand__: _qb.ExprAlias | None = None,
    **kwargs: bool | _Value,
) -> _qb.ShapeOp:
    this_type = cls.__gel_reflection__.name
    scope = _qb.Scope()

    if __operand__ is not None:
        operand = _qb.edgeql_qb_expr(__operand__)
        if (
            isinstance(operand, _qb.ShapeOp)
            and len(operand.shape.elements) == 1
            and operand.shape.elements[0].name is _qb.Splat.STAR
            and operand.shape.elements[0].origin == this_type
        ):
            operand = operand.iter_expr
    else:
        operand = _qb.SchemaSet(type_=this_type)

    subject = _qb.edgeql_qb_expr(operand)
    prefix = _qb.PathPrefix(type_=subject.type, scope=scope)
    prefix_alias = _qb.PathAlias(cls, prefix)

    shape_elements: list[_qb.ShapeElement] = []
    for elem in elements:
        if elem == "*":
            shape_elements.extend(iter(cls))
        else:
            shape_elements.append(elem)

    for ptrname, kwarg in kwargs.items():
        if isinstance(kwarg, bool):
            ptr_expr: _qb.Expr
            ptr, ptr_expr = _get_prefixed_ptr(cls, ptrname, scope=scope)
            if kwarg:
                target = ptr.__gel_origin__
                if issubclass(target, AbstractGelModel):
                    ptr_expr = _qb.ShapeOp(
                        iter_expr=ptr_expr,
                        shape=_qb.get_object_type_splat(target),
                    )
                    shape_el = _qb.ShapeElement(
                        name=ptrname,
                        expr=ptr_expr,
                        origin=this_type,
                    )
                else:
                    shape_el = _qb.ShapeElement(
                        name=ptrname,
                        origin=this_type,
                    )

                shape_elements.append(shape_el)
            else:
                shape_elements = list(
                    filter(lambda el: el.name != ptrname, shape_elements)
                )
        elif isinstance(kwarg, PyConstType):
            shape_el = _qb.ShapeElement(
                name=ptrname,
                expr=_const_to_expr(cls, ptrname, kwarg),
                origin=this_type,
            )
            shape_elements.append(shape_el)
        else:
            el_expr = _qb.edgeql_qb_expr(kwarg, var=prefix_alias)
            shape_el = _qb.ShapeElement(
                name=ptrname,
                expr=el_expr,
                origin=this_type,
            )
            shape_elements.append(shape_el)

    if issubclass(cls, AbstractGelProxyModel):
        shape_names = {el.name: el for el in shape_elements}
        link_class = cls.__linkprops__  # pyright: ignore [reportGeneralTypeIssues]
        for lprop in link_class.__gel_reflection__.pointers:
            ptrname = f"@{lprop}"
            if ptrname not in shape_names:
                shape_elements.append(
                    _qb.ShapeElement(
                        name=ptrname,
                        origin=this_type,
                    )
                )

    return _qb.ShapeOp(
        iter_expr=operand,
        shape=_qb.Shape(elements=shape_elements),
        scope=scope,
        body_scope=scope,
    )


def update(
    cls: type[GelType],
    /,
    __operand__: _qb.ExprAlias | None = None,
    **kwargs: _Value,
) -> _qb.UpdateStmt:
    this_type = cls.__gel_reflection__.name
    scope = _qb.Scope()
    operand = _qb.edgeql_qb_expr(cls if __operand__ is None else __operand__)
    this_type = cls.__gel_reflection__.name
    prefix = _qb.PathPrefix(type_=this_type, scope=scope)
    prefix_alias = _qb.PathAlias(cls, prefix)

    shape: list[_qb.ShapeElement] = []
    for ptrname, kwarg in kwargs.items():
        ptr_expr: _qb.Expr
        if isinstance(kwarg, PyConstType):
            ptr_expr = _const_to_expr(cls, ptrname, kwarg)
        else:
            ptr_expr = _qb.edgeql_qb_expr(kwarg, var=prefix_alias)
        shape.append(
            _qb.ShapeElement(name=ptrname, expr=ptr_expr, origin=this_type)
        )

    return _qb.UpdateStmt(
        iter_expr=operand,
        shape=_qb.Shape(elements=shape),
        body_scope=scope,
    )


def delete(
    cls: type[GelType],
    /,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.DeleteStmt:
    operand = cls if __operand__ is None else __operand__
    subject = _qb.edgeql_qb_expr(operand)
    return _qb.DeleteStmt(iter_expr=subject)


def add_filter(
    cls: type[GelType],
    /,
    *exprs: _qb.ExprClosure | _qb.ExprCompatible,
    __operand__: _qb.ExprAlias | None = None,
    **properties: Any,
) -> _qb.Expr:
    stmt, prefix = _select_stmt_context(cls, __operand__)
    all_exprs = [_qb.edgeql_qb_expr(expr, var=prefix) for expr in exprs]
    for ptrname, value in properties.items():
        ptr, ptr_expr = _get_prefixed_ptr(cls, ptrname, scope=stmt.scope)
        ptr_comp = _qb.edgeql_qb_expr(ptr == value)
        if not isinstance(ptr_comp, _qb.InfixOp):
            raise AssertionError(
                f"comparing {ptrname} to {value} did not produce an infix op"
            )
        ptr_comp = dataclasses.replace(ptr_comp, lexpr=ptr_expr)
        all_exprs.append(ptr_comp)

    if stmt.filter is None:
        filter_ = _qb.Filter(filters=all_exprs)
    else:
        filter_ = _qb.Filter(filters=stmt.filter.filters + all_exprs)

    return dataclasses.replace(stmt, filter=filter_)


def order_by(
    cls: type[GelType],
    /,
    *exprs: _qb.ExprClosure
    | tuple[_qb.ExprClosure, str]
    | tuple[_qb.ExprClosure, str, str],
    __operand__: _qb.ExprAlias | None = None,
    **kwargs: bool | str | tuple[str, str],
) -> _qb.Expr:
    stmt, prefix = _select_stmt_context(
        cls,
        __operand__,
        new_stmt_if=lambda s: s.limit is not None or s.offset is not None,
    )
    elems: list[_qb.OrderByElem] = []
    for expr in exprs:
        match expr:
            case (f, str(d), str(nd)):
                elem = _qb.OrderByElem(
                    expr=_qb.edgeql_qb_expr(f, var=prefix),  # type: ignore [arg-type]
                    direction=_qb.OrderDirection(d),
                    empty_direction=_qb.OrderEmptyDirection(nd),
                )
            case (f, str(d)):
                elem = _qb.OrderByElem(
                    expr=_qb.edgeql_qb_expr(f, var=prefix),  # type: ignore [arg-type]
                    direction=_qb.OrderDirection(d),
                )
            case f:
                elem = _qb.OrderByElem(
                    expr=_qb.edgeql_qb_expr(f, var=prefix),  # type: ignore [arg-type]
                )

        elems.append(elem)

    for ptrname, val in kwargs.items():
        _, ptr_expr = _get_prefixed_ptr(cls, ptrname, scope=stmt.scope)
        match val:
            case (str(d), str(nd)):
                elem = _qb.OrderByElem(
                    expr=ptr_expr,
                    direction=_qb.OrderDirection(d),
                    empty_direction=_qb.OrderEmptyDirection(nd),
                )
            case str(d):
                elem = _qb.OrderByElem(
                    expr=ptr_expr,
                    direction=_qb.OrderDirection(d),
                )
            case bool():
                elem = _qb.OrderByElem(
                    expr=ptr_expr,
                )
            case _:
                raise ValueError(
                    f"invalid order_by element, expected two to three "
                    f"elements, got {_}"
                )
        elems.append(elem)

    return dataclasses.replace(stmt, order_by=_qb.OrderBy(directions=elems))


def add_limit(
    cls: type[GelType],
    /,
    expr: _qb.ExprCompatible | int,
    *,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.Expr:
    stmt, _ = _select_stmt_context(cls, __operand__)

    if not isinstance(expr, int):
        limit = _qb.edgeql_qb_expr(expr)
    else:
        limit = _qb.IntLiteral(val=expr)

    if stmt.limit is not None:
        limit = _qb.FuncCall(
            fname="std::min",
            args=[
                _qb.SetLiteral(
                    items=(stmt.limit.limit, limit),
                    type_=limit.type,
                ),
            ],
            kwargs={},
            type_=limit.type,
        )

    return dataclasses.replace(stmt, limit=_qb.Limit(limit=limit))


def add_offset(
    cls: type[GelType],
    /,
    expr: _qb.ExprCompatible | int,
    *,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.Expr:
    stmt, _ = _select_stmt_context(cls, __operand__)

    if not isinstance(expr, int):
        offset = _qb.edgeql_qb_expr(expr)
    else:
        offset = _qb.IntLiteral(val=expr)

    if stmt.offset is not None:
        offset = _qb.FuncCall(
            fname="std::min",
            args=[
                _qb.SetLiteral(
                    items=(stmt.offset.offset, offset),
                    type_=offset.type,
                ),
            ],
            kwargs={},
            type_=offset.type,
        )

    return dataclasses.replace(stmt, offset=_qb.Offset(offset=offset))


@overload
def empty_set_if_none(
    val: None,
    type_: type[GelType],
) -> _qb.CastOp: ...


@overload
def empty_set_if_none(
    val: _T,
    type_: type[GelType],
) -> _T: ...


def empty_set_if_none(
    val: _T | None,
    type_: type[GelType],
) -> _T | _qb.CastOp:
    return _qb.empty_set_if_none(val, type_=type_.__gel_reflection__.name)
