# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""Base object types used to implement class-based query builders"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any

import dataclasses

from gel._internal import _qb
from gel._internal._utils import Unspecified

from ._base import GelType

if TYPE_CHECKING:
    from collections.abc import Callable


def _get_prefixed_ptr(
    cls: type[GelType],
    ptrname: str,
    scope: _qb.Scope,
) -> tuple[_qb.PathAlias, _qb.Path]:
    this_type = cls.__gel_reflection__.name

    ptr = getattr(cls, ptrname, Unspecified)
    if ptr is Unspecified:
        sn = this_type.as_schema_name()
        msg = f"{ptrname} is not a valid {sn} property"
        raise AttributeError(msg)
    if not isinstance(ptr, _qb.PathAlias):
        raise AssertionError(
            f"expected {cls.__name__}.{ptrname} to be a PathAlias"
        )

    expr = _qb.edgeql_qb_expr(ptr)
    assert isinstance(expr, _qb.Path)
    expr = dataclasses.replace(expr, source=_qb.PathPrefix(type_=this_type))
    return ptr, expr


def _fuse_filters(
    expr: _qb.Expr,
    filters: list[_qb.Expr],
) -> _qb.Expr:
    if not isinstance(expr, _qb.SelectStmt):
        expr = _qb.SelectStmt(
            expr=expr,
            implicit=True,
        )

    if expr.filter is None:
        expr.filter = _qb.Filter(filters=filters)
    else:
        expr.filter.filters.extend(filters)

    return expr


def _fuse_orderbys(
    expr: _qb.Expr,
    directions: list[_qb.Expr],
) -> _qb.Expr:
    if (
        not isinstance(expr, _qb.SelectStmt)
        or expr.limit is not None
        or expr.offset is not None
    ):
        expr = _qb.SelectStmt(
            expr=expr,
            implicit=True,
        )

    return dataclasses.replace(
        expr,
        order_by=_qb.OrderBy(directions=directions),
    )


def _fuse_limit(
    expr: _qb.Expr,
    limit: _qb.Expr,
) -> _qb.Expr:
    if not isinstance(expr, _qb.SelectStmt):
        expr = _qb.SelectStmt(
            expr=expr,
            implicit=True,
        )

    if expr.limit is not None:
        limit = _qb.FuncCall(
            fname="std::min",
            args=[
                _qb.SetLiteral(
                    items=[expr.limit.limit, limit],
                    type_=limit.type,
                ),
            ],
            kwargs={},
            type_=limit.type,
        )

    return dataclasses.replace(
        expr,
        limit=_qb.Limit(limit=limit),
    )


def _fuse_offset(
    expr: _qb.Expr,
    offset: _qb.Expr,
) -> _qb.Expr:
    if not isinstance(expr, _qb.SelectStmt):
        expr = _qb.SelectStmt(
            expr=expr,
            implicit=True,
        )

    if expr.offset is not None:
        offset = _qb.FuncCall(
            fname="std::min",
            args=[
                _qb.SetLiteral(
                    items=[expr.offset.offset, offset],
                    type_=offset.type,
                ),
            ],
            kwargs={},
            type_=offset.type,
        )

    return dataclasses.replace(
        expr,
        offset=_qb.Offset(offset=offset),
    )


def select(
    cls: type[GelType],
    /,
    *elements: _qb.PathAlias,
    __operand__: _qb.ExprAlias | None = None,
    **kwargs: bool | Callable[[type[GelType] | _qb.BaseAlias], type[GelType]],
) -> _qb.ShapeOp:
    shape: dict[str, _qb.Expr] = {}

    this_type = cls.__gel_reflection__.name
    scope = _qb.Scope()

    if __operand__ is not None:
        operand = _qb.edgeql_qb_expr(__operand__)
        if (
            isinstance(operand, _qb.ShapeOp)
            and not operand.shape.elements
            and operand.shape.star_splat
        ):
            operand = operand.expr
    else:
        operand = _qb.SchemaSet(type_=this_type)

    subject = _qb.edgeql_qb_expr(operand)
    prefix = _qb.PathPrefix(type_=subject.type, scope=scope)
    prefix_alias = _qb.PathAlias(cls, prefix)

    for elem in elements:
        path = _qb.edgeql_qb_expr(elem)
        if not isinstance(path, _qb.Path):
            raise TypeError(f"{elem} is not a valid path expression")

        if path.source.type == this_type:
            path = dataclasses.replace(path, source=prefix)

        shape[path.name] = path

    for ptrname, kwarg in kwargs.items():
        if isinstance(kwarg, bool):
            _, ptr_expr = _get_prefixed_ptr(cls, ptrname, scope=scope)
            if kwarg:
                shape[ptrname] = ptr_expr
            else:
                shape.pop(ptrname, None)
        else:
            shape[ptrname] = _qb.edgeql_qb_expr(kwarg(prefix_alias))

    return _qb.ShapeOp(
        expr=operand,
        shape_=_qb.Shape(elements=shape),
        scope=scope,
    )


def update(
    cls: type[GelType],
    /,
    __operand__: _qb.ExprAlias | None = None,
    **kwargs: bool | type[GelType],
) -> _qb.UpdateStmt:
    shape: dict[str, _qb.Expr] = {}

    this_type = cls.__gel_reflection__.name
    scope = _qb.Scope()

    for ptrname, kwarg in kwargs.items():
        if isinstance(kwarg, bool):
            _, ptr_expr = _get_prefixed_ptr(cls, ptrname, scope=scope)
            if kwarg:
                shape[ptrname] = ptr_expr
            else:
                shape.pop(ptrname, None)
        else:
            if not isinstance(kwarg, GelType):
                # Raw Python constant, convert to a literal
                val = getattr(cls, ptrname)(kwarg)
            else:
                val = kwarg
            shape[ptrname] = _qb.edgeql_qb_expr(val)

    if __operand__ is not None:
        operand = _qb.edgeql_qb_expr(__operand__)
        if (
            isinstance(operand, _qb.ShapeOp)
            and not operand.shape.elements
            and operand.shape.star_splat
        ):
            operand = operand.expr
    else:
        operand = _qb.SchemaSet(type_=this_type)

    return _qb.UpdateStmt(expr=operand, shape_=_qb.Shape(elements=shape))


def delete(
    cls: type[GelType],
    /,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.DeleteStmt:
    operand = cls if __operand__ is None else __operand__
    subject = _qb.edgeql_qb_expr(operand)
    return _qb.DeleteStmt(expr=subject)


def add_filter(
    cls: type[GelType],
    /,
    *exprs: Any,
    __operand__: _qb.ExprAlias | None = None,
    **properties: Any,
) -> _qb.Expr:
    operand = cls if __operand__ is None else __operand__
    subject = _qb.edgeql_qb_expr(operand)
    prefix = _qb.AnnotatedExpr(cls, _qb.PathPrefix(type_=subject.type))

    all_exprs = [expr(prefix) for expr in exprs]
    for ptrname, value in properties.items():
        ptr, ptr_expr = _get_prefixed_ptr(cls, ptrname)
        ptr_comp = _qb.edgeql_qb_expr(ptr == value)
        if not isinstance(ptr_comp, _qb.InfixOp):
            raise AssertionError(
                f"comparing {ptrname} to {value} did not produce an infix op"
            )
        ptr_comp.lexpr = ptr_expr
        all_exprs.append(ptr_comp)

    return _fuse_filters(subject, all_exprs)


def order_by(
    cls: type[GelType],
    /,
    *elements: _qb.PathAlias,
    __operand__: _qb.ExprAlias | None = None,
    **kwargs: str,
) -> _qb.Expr:
    all_exprs: list[_qb.Expr] = []
    this_type = cls.__gel_reflection__.name

    for elem in elements:
        path = _qb.edgeql_qb_expr(elem)
        if not isinstance(path, _qb.Path):
            raise TypeError(f"{elem} is not a valid path expression")

        if path.source.type == this_type:
            path = copy.copy(path)
            path.source = _qb.PathPrefix(type_=this_type)

        all_exprs.append(path)

    for ptrname in kwargs:
        _, ptr_expr = _get_prefixed_ptr(cls, ptrname)
        all_exprs.append(ptr_expr)

    if __operand__ is not None:
        operand = _qb.edgeql_qb_expr(__operand__)
    else:
        operand = _qb.SchemaSet(type_=this_type)

    return _fuse_orderbys(operand, all_exprs)


def add_limit(
    cls: type[GelType],
    /,
    expr: Any,
    *,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.Expr:
    if isinstance(expr, int):
        expr = _qb.IntLiteral(val=expr)
    operand = cls if __operand__ is None else __operand__
    subject = _qb.edgeql_qb_expr(operand)
    return _fuse_limit(subject, expr)


def add_offset(
    cls: type[GelType],
    /,
    expr: Any,
    *,
    __operand__: _qb.ExprAlias | None = None,
) -> _qb.Expr:
    if isinstance(expr, int):
        expr = _qb.IntLiteral(val=expr)
    operand = cls if __operand__ is None else __operand__
    subject = _qb.edgeql_qb_expr(operand)
    return _fuse_offset(subject, expr)
