from collections.abc import Iterator
from functools import reduce, singledispatch
from typing import Any

from edgeql_qb.expression import (
    AnyExpression,
    BaseModel,
    Column,
    Expression,
    QueryLiteral,
    SubQuery,
    UnlessConflict,
    UpdateSubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.func import render_function, render_function_args
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    join_renderers,
    render_binary_node,
    render_parentheses,
)
from edgeql_qb.render.types import RenderedQuery


def render_insert(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'insert {model_name}')


def render_values(values: list[Expression], generator: Iterator[int]) -> RenderedQuery:
    assert values
    renderers = [
        render_insert_expression(value.to_infix_notation(), generator)
        for value in values
    ]
    return combine_many_renderers(
        RenderedQuery(' { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


@singledispatch
def render_insert_expression(expression: AnyExpression, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_insert_expression.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_insert_expression(arg, generator)
        for arg in expression.args
    ]
    return render_function(func, arg_renderers)


@render_insert_expression.register
def _(expression: Column, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression.column_name)


@render_insert_expression.register
def _(expression: Node, generator: Iterator[int]) -> RenderedQuery:
    assert expression.right is not None, 'Unary operations is not supported in insert expressions'
    return render_binary_node(
        left=render_insert_expression(expression.left, generator),
        right=render_insert_expression(expression.right, generator),
        expression=expression,
    )


@render_insert_expression.register
def _(expression: Alias, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_insert_expression.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'insert_{index}'
    return render_query_literal(expression.value, name)


@render_insert_expression.register
def _(expression: SubQuery, generator: Iterator[int]) -> RenderedQuery:
    return expression.build(generator)


@singledispatch
def render_unless_conflict_on(on: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{on!r} is not supported')  # pragma: no cover


@render_unless_conflict_on.register
def _(on: Column, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(f'.{on.column_name}')


@render_unless_conflict_on.register
def _(
    on: tuple,  # type: ignore[type-arg]
    generator: Iterator[int],
) -> RenderedQuery:
    renderers = [
        render_unless_conflict_on(value, generator)
        for value in on
    ]
    return render_function_args(renderers)


@singledispatch
def render_unless_conflict(conflict: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{conflict!r} is not supported')  # pragma: no cover


@render_unless_conflict.register
def _(conflict: None, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery()


@render_unless_conflict.register
def _(conflict: UnlessConflict, generator: Iterator[int]) -> RenderedQuery:
    rendered_on = (
        combine_many_renderers(
            RenderedQuery(' on '),
            render_unless_conflict_on(conflict.on, generator),
        )
        if conflict.on
        else RenderedQuery()
    )
    rendered_else = (
        combine_many_renderers(
            RenderedQuery(' else '),
            render_unless_conflict_else(conflict.else_, generator),
        )
        if conflict.else_
        else RenderedQuery()
    )
    return combine_many_renderers(
        RenderedQuery(' unless conflict'),
        rendered_on,
        rendered_else,
    )


@singledispatch
def render_unless_conflict_else(else_: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{else_!r} is not supported')  # pragma: no cover


@render_unless_conflict_else.register
def _(else_: BaseModel, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(else_.name)


@render_unless_conflict_else.register
def _(expression: UpdateSubQuery, generator: Iterator[int]) -> RenderedQuery:
    return render_parentheses(expression.build(generator))
