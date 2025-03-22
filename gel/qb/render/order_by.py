from collections.abc import Iterator
from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Node, SortedExpression
from edgeql_qb.render.func import render_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_order_by_expressions(
    ordered_by: tuple[Expression, ...],
    generator: Iterator[int],
) -> RenderedQuery:
    renderers = [
        render_order_by_expression(expression.to_infix_notation(), generator)
        for expression in ordered_by
    ]
    return combine_renderers(
        RenderedQuery(' order by '),
        reduce(join_renderers(' then '), renderers),
    )


def render_order_by(ordered_by: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    return render_order_by_expressions(ordered_by, generator) if ordered_by else RenderedQuery()


@singledispatch
def render_order_by_expression(
        expression: AnyExpression,
        generator: Iterator[int],
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_order_by_expression.register
def _(expression: Column, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(f'.{expression.column_name}')


@render_order_by_expression.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_order_by_expression(arg, generator)
        for arg in expression.args
    ]
    return render_function(func, arg_renderers)


@render_order_by_expression.register
def _(expression: Node, generator: Iterator[int]) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_order_by_expression(expression.left, generator),
        )
    return render_binary_node(
        left=render_order_by_expression(expression.left, generator),
        right=render_order_by_expression(expression.right, generator),
        expression=expression,
    )


@render_order_by_expression.register
def _(expression: SortedExpression, generator: Iterator[int]) -> RenderedQuery:
    return combine_renderers(
        render_order_by_expression(expression.expression, generator),
        RenderedQuery(f' {expression.order}'),
    )


@render_order_by_expression.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'order_by_{index}'
    return render_query_literal(expression.value, name)
