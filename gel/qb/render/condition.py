from collections.abc import Iterator
from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.func import render_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    linearize_filter_left,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import unsafe_text


def render_filters(filters: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    conditions = [
        render_condition(filter_.to_infix_notation(), generator)
        for filter_ in filters
    ]
    return combine_renderers(
        RenderedQuery(' filter '),
        reduce(join_renderers(' and '), conditions),
    )


def render_conditions(filters: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    return render_filters(filters, generator) if filters else RenderedQuery()


@singledispatch
def render_condition(expression: AnyExpression, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_condition.register
def _(expression: Alias, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_condition.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_condition(arg, generator)
        for arg in expression.args
    ]
    return render_function(func, arg_renderers)


@render_condition.register
def _(expression: Node, generator: Iterator[int]) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_condition(expression.left, generator),
        )
    return render_binary_node(
        left=render_condition(expression.left, generator),
        right=render_condition(expression.right, generator),
        expression=expression,
    )


@render_condition.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'filter_{index}'
    return render_query_literal(expression.value, name)


@render_condition.register
def _(expression: unsafe_text, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression)


@render_condition.register
def _(expression: Column, generator: Iterator[int]) -> RenderedQuery:
    columns = linearize_filter_left(expression)
    dot_names = '.'.join(c.column_name for c in columns)
    return RenderedQuery(f'.{dot_names}')
