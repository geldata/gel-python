from collections.abc import Iterator
from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
    Shape,
    SubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.condition import render_conditions
from edgeql_qb.render.func import render_function
from edgeql_qb.render.order_by import render_order_by
from edgeql_qb.render.pagination import render_limit, render_offset
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    render_binary_node,
    render_parentheses,
)
from edgeql_qb.render.types import RenderedQuery


def render_select_columns(
        select: tuple[Expression, ...],
        generator: Iterator[int],
) -> RenderedQuery:
    renderers = (
        render_select_expression(selectable.to_infix_notation(), generator)
        for selectable in select
    )
    return combine_many_renderers(
        RenderedQuery(' { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


def render_select(
    model_name: str,
    select: tuple[Expression, ...],
    generator: Iterator[int],
    select_from_query: SubQuery | None = None,
) -> RenderedQuery:
    rendered_select = (
        render_parentheses(select_from_query.build(generator))
        if select_from_query
        else RenderedQuery(model_name)
    )
    return combine_many_renderers(
        RenderedQuery('select '),
        rendered_select,
    ).map(
        lambda r: (
            combine_renderers(r, render_select_columns(select, generator))
            if select
            else r
        ),
    )


@singledispatch
def render_select_expression(
        expression: AnyExpression,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_select_expression.register
def _(expression: Column, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_select_expression.register
def _(
        expression: FuncInvocation,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_select_expression(arg, generator, column_prefix)
        for arg in expression.args
    ]
    return render_function(func, arg_renderers)


@render_select_expression.register
def _(expression: Alias, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_select_expression.register
def _(expression: Shape, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    expressions = (
        render_select_expression(exp, generator, column_prefix)
        for exp in expression.columns
    )
    conditions = render_conditions(expression.filters, generator=generator)
    order_by = render_order_by(expression.ordered_by, generator=generator)
    rendered_offset = render_offset(expression.offset_val, generator=generator)
    rendered_limit = render_limit(expression.limit_val, generator=generator)
    return combine_many_renderers(
        RenderedQuery(f'{expression.parent.column_name}: {{ '),
        reduce(join_renderers(', '), expressions),
        RenderedQuery(' }'),
        conditions,
        order_by,
        rendered_offset,
        rendered_limit,
    )


@render_select_expression.register
def _(
        expression: QueryLiteral,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    index = next(generator)
    name = f'select_{index}'
    return render_query_literal(expression.value, name)


@render_select_expression.register
def _(expression: Node, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_select_expression(expression.left, generator, column_prefix),
        )
    return render_binary_node(
        left=render_select_expression(
            expression.left,
            generator,
            expression.op != ':=' and '.' or '',
        ),
        right=render_select_expression(expression.right, generator, '.'),
        expression=expression,
    )


@render_select_expression.register
def _(expression: SubQuery, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return expression.build(generator)
