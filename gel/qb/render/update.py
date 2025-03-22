from collections.abc import Iterator
from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
    SubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.func import render_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_update(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'update {model_name}')


def render_values(values: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    assert values
    renderers = [
        render_update_expression(value.to_infix_notation(), generator, '.')
        for value in values
    ]
    return combine_many_renderers(
        RenderedQuery(' set { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


@singledispatch
def render_update_expression(
        expression: AnyExpression,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_update_expression.register
def _(expression: Column, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_update_expression.register
def _(expression: Alias, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_update_expression.register
def _(expression: Node, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    assert expression.right is not None, 'Unary operations is not supported in update expressions'
    return render_binary_node(
        left=render_update_expression(
            expression.left,
            generator,
            '.' if expression.op != ':=' else '',
        ),
        right=render_update_expression(expression.right, generator, '.'),
        expression=expression,
    )


@render_update_expression.register
def _(
        expression: FuncInvocation,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_update_expression(arg, generator, column_prefix)
        for arg in expression.args
    ]
    return render_function(func, arg_renderers)


@render_update_expression.register
def _(
        expression: QueryLiteral,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    index = next(generator)
    name = f'update_{index}'
    return render_query_literal(expression.value, name)


@render_update_expression.register
def _(expression: SubQuery, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return expression.build(generator)
