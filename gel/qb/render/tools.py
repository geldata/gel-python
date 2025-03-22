from collections.abc import Callable
from functools import reduce
from typing import cast

from edgeql_qb.expression import Column, SubQuery
from edgeql_qb.operators import Node
from edgeql_qb.render.types import RenderedQuery


def combine_many_renderers(*renderers: RenderedQuery) -> RenderedQuery:
    return reduce(combine_renderers, renderers)


def join_renderers(separator: str = '') -> Callable[[RenderedQuery, RenderedQuery], RenderedQuery]:
    def inner(r1: RenderedQuery, r2: RenderedQuery) -> RenderedQuery:
        return RenderedQuery(
            query=f'{r1.query}{separator}{r2.query}',
            context=r1.context | r2.context,
        )
    return inner


def combine_renderers(r1: RenderedQuery, r2: RenderedQuery) -> RenderedQuery:
    return join_renderers()(r1, r2)


def need_left_parentheses(left: Node, expression: Node) -> bool:
    return (
        isinstance(left, Node)
        and left < expression
        or (
            left == expression
            and not expression.assocright
            and (
                not isinstance(left, Column)
                and getattr(left, 'operation', 1) != getattr(expression, 'operation', 2)
            )
        )
    )


def need_right_parentheses(right: Node, expression: Node) -> bool:
    return (
        isinstance(right, Node)
        and right < expression
        or (isinstance(right, SubQuery))
        or (isinstance(right, Node) and right == expression and expression.assocright)
        or (getattr(right, 'op', None) == '-' and expression.assocright)
    )


def render_parentheses(inner: RenderedQuery) -> RenderedQuery:
    return combine_many_renderers(RenderedQuery('('), inner, RenderedQuery(')'))


def render_assoc_parentheses(
        is_need: bool,  # noqa: FBT001
) -> Callable[[RenderedQuery], RenderedQuery]:
    def inner(column: RenderedQuery) -> RenderedQuery:
        return is_need and render_parentheses(column) or column
    return inner


def render_right_parentheses(
    right: Node,
    expression: Node,
) -> Callable[[RenderedQuery], RenderedQuery]:
    is_need = need_right_parentheses(right, expression)
    return render_assoc_parentheses(is_need=is_need)


def render_left_expression(query: RenderedQuery) -> Callable[[Node], RenderedQuery]:
    def inner(expression: Node) -> RenderedQuery:
        left = render_left_parentheses(expression.left, expression)
        return query.map(left)
    return inner


def render_right_expression(query: RenderedQuery) -> Callable[[Node], RenderedQuery]:
    def inner(expression: Node) -> RenderedQuery:
        right = render_right_parentheses(cast(Node, expression.right), expression)
        return query.map(right)
    return inner


def render_binary_node(
    left: RenderedQuery,
    right: RenderedQuery,
    expression: Node,
) -> RenderedQuery:
    left = render_left_expression(left)(expression)
    right = render_right_expression(right)(expression)
    return join_renderers(f' {expression.op} ')(left, right)


def render_left_parentheses(
    left: Node,
    expression: Node,
) -> Callable[[RenderedQuery], RenderedQuery]:
    is_need = need_left_parentheses(left, expression)
    return render_assoc_parentheses(is_need=is_need)


def linearize_filter_left(column: Column) -> list[Column]:
    match column.parent:
        case None:
            return [column]
        case _:
            return [*linearize_filter_left(column.parent), column]
