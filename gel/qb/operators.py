from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from edgeql_qb.types import GenericHolder

if TYPE_CHECKING:
    from edgeql_qb.expression import SubQuery  # pragma: no cover


OpLiterals = Literal[
    '=', ':=', '!=', '>', '>=', '<', '<=', '*', '/', '//', '%', '^',
    'and', 'or', '+', '++', '-', 'like', 'ilike', 'in', 'not in', '??', '?=', '?!=',
    'not ', 'exists ', 'not exists ',
]
SortOpLiterals = Literal['asc', 'desc']
Ops: set[OpLiterals] = {
    '=', ':=', '!=', '>', '>=', '<', '<=',
    '*', '/', '//', '%', '^', 'and', 'or', '+', '++', '-',
    'like', 'ilike', 'in', 'not in', '??', '?=', '?!=',
    'not ', 'exists ', 'not exists ',
}
prec_dict: dict[OpLiterals, int] = {
    '^': 11,

    '??': 10,

    '/': 9,
    '*': 9,
    '//': 9,
    '%': 9,

    '+': 8,
    '-': 8,
    '++': 8,

    'in': 7,
    'not in': 7,

    'like': 6,
    'ilike': 6,

    '>': 5,
    '<': 5,
    '>=': 5,
    '<=': 5,

    '=': 4,
    '!=': 4,
    '?=': 4,
    '?!=': 4,

    'not ': 3,
    'exists ': 3,
    'not exists ': 3,

    'and': 2,
    'or': 1,

    ':=': 0,
}
right_assoc_operations = {'^'}
sort_ops = {'asc', 'desc'}
prec_dict_limit = max(prec_dict.values()) + 1


class OperationsMixin:
    def asc(self) -> 'SortedExpression':
        return SortedExpression(self, 'asc')

    def desc(self) -> 'SortedExpression':
        return SortedExpression(self, 'desc')

    def op(self, operation: OpLiterals) -> Callable[[Any], 'BinaryOp']:
        def inner(other: Any) -> BinaryOp:
            return BinaryOp(operation, self, other)
        return inner

    def label(self, name: str) -> 'BinaryOp':
        return BinaryOp(':=', Alias(name), self)

    def like(self, other: Any) -> 'BinaryOp':
        return BinaryOp('like', self, other)

    def ilike(self, other: Any) -> 'BinaryOp':
        return BinaryOp('ilike', self, other)

    def in_(self, other: Any) -> 'BinaryOp':
        return BinaryOp('in', self, other)

    def not_in(self, other: Any) -> 'BinaryOp':
        return BinaryOp('not in', self, other)

    def coalesce(self, other: Any) -> 'BinaryOp':
        return BinaryOp('??', self, other)

    def concat(self, other: Any) -> 'BinaryOp':
        return BinaryOp('++', self, other)

    def exists(self) -> 'UnaryOp':
        return UnaryOp('exists ', self)

    def not_exists(self) -> 'UnaryOp':
        return UnaryOp('not exists ', self)

    def __pos__(self) -> 'UnaryOp':
        return UnaryOp('+', self)

    def __neg__(self) -> 'UnaryOp':
        return UnaryOp('-', self)

    def __invert__(self) -> 'UnaryOp':
        return UnaryOp('not ', self)

    def __and__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('and', self, other)

    def __or__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('or', self, other)

    def __gt__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('>', self, other)

    def __ge__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('>=', self, other)

    def __lt__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('<', self, other)

    def __le__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('<=', self, other)

    def __add__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('+', self, other)

    def __sub__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('-', self, other)

    def __mul__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('*', self, other)

    def __truediv__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('/', self, other)

    def __floordiv__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('//', self, other)

    def __mod__(self, other: Any) -> 'BinaryOp':
        return BinaryOp('%', self, other)


@dataclass(slots=True, frozen=True)
class BinaryOp(OperationsMixin):
    operation: OpLiterals
    left: OperationsMixin
    right: Union[OperationsMixin, 'SubQuery', GenericHolder[Any]]

    def __eq__(self, other: Any) -> 'BinaryOp':  # type: ignore[override]
        return BinaryOp('=', self, other)


@dataclass(slots=True, frozen=True)
class UnaryOp(OperationsMixin):
    operation: OpLiterals
    element: Any


@dataclass(slots=True, frozen=True)
class Alias(OperationsMixin):
    name: str

    def assign(self, value: Union[OperationsMixin, 'SubQuery', GenericHolder[Any]]) -> BinaryOp:
        return BinaryOp(':=', self, value)


@dataclass(slots=True, frozen=True)
class SortedExpression:
    expression: OperationsMixin
    order: Literal['asc', 'desc']


@dataclass(slots=True, frozen=True)
class Node:
    left: 'Node'
    op: OpLiterals
    right: Optional['Node'] = None

    @property
    def precedence(self) -> int:
        return prec_dict[self.op]

    @property
    def assocright(self) -> bool:
        return self.op in right_assoc_operations

    def __repr__(self) -> str:
        return f'Node({self.left!r},{self.op!r},{self.right!r})'

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, Node):
            return self.precedence < other.precedence
        return self.precedence < prec_dict.get(other, prec_dict_limit)

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, Node):
            return self.precedence > other.precedence
        return self.precedence > prec_dict.get(other, prec_dict_limit)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Node):
            return self.precedence == other.precedence
        return self.precedence == prec_dict.get(other, prec_dict_limit)
