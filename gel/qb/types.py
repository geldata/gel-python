from typing import Generic, TypeVar

T = TypeVar('T')


class GenericHolder(Generic[T]):
    __match_args__ = 'edgeql_name', 'value'

    edgeql_name: str

    def __init__(self, value: T):
        self.value = value

    def __repr__(self) -> str:
        return f'<{self.edgeql_name}>{self.value}'


class int16(GenericHolder[int]):
    edgeql_name = 'int16'


class int32(GenericHolder[int]):
    edgeql_name = 'int32'


class int64(GenericHolder[int]):
    edgeql_name = 'int64'


class bigint(GenericHolder[int]):
    edgeql_name = 'bigint'


class float32(GenericHolder[float]):
    edgeql_name = 'float32'


class float64(GenericHolder[float]):
    edgeql_name = 'float64'


class unsafe_text(str):
    """Wrapper for rendering a text node as is.

    WARNING: Don't pass unvalidated user input to this wrapper.
    Otherwise, you may be the first victim of EdgeQL Injection attack.
    There's no honor in this.
    """
