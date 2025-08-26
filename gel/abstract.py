#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import (
    Any,
    Generic,
    overload,
)

import abc
import dataclasses
import typing
import typing_extensions

from . import datatypes
from . import describe
from . import enums
from . import errors
from . import options
from .protocol import protocol  # pyright: ignore [reportAttributeAccessIssue]

__all__ = (
    "QueryWithArgs",
    "QueryCache",
    "QueryOptions",
    "BaseQueryContext",
    "QueryContext",
    "QuerySingleContext",
    "QueryRequiredSingleContext",
    "QueryJsonContext",
    "QuerySingleJsonContext",
    "QueryRequiredSingleJsonContext",
    "Executor",
    "ExecuteContext",
    "AsyncIOExecutor",
    "ReadOnlyExecutor",
    "AsyncIOReadOnlyExecutor",
    "DescribeContext",
    "DescribeResult",
)


_T_ql = typing.TypeVar("_T_ql", covariant=True)
_T_get = typing.TypeVar("_T_get")


if typing.TYPE_CHECKING:

    class QueryableObject(typing_extensions.Protocol, typing.Generic[_T_ql]):
        def __edgeql__(self) -> tuple[type[_T_ql], str]: ...

    class QueryableType(typing_extensions.Protocol, typing.Generic[_T_ql]):
        @classmethod
        def __edgeql__(cls) -> tuple[type[_T_ql], str]: ...

    Queryable = typing_extensions.TypeAliasType(
        "Queryable",
        QueryableObject[_T_ql] | type[QueryableType[_T_ql]],
        type_params=(_T_ql,),
    )


_unset = object()


@dataclasses.dataclass(frozen=True)
class TypedQueryExpression(typing.Generic[_T_ql]):
    tp: type[_T_ql]
    query: str

    def __edgeql__(self) -> tuple[type[_T_ql], str]:
        return (self.tp, self.query)


def expr(tp: type[_T_ql], query: str) -> TypedQueryExpression[_T_ql]:
    """Create a typed query expression.

    This function creates a TypedQueryExpression that associates a raw
    EdgeQL query string with a return type.

    Args:
        tp: The expected return type of the query expression.
        query: The raw EdgeQL query string.

    Returns:
        A TypedQueryExpression that combines the type and query string.

    Example:
        >>> from myapp.models import User
        >>> users = client.query(gel.expr(User, "SELECT User { name, email }"))
        >>> reveal_type(users)
        note: Revealed type is "builtins.list[models.default.User]"
    """
    return TypedQueryExpression(tp, query)


@dataclasses.dataclass(frozen=True)
class QueryWithArgs(Generic[_T_ql]):
    query: str | Queryable[_T_ql]
    return_type: type[_T_ql] | None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    input_language: protocol.InputLanguage = protocol.InputLanguage.EDGEQL

    @overload
    @classmethod
    def from_query(
        cls,
        query: Queryable[_T_ql],
        args: Any,
        kwargs: Any,
    ) -> QueryWithArgs[_T_ql]: ...

    @overload
    @classmethod
    def from_query(
        cls,
        query: str,
        args: Any,
        kwargs: Any,
    ) -> QueryWithArgs[Any]: ...

    @classmethod
    def from_query(
        cls,
        query: str | Queryable[_T_ql],
        args: Any,
        kwargs: Any,
    ) -> QueryWithArgs[Any]:
        if type(query) is str or isinstance(query, str):
            return cls(query, None, args, kwargs)

        try:
            eql = query.__edgeql__
        except AttributeError:
            pass
        else:
            return_type, query = eql()
            return cls(query, return_type, args, kwargs)

        raise ValueError("unsupported query type")


class QueryCache(typing.NamedTuple):
    codecs_registry: protocol.CodecsRegistry
    query_cache: protocol.LRUMapping


class QueryOptions(typing.NamedTuple):
    output_format: protocol.OutputFormat
    expect_one: bool
    required_one: bool


@dataclasses.dataclass(kw_only=True, frozen=True)
class BaseQueryContext(Generic[_T_ql]):
    query: QueryWithArgs[_T_ql]
    cache: QueryCache
    query_options: QueryOptions
    retry_options: options.RetryOptions | None
    state: options.State | None
    warning_handler: options.WarningHandler
    annotations: dict[str, str]
    transaction_options: options.TransactionOptions | None

    def lower(
        self, *, allow_capabilities: enums.Capability
    ) -> protocol.ExecuteContext:
        return protocol.ExecuteContext(
            query=self.query.query,
            return_type=self.query.return_type,
            args=self.query.args,
            kwargs=self.query.kwargs,
            reg=self.cache.codecs_registry,
            qc=self.cache.query_cache,
            input_language=self.query.input_language,
            output_format=self.query_options.output_format,
            expect_one=self.query_options.expect_one,
            required_one=self.query_options.required_one,
            inline_typenames=self.query.return_type is not None,
            allow_capabilities=allow_capabilities,
            state=self.state.as_dict() if self.state else None,
            annotations=self.annotations,
            transaction_options=self.transaction_options,
        )


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueryContext(BaseQueryContext[_T_ql]):
    query_options: QueryOptions = QueryOptions(
        output_format=protocol.OutputFormat.BINARY,
        expect_one=False,
        required_one=False,
    )


@dataclasses.dataclass(kw_only=True, frozen=True)
class QuerySingleContext(BaseQueryContext[_T_ql]):
    query_options: QueryOptions = QueryOptions(
        output_format=protocol.OutputFormat.BINARY,
        expect_one=True,
        required_one=False,
    )


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueryRequiredSingleContext(BaseQueryContext[_T_ql]):
    query_options: QueryOptions = QueryOptions(
        output_format=protocol.OutputFormat.BINARY,
        expect_one=True,
        required_one=True,
    )


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueryJsonContext(BaseQueryContext[_T_ql]):
    query_options: QueryOptions = QueryOptions(
        output_format=protocol.OutputFormat.JSON,
        expect_one=False,
        required_one=False,
    )


@dataclasses.dataclass(kw_only=True, frozen=True)
class QuerySingleJsonContext(BaseQueryContext[_T_ql]):
    query_options: QueryOptions = QueryOptions(
        output_format=protocol.OutputFormat.JSON,
        expect_one=True,
        required_one=False,
    )


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueryRequiredSingleJsonContext(BaseQueryContext[_T_ql]):
    query_options: QueryOptions = QueryOptions(
        output_format=protocol.OutputFormat.JSON,
        expect_one=True,
        required_one=True,
    )


@dataclasses.dataclass(kw_only=True, frozen=True)
class ExecuteContext(Generic[_T_ql]):
    query: QueryWithArgs[_T_ql]
    cache: QueryCache
    retry_options: options.RetryOptions | None
    state: options.State | None
    warning_handler: options.WarningHandler
    annotations: dict[str, str]
    transaction_options: options.TransactionOptions | None

    def lower(
        self, *, allow_capabilities: enums.Capability
    ) -> protocol.ExecuteContext:
        return protocol.ExecuteContext(
            query=self.query.query,
            args=self.query.args,
            kwargs=self.query.kwargs,
            reg=self.cache.codecs_registry,
            qc=self.cache.query_cache,
            input_language=self.query.input_language,
            output_format=protocol.OutputFormat.NONE,
            allow_capabilities=allow_capabilities,
            state=self.state.as_dict() if self.state else None,
            annotations=self.annotations,
            transaction_options=self.transaction_options,
            return_type=None,
        )


@dataclasses.dataclass
class DescribeContext:
    query: str
    state: options.State | None
    inject_type_names: bool
    input_language: protocol.InputLanguage
    output_format: protocol.OutputFormat
    expect_one: bool

    def lower(
        self, *, allow_capabilities: enums.Capability
    ) -> protocol.ExecuteContext:
        return protocol.ExecuteContext(
            query=self.query,
            args=None,
            kwargs=None,
            return_type=None,
            reg=protocol.CodecsRegistry(),
            qc=protocol.LRUMapping(maxsize=1),
            input_language=self.input_language,
            output_format=self.output_format,
            expect_one=self.expect_one,
            inline_typenames=self.inject_type_names,
            allow_capabilities=allow_capabilities,
            state=self.state.as_dict() if self.state else None,
        )


@dataclasses.dataclass
class DescribeResult:
    input_type: describe.AnyType | None
    output_type: describe.AnyType | None
    output_cardinality: enums.Cardinality
    capabilities: enums.Capability


class BaseReadOnlyExecutor(abc.ABC):
    __slots__ = ()

    @abc.abstractmethod
    def _get_query_cache(self) -> QueryCache: ...

    @abc.abstractmethod
    def _get_retry_options(self) -> options.RetryOptions | None: ...

    @abc.abstractmethod
    def _get_state(self) -> options.State: ...

    @abc.abstractmethod
    def _get_warning_handler(self) -> options.WarningHandler: ...

    def _get_annotations(self) -> dict[str, str]:
        return {}


class ReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @overload
    def _query(self, query_context: QueryContext[_T_ql]) -> list[_T_ql]: ...

    @overload
    def _query(
        self, query_context: QuerySingleContext[_T_ql]
    ) -> _T_ql | None: ...

    @overload
    def _query(
        self, query_context: QueryRequiredSingleContext[_T_ql]
    ) -> _T_ql: ...

    @overload
    def _query(self, query_context: QueryJsonContext[_T_ql]) -> str: ...

    @overload
    def _query(self, query_context: QuerySingleJsonContext[_T_ql]) -> str: ...

    @overload
    def _query(
        self, query_context: QueryRequiredSingleJsonContext[_T_ql]
    ) -> str: ...

    @abc.abstractmethod
    def _query(
        self, query_context: BaseQueryContext[_T_ql]
    ) -> list[_T_ql] | _T_ql | str | None: ...

    @abc.abstractmethod
    def _get_active_tx_options(
        self,
    ) -> options.TransactionOptions | None: ...

    @typing.overload
    def query(
        self,
        query: Queryable[_T_ql],
        /,
        **kwargs: Any,
    ) -> list[_T_ql]: ...

    @typing.overload
    def query(
        self,
        query: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> list[Any]: ...

    def query(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> list[Any] | list[_T_ql]:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    def get(self, query: str, /, **kwargs: Any) -> Any: ...

    @typing.overload
    def get(self, query: str, default: _T_get, /, **kwargs: Any) -> _T_get: ...

    @typing.overload
    def get(
        self,
        query: Queryable[_T_ql],
        /,
        **kwargs: Any,
    ) -> _T_ql: ...

    @typing.overload
    def get(
        self,
        query: Queryable[_T_ql],
        default: _T_ql,  # type: ignore [misc]
        /,
        **kwargs: Any,
    ) -> _T_ql: ...

    def get(
        self,
        query: str | Queryable[_T_ql],
        default: Any = _unset,
        /,
        **kwargs: Any,
    ) -> _T_ql | Any:
        if hasattr(query, "__edgeql__"):
            query = query.__gel_assert_single__(  # type: ignore
                message=(
                    "client.get() requires 0 or 1 returned objects, "
                    "got more than that"
                )
            )
        if default is _unset:
            try:
                return self.query_required_single(query, **kwargs)
            except errors.NoDataError:
                raise errors.NoDataError(
                    "client.get() without a default expects "
                    "exactly one result, got none"
                ) from None
        else:
            result = self.query_single(query, **kwargs)
            if result is None:
                return default
            else:
                return result

    @typing.overload
    def query_single(
        self, query: Queryable[_T_ql], **kwargs: Any
    ) -> _T_ql | None: ...

    @typing.overload
    def query_single(
        self, query: str, *args: Any, **kwargs: Any
    ) -> Any | None: ...

    def query_single(
        self,
        query: str | Queryable[_T_ql],
        *args: Any,
        **kwargs: Any,
    ) -> _T_ql | Any:
        return self._query(
            QuerySingleContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    def query_required_single(
        self, query: Queryable[_T_ql], **kwargs: Any
    ) -> _T_ql: ...

    @typing.overload
    def query_required_single(
        self, query: str, *args: Any, **kwargs: Any
    ) -> Any: ...

    def query_required_single(
        self,
        query: str | Queryable[_T_ql],
        *args: Any,
        **kwargs: Any,
    ) -> _T_ql | Any:
        return self._query(
            QueryRequiredSingleContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_json(
        self, query: str | Queryable[_T_ql], *args: Any, **kwargs: Any
    ) -> str:
        return self._query(
            QueryJsonContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_single_json(
        self,
        query: str | Queryable[_T_ql],
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return self._query(
            QuerySingleJsonContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_required_single_json(
        self,
        query: str | Queryable[_T_ql],
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return self._query(
            QueryRequiredSingleJsonContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_sql(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> list[datatypes.Record]:  # type: ignore
        return self._query(
            QueryContext(
                query=QueryWithArgs(
                    query,
                    None,
                    args,
                    kwargs,
                    input_language=protocol.InputLanguage.SQL,
                ),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_graphql_json(
        self,
        query: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return self._query(
            QuerySingleJsonContext(
                query=QueryWithArgs(
                    query,
                    None,
                    args,
                    kwargs,
                    input_language=protocol.InputLanguage.GRAPHQL,
                ),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @abc.abstractmethod
    def _execute(self, execute_context: ExecuteContext[_T_ql]) -> None: ...

    def execute(self, commands: str, *args: Any, **kwargs: Any) -> None:
        self._execute(
            ExecuteContext(
                query=QueryWithArgs(commands, None, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def execute_sql(
        self,
        commands: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._execute(
            ExecuteContext(
                query=QueryWithArgs(
                    commands,
                    None,
                    args,
                    kwargs,
                    input_language=protocol.InputLanguage.SQL,
                ),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )


class Executor(ReadOnlyExecutor):
    """Subclasses can execute both read-only and modification queries"""

    __slots__ = ()


class AsyncIOReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @overload
    async def _query(
        self, query_context: QueryContext[_T_ql]
    ) -> list[_T_ql]: ...

    @overload
    async def _query(
        self, query_context: QuerySingleContext[_T_ql]
    ) -> _T_ql | None: ...

    @overload
    async def _query(
        self, query_context: QueryRequiredSingleContext[_T_ql]
    ) -> _T_ql: ...

    @overload
    async def _query(self, query_context: QueryJsonContext[_T_ql]) -> str: ...

    @overload
    async def _query(
        self, query_context: QuerySingleJsonContext[_T_ql]
    ) -> str: ...

    @overload
    async def _query(
        self, query_context: QueryRequiredSingleJsonContext[_T_ql]
    ) -> str: ...

    @abc.abstractmethod
    async def _query(self, query_context: BaseQueryContext[_T_ql]) -> Any: ...

    @abc.abstractmethod
    def _get_active_tx_options(
        self,
    ) -> options.TransactionOptions | None: ...

    @typing.overload
    async def query(
        self,
        query: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> list[Any]: ...

    @typing.overload
    async def query(
        self,
        query: Queryable[_T_ql],
        /,
        **kwargs: Any,
    ) -> list[_T_ql]: ...

    async def query(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> list[_T_ql] | list[Any]:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    async def get(
        self,
        query: str,
        /,
        **kwargs: Any,
    ) -> Any: ...

    @typing.overload
    async def get(
        self,
        query: str,
        default: _T_get,
        /,
        **kwargs: Any,
    ) -> Any | _T_get: ...

    @typing.overload
    async def get(
        self,
        query: Queryable[_T_ql],
        /,
        **kwargs: Any,
    ) -> _T_ql: ...

    @typing.overload
    async def get(
        self,
        query: Queryable[_T_ql],
        default: _T_ql,  # type: ignore [misc]
        /,
        **kwargs: Any,
    ) -> _T_ql: ...

    async def get(
        self,
        query: str | Queryable[_T_ql],
        default: Any = _unset,
        /,
        **kwargs: Any,
    ) -> _T_ql | Any:
        if hasattr(query, "__edgeql__"):
            query = query.__gel_assert_single__(  # type: ignore
                message=(
                    "client.get() requires 0 or 1 returned objects, "
                    "got more than that"
                )
            )
        if default is _unset:
            try:
                return await self.query_required_single(query, **kwargs)
            except errors.NoDataError:
                raise errors.NoDataError(
                    "client.get() without a default expects "
                    "exactly one result, got none"
                ) from None
        else:
            result = await self.query_single(query, **kwargs)
            if result is None:
                return default
            else:
                return result

    @typing.overload
    async def query_single(
        self, query: Queryable[_T_ql], **kwargs: Any
    ) -> _T_ql | None: ...

    @typing.overload
    async def query_single(
        self, query: str, *args: Any, **kwargs: Any
    ) -> Any | None: ...

    async def query_single(
        self,
        query: str | Queryable[_T_ql],
        *args: Any,
        **kwargs: Any,
    ) -> _T_ql | Any:
        return await self._query(
            QuerySingleContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    async def query_required_single(
        self,
        query: Queryable[_T_ql],
        /,
        **kwargs: Any,
    ) -> _T_ql: ...

    @typing.overload
    async def query_required_single(
        self,
        query: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...

    async def query_required_single(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _T_ql | Any:
        return await self._query(
            QueryRequiredSingleContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_json(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return await self._query(
            QueryJsonContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_single_json(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return await self._query(
            QuerySingleJsonContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_required_single_json(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return await self._query(
            QueryRequiredSingleJsonContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_sql(
        self,
        query: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return await self._query(
            QueryContext(
                query=QueryWithArgs(
                    query,
                    None,
                    args,
                    kwargs,
                    input_language=protocol.InputLanguage.SQL,
                ),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_graphql_json(
        self,
        query: str | Queryable[_T_ql],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        return await self._query(
            QuerySingleJsonContext(
                query=QueryWithArgs(
                    query,
                    None,
                    args,
                    kwargs,
                    input_language=protocol.InputLanguage.GRAPHQL,
                ),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @abc.abstractmethod
    async def _execute(
        self, execute_context: ExecuteContext[_T_ql]
    ) -> None: ...

    async def execute(
        self,
        commands: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        await self._execute(
            ExecuteContext(
                query=QueryWithArgs(commands, None, args, kwargs),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def execute_sql(
        self,
        commands: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        await self._execute(
            ExecuteContext(
                query=QueryWithArgs(
                    commands,
                    None,
                    args,
                    kwargs,
                    input_language=protocol.InputLanguage.SQL,
                ),
                cache=self._get_query_cache(),
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )


class AsyncIOExecutor(AsyncIOReadOnlyExecutor):
    """Subclasses can execute both read-only and modification queries"""

    __slots__ = ()
