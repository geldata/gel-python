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
import abc
import dataclasses
import typing

from . import datatypes
from . import describe
from . import enums
from . import errors
from . import options
from .protocol import protocol  # type: ignore

__all__ = (
    "QueryWithArgs",
    "QueryCache",
    "QueryOptions",
    "QueryContext",
    "Executor",
    "ExecuteContext",
    "AsyncIOExecutor",
    "ReadOnlyExecutor",
    "AsyncIOReadOnlyExecutor",
    "DescribeContext",
    "DescribeResult",
)


T_ql = typing.TypeVar("T_ql", covariant=True)
T_get = typing.TypeVar("T_get")


class QueryBuilderExpression(typing.Protocol[T_ql]):
    def __edgeql__(self) -> typing.Tuple[type[T_ql], str]: ...


AnyEdgeQLQuery = QueryBuilderExpression | str


_unset = object()


class QueryWithArgs(typing.NamedTuple):
    query: str
    return_type: typing.Type | None
    args: typing.Tuple
    kwargs: typing.Dict[str, typing.Any]
    input_language: protocol.InputLanguage = protocol.InputLanguage.EDGEQL

    @classmethod
    def from_query(cls, query: AnyEdgeQLQuery, args, kwargs) -> QueryWithArgs:
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


class QueryContext(typing.NamedTuple):
    query: QueryWithArgs
    cache: QueryCache
    query_options: QueryOptions
    retry_options: typing.Optional[options.RetryOptions]
    state: typing.Optional[options.State]
    warning_handler: options.WarningHandler
    annotations: typing.Dict[str, str]
    transaction_options: typing.Optional[options.TransactionOptions]

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
            allow_capabilities=allow_capabilities,
            state=self.state.as_dict() if self.state else None,
            annotations=self.annotations,
            transaction_options=self.transaction_options,
        )


class ExecuteContext(typing.NamedTuple):
    query: QueryWithArgs
    cache: QueryCache
    retry_options: typing.Optional[options.RetryOptions]
    state: typing.Optional[options.State]
    warning_handler: options.WarningHandler
    annotations: typing.Dict[str, str]
    transaction_options: typing.Optional[options.TransactionOptions]

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
    state: typing.Optional[options.State]
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
    input_type: typing.Optional[describe.AnyType]
    output_type: typing.Optional[describe.AnyType]
    output_cardinality: enums.Cardinality
    capabilities: enums.Capability


_query_opts = QueryOptions(
    output_format=protocol.OutputFormat.BINARY,
    expect_one=False,
    required_one=False,
)
_query_single_opts = QueryOptions(
    output_format=protocol.OutputFormat.BINARY,
    expect_one=True,
    required_one=False,
)
_query_required_single_opts = QueryOptions(
    output_format=protocol.OutputFormat.BINARY,
    expect_one=True,
    required_one=True,
)
_query_json_opts = QueryOptions(
    output_format=protocol.OutputFormat.JSON,
    expect_one=False,
    required_one=False,
)
_query_single_json_opts = QueryOptions(
    output_format=protocol.OutputFormat.JSON,
    expect_one=True,
    required_one=False,
)
_query_required_single_json_opts = QueryOptions(
    output_format=protocol.OutputFormat.JSON,
    expect_one=True,
    required_one=True,
)


class BaseReadOnlyExecutor(abc.ABC):
    __slots__ = ()

    @abc.abstractmethod
    def _get_query_cache(self) -> QueryCache: ...

    @abc.abstractmethod
    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]: ...

    @abc.abstractmethod
    def _get_state(self) -> options.State: ...

    @abc.abstractmethod
    def _get_warning_handler(self) -> options.WarningHandler: ...

    def _get_annotations(self) -> typing.Dict[str, str]:
        return {}


class ReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @abc.abstractmethod
    def _query(self, query_context: QueryContext) -> typing.Any: ...

    @abc.abstractmethod
    def _get_active_tx_options(
        self,
    ) -> typing.Optional[options.TransactionOptions]: ...

    @typing.overload
    def query(
        self, query: type[QueryBuilderExpression[T_ql]], **kwargs: typing.Any
    ) -> list[T_ql]: ...

    @typing.overload
    def query(
        self, query: str, *args: typing.Any, **kwargs: typing.Any
    ) -> list[typing.Any]: ...

    def query(
        self, query: AnyEdgeQLQuery, *args: typing.Any, **kwargs: typing.Any
    ) -> list[typing.Any]:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    def get(
        self,
        query: type[QueryBuilderExpression[T_ql]],
        /,
        **kwargs: typing.Any,
    ) -> T_ql: ...

    @typing.overload
    def get(
        self,
        query: type[QueryBuilderExpression[T_ql]],
        default: T_get,
        /,
        **kwargs: typing.Any,
    ) -> T_ql | T_get: ...

    @typing.overload
    def get(self, query: str, /, **kwargs: typing.Any) -> typing.Any: ...

    @typing.overload
    def get(
        self, query: str, default: T_get, /, **kwargs: typing.Any
    ) -> typing.Any | T_get: ...

    def get(
        self,
        query: AnyEdgeQLQuery,
        default: typing.Any = _unset,
        **kwargs: typing.Any,
    ) -> typing.Any:
        if hasattr(query, "__edgeql__"):
            query = query.__gel_assert_single__(
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
        self, query: type[QueryBuilderExpression[T_ql]], **kwargs: typing.Any
    ) -> T_ql | None: ...

    @typing.overload
    def query_single(
        self, query: str, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any | None: ...

    def query_single(
        self, query: AnyEdgeQLQuery, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_single_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    def query_required_single(
        self, query: type[QueryBuilderExpression[T_ql]], **kwargs: typing.Any
    ) -> T_ql: ...

    @typing.overload
    def query_required_single(
        self, query: str, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any: ...

    def query_required_single(
        self, query: AnyEdgeQLQuery, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_required_single_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_json(self, query: AnyEdgeQLQuery, *args, **kwargs) -> str:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_json_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_single_json(self, query: AnyEdgeQLQuery, *args, **kwargs) -> str:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_single_json_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_required_single_json(
        self, query: AnyEdgeQLQuery, *args, **kwargs
    ) -> str:
        return self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_required_single_json_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    def query_sql(self, query: str, *args, **kwargs) -> list[datatypes.Record]:
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
                query_options=_query_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @abc.abstractmethod
    def _execute(self, execute_context: ExecuteContext): ...

    def execute(self, commands: str, *args, **kwargs) -> None:
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

    def execute_sql(self, commands: str, *args, **kwargs) -> None:
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

    @abc.abstractmethod
    async def _query(self, query_context: QueryContext) -> typing.Any: ...

    @abc.abstractmethod
    def _get_active_tx_options(
        self,
    ) -> typing.Optional[options.TransactionOptions]: ...

    @typing.overload
    async def query(
        self, query: type[QueryBuilderExpression[T_ql]], **kwargs: typing.Any
    ) -> list[T_ql]: ...

    @typing.overload
    async def query(
        self, query: str, *args: typing.Any, **kwargs: typing.Any
    ) -> list[typing.Any]: ...

    async def query(self, query: AnyEdgeQLQuery, *args, **kwargs) -> list:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_opts,
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
        query: type[QueryBuilderExpression[T_ql]],
        /,
        **kwargs: typing.Any,
    ) -> T_ql: ...

    @typing.overload
    async def get(
        self,
        query: type[QueryBuilderExpression[T_ql]],
        default: T_get,
        /,
        **kwargs: typing.Any,
    ) -> T_ql | T_get: ...

    @typing.overload
    async def get(self, query: str, /, **kwargs: typing.Any) -> typing.Any: ...

    @typing.overload
    async def get(
        self, query: str, default: T_get, /, **kwargs: typing.Any
    ) -> typing.Any | T_get: ...

    async def get(
        self,
        query: AnyEdgeQLQuery,
        default: typing.Any = _unset,
        **kwargs: typing.Any,
    ) -> typing.Any:
        if hasattr(query, "__edgeql__"):
            query = query.__gel_assert_single__(
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
        self, query: type[QueryBuilderExpression[T_ql]], **kwargs: typing.Any
    ) -> T_ql | None: ...

    @typing.overload
    async def query_single(
        self, query: str, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any | None: ...

    async def query_single(
        self, query: AnyEdgeQLQuery, *args, **kwargs
    ) -> typing.Any:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_single_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @typing.overload
    async def query_required_single(
        self, query: type[QueryBuilderExpression[T_ql]], **kwargs: typing.Any
    ) -> T_ql: ...

    @typing.overload
    async def query_required_single(
        self, query: str, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Any: ...

    async def query_required_single(
        self, query: AnyEdgeQLQuery, *args, **kwargs
    ) -> typing.Any:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_required_single_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_json(self, query: AnyEdgeQLQuery, *args, **kwargs) -> str:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_json_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_single_json(
        self, query: AnyEdgeQLQuery, *args, **kwargs
    ) -> str:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_single_json_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_required_single_json(
        self, query: AnyEdgeQLQuery, *args, **kwargs
    ) -> str:
        return await self._query(
            QueryContext(
                query=QueryWithArgs.from_query(query, args, kwargs),
                cache=self._get_query_cache(),
                query_options=_query_required_single_json_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    async def query_sql(self, query: str, *args, **kwargs) -> typing.Any:
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
                query_options=_query_opts,
                retry_options=self._get_retry_options(),
                state=self._get_state(),
                transaction_options=self._get_active_tx_options(),
                warning_handler=self._get_warning_handler(),
                annotations=self._get_annotations(),
            )
        )

    @abc.abstractmethod
    async def _execute(self, execute_context: ExecuteContext) -> None: ...

    async def execute(self, commands: str, *args, **kwargs) -> None:
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

    async def execute_sql(self, commands: str, *args, **kwargs) -> None:
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
