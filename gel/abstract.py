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
from . import options
from .protocol import protocol

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


class QueryWithArgs(typing.NamedTuple):
    query: str
    args: typing.Tuple
    kwargs: typing.Dict[str, typing.Any]
    input_language: protocol.InputLanguage = protocol.InputLanguage.EDGEQL


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
            output_format=self.query_options.output_format,
            expect_one=self.query_options.expect_one,
            required_one=self.query_options.required_one,
            allow_capabilities=allow_capabilities,
            state=self.state.as_dict() if self.state else None,
            annotations=self.annotations,
        )


class ExecuteContext(typing.NamedTuple):
    query: QueryWithArgs
    cache: QueryCache
    retry_options: typing.Optional[options.RetryOptions]
    state: typing.Optional[options.State]
    warning_handler: options.WarningHandler
    annotations: typing.Dict[str, str]

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
    def _get_query_cache(self) -> QueryCache:
        ...

    @abc.abstractmethod
    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]:
        ...

    @abc.abstractmethod
    def _get_state(self) -> options.State:
        ...

    @abc.abstractmethod
    def _get_warning_handler(self) -> options.WarningHandler:
        ...

    def _get_annotations(self) -> typing.Dict[str, str]:
        return {}


class ReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @abc.abstractmethod
    def _query(self, query_context: QueryContext):
        ...

    def query(self, query: str, *args, **kwargs) -> list:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def query_single(
        self, query: str, *args, **kwargs
    ) -> typing.Union[typing.Any, None]:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def query_required_single(self, query: str, *args, **kwargs) -> typing.Any:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def query_json(self, query: str, *args, **kwargs) -> str:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_json_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def query_single_json(self, query: str, *args, **kwargs) -> str:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_json_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def query_required_single_json(self, query: str, *args, **kwargs) -> str:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_json_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def query_sql(self, query: str, *args, **kwargs) -> list[datatypes.Record]:
        return self._query(QueryContext(
            query=QueryWithArgs(
                query,
                args,
                kwargs,
                input_language=protocol.InputLanguage.SQL,
            ),
            cache=self._get_query_cache(),
            query_options=_query_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    @abc.abstractmethod
    def _execute(self, execute_context: ExecuteContext):
        ...

    def execute(self, commands: str, *args, **kwargs) -> None:
        self._execute(ExecuteContext(
            query=QueryWithArgs(commands, args, kwargs),
            cache=self._get_query_cache(),
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    def execute_sql(self, commands: str, *args, **kwargs) -> None:
        self._execute(ExecuteContext(
            query=QueryWithArgs(
                commands,
                args,
                kwargs,
                input_language=protocol.InputLanguage.SQL,
            ),
            cache=self._get_query_cache(),
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))


class Executor(ReadOnlyExecutor):
    """Subclasses can execute both read-only and modification queries"""

    __slots__ = ()


class AsyncIOReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @abc.abstractmethod
    async def _query(self, query_context: QueryContext):
        ...

    async def query(self, query: str, *args, **kwargs) -> list:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def query_single(self, query: str, *args, **kwargs) -> typing.Any:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def query_required_single(
        self,
        query: str,
        *args,
        **kwargs
    ) -> typing.Any:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def query_json(self, query: str, *args, **kwargs) -> str:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_json_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def query_single_json(self, query: str, *args, **kwargs) -> str:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_json_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def query_required_single_json(
        self,
        query: str,
        *args,
        **kwargs
    ) -> str:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_json_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def query_sql(self, query: str, *args, **kwargs) -> typing.Any:
        return await self._query(QueryContext(
            query=QueryWithArgs(
                query,
                args,
                kwargs,
                input_language=protocol.InputLanguage.SQL,
            ),
            cache=self._get_query_cache(),
            query_options=_query_opts,
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    @abc.abstractmethod
    async def _execute(self, execute_context: ExecuteContext) -> None:
        ...

    async def execute(self, commands: str, *args, **kwargs) -> None:
        await self._execute(ExecuteContext(
            query=QueryWithArgs(commands, args, kwargs),
            cache=self._get_query_cache(),
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))

    async def execute_sql(self, commands: str, *args, **kwargs) -> None:
        await self._execute(ExecuteContext(
            query=QueryWithArgs(
                commands,
                args,
                kwargs,
                input_language=protocol.InputLanguage.SQL,
            ),
            cache=self._get_query_cache(),
            retry_options=self._get_retry_options(),
            state=self._get_state(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))


class AsyncIOExecutor(AsyncIOReadOnlyExecutor):
    """Subclasses can execute both read-only and modification queries"""

    __slots__ = ()
