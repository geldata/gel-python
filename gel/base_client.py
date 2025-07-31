#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Generic, TypeVar, Protocol
from typing_extensions import Self

import abc
import dataclasses
import random
import time
import typing

from . import abstract
from . import con_utils
from . import enums
from . import errors
from . import options as _options
from .protocol import protocol  # pyright: ignore [reportAttributeAccessIssue]


QUERY_CACHE_SIZE = 1000


class EventProtocol(Protocol):
    def __init__(self) -> None: ...
    def is_set(self) -> bool: ...
    def set(self) -> None: ...
    def clear(self) -> None: ...


_T_co = typing.TypeVar("_T_co", covariant=True)
_T_Event = TypeVar("_T_Event", bound=EventProtocol, covariant=True)


class BaseConnection(Generic[_T_Event], metaclass=abc.ABCMeta):
    _protocol: Any
    _holder: PoolConnectionHolder[Self, _T_Event] | None
    _addr: str | tuple[str, int] | None
    _addrs: typing.Iterable[str | tuple[str, int]]
    _config: con_utils.ClientConfiguration
    _params: con_utils.ResolvedConnectConfig
    _log_listeners: set[typing.Callable[[Self, errors.EdgeDBMessage], object]]
    __slots__ = (
        "__weakref__",
        "_protocol",
        "_addr",
        "_addrs",
        "_config",
        "_params",
        "_log_listeners",
        "_holder",
    )

    def __init__(
        self,
        addrs: typing.Iterable[str | tuple[str, int]],
        config: con_utils.ClientConfiguration,
        params: con_utils.ResolvedConnectConfig,
    ) -> None:
        self._addr = None
        self._protocol = None
        self._addrs = addrs
        self._config = config
        self._params = params
        self._log_listeners = set()
        self._holder = None

    @abc.abstractmethod
    def _dispatch_log_message(self, msg: errors.EdgeDBMessage) -> None: ...

    def _on_log_message(self, msg: errors.EdgeDBMessage) -> None:
        if self._log_listeners:
            self._dispatch_log_message(msg)

    def connected_addr(self) -> str | tuple[str, int] | None:
        return self._addr

    def _get_last_status(self) -> str | None:
        if self._protocol is None:
            return None
        status = self._protocol.last_status
        if status is not None:
            status = status.decode()
        return status  # type: ignore [no-any-return]

    def _cleanup(self) -> None:
        self._log_listeners.clear()
        if self._holder:
            self._holder._release_on_close()
            self._holder = None

    def add_log_listener(
        self,
        callback: typing.Callable[[Self, errors.EdgeDBMessage], None],
    ) -> None:
        """Add a listener for EdgeDB log messages.

        :param callable callback:
            A callable receiving the following arguments:
            **connection**: a Connection the callback is registered with;
            **message**: the `gel.EdgeDBMessage` message.
        """
        self._log_listeners.add(callback)

    def remove_log_listener(
        self,
        callback: typing.Callable[[Self, errors.EdgeDBMessage], None],
    ) -> None:
        """Remove a listening callback for log messages."""
        self._log_listeners.discard(callback)

    @property
    def dbname(self) -> str:
        return self._params.database  # type: ignore [no-any-return]

    @property
    def branch(self) -> str:
        return self._params.branch  # type: ignore [no-any-return]

    @abc.abstractmethod
    def is_closed(self) -> bool: ...

    @abc.abstractmethod
    async def connect_addr(
        self, addr: str | tuple[str, int], timeout: float
    ) -> None: ...

    @abc.abstractmethod
    async def sleep(self, seconds: float) -> None: ...

    async def connect(self, *, single_attempt: bool = False) -> None:
        start = time.monotonic()
        if single_attempt:
            max_time = 0
        else:
            max_time = start + self._config.wait_until_available  # type: ignore [assignment]
        iteration = 1

        while True:
            for addr in self._addrs:
                try:
                    await self.connect_addr(addr, self._config.connect_timeout)
                except TimeoutError as e:
                    if iteration == 1 or time.monotonic() < max_time:
                        continue
                    else:
                        raise errors.ClientConnectionTimeoutError(
                            f"connecting to {addr} failed in"
                            f" {self._config.connect_timeout} sec"
                        ) from e
                except errors.ClientConnectionError as e:
                    if e.has_tag(errors.SHOULD_RECONNECT) and (
                        iteration == 1 or time.monotonic() < max_time
                    ):
                        continue
                    nice_err = e.__class__(
                        con_utils.render_client_no_connection_error(
                            e,
                            addr,
                            attempts=iteration,
                            duration=time.monotonic() - start,
                        )
                    )
                    raise nice_err from e.__cause__
                else:
                    return

            iteration += 1
            await self.sleep(0.01 + random.random() * 0.2)

    async def privileged_execute(
        self, execute_context: abstract.ExecuteContext[Any]
    ) -> None:
        if self._protocol.is_legacy:
            await self._protocol.legacy_simple_query(
                execute_context.query.query, enums.Capability.ALL
            )
        else:
            await self._protocol.execute(
                execute_context.lower(allow_capabilities=enums.Capability.ALL)
            )

    def is_in_transaction(self) -> bool:
        """Return True if Connection is currently inside a transaction.

        :return bool: True if inside transaction, False otherwise.
        """
        return self._protocol.is_in_transaction()  # type: ignore [no-any-return]

    def get_settings(self) -> dict[str, Any]:
        return self._protocol.get_settings()  # type: ignore [no-any-return]

    async def _retry_operation(
        self,
        func: typing.Callable[[], typing.Awaitable[Any]],
        retry_options: _options.RetryOptions | None,
        ctx: Any,
    ) -> Any:
        reconnect = False
        i = 0
        while True:
            i += 1
            try:
                if reconnect:
                    await self.connect(single_attempt=True)
                return await func()

            except errors.EdgeDBError as e:
                if retry_options is None:
                    raise
                if not e.has_tag(errors.SHOULD_RETRY):
                    raise e
                # A query is read-only if it has no capabilities i.e.
                # capabilities == 0. Read-only queries are safe to retry.
                # Explicit transaction conflicts as well.
                if ctx.capabilities != 0 and not isinstance(
                    e, errors.TransactionConflictError
                ):
                    raise e
                rule = retry_options.get_rule_for_exception(e)
                if i >= rule.attempts:
                    raise e
                await self.sleep(rule.backoff(i))
                reconnect = self.is_closed()

    async def raw_query(
        self, query_context: abstract.BaseQueryContext[Any]
    ) -> Any:
        if self.is_closed():
            await self.connect()

        if self._protocol.is_legacy:
            allow_capabilities = enums.Capability.LEGACY_EXECUTE
        else:
            allow_capabilities = enums.Capability.EXECUTE
        ctx = query_context.lower(allow_capabilities=allow_capabilities)

        async def _inner() -> Any:
            if self._protocol.is_legacy:
                return await self._protocol.legacy_execute_anonymous(ctx)
            else:
                res = await self._protocol.query(ctx)
                if ctx.warnings:
                    res = query_context.warning_handler(ctx.warnings, res)
                return res

        return await self._retry_operation(
            _inner, query_context.retry_options, ctx
        )

    async def _execute(
        self, execute_context: abstract.ExecuteContext[Any]
    ) -> None:
        if self._protocol.is_legacy:
            if execute_context.query.args or execute_context.query.kwargs:
                raise errors.InterfaceError(
                    "Legacy protocol doesn't support arguments in execute()"
                )
            await self._protocol.legacy_simple_query(
                execute_context.query.query, enums.Capability.LEGACY_EXECUTE
            )
        else:
            ctx = execute_context.lower(
                allow_capabilities=enums.Capability.EXECUTE
            )

            async def _inner() -> None:
                res = await self._protocol.execute(ctx)
                if ctx.warnings:
                    execute_context.warning_handler(ctx.warnings, res)

            await self._retry_operation(
                _inner, execute_context.retry_options, ctx
            )

    async def batch_query(
        self,
        ops: list[abstract.QueryContext[Any] | abstract.ExecuteContext[Any]],
    ) -> list[Any]:
        ctxs = [
            ctx.lower(
                allow_capabilities=(
                    enums.Capability.EXECUTE & ~enums.Capability.DDL
                )
            )
            for ctx in ops
        ]
        rv = await self._protocol.batch_execute(ctxs)
        return [
            op.warning_handler(ctx.warnings, res) if ctx.warnings else res
            for op, ctx, res in zip(ops, ctxs, rv, strict=False)
        ]

    async def describe(
        self, describe_context: abstract.DescribeContext
    ) -> abstract.DescribeResult:
        ctx = describe_context.lower(
            allow_capabilities=enums.Capability.EXECUTE
        )
        await self._protocol._parse(ctx)
        return abstract.DescribeResult(
            input_type=ctx.in_dc.make_type(describe_context),
            output_type=ctx.out_dc.make_type(describe_context),
            output_cardinality=enums.Cardinality(ctx.cardinality[0]),
            capabilities=ctx.capabilities,
        )

    def terminate(self) -> None:
        if not self.is_closed():
            try:
                self._protocol.abort()
            finally:
                self._cleanup()

    def __repr__(self) -> str:
        if self.is_closed():
            return "<{classname} [closed] {id:#x}>".format(
                classname=self.__class__.__name__, id=id(self)
            )
        else:
            return "<{classname} [connected to {addr}] {id:#x}>".format(
                classname=self.__class__.__name__,
                addr=self.connected_addr(),
                id=id(self),
            )


_T_Conn = TypeVar("_T_Conn", bound=BaseConnection[EventProtocol])


class PoolConnectionHolder(abc.ABC, Generic[_T_Conn, _T_Event]):
    __slots__ = (
        "_con",
        "_pool",
        "_release_event",
        "_timeout",
        "_generation",
    )
    _event_class: type[_T_Event]

    def __init__(self, pool: BasePoolImpl[_T_Conn, _T_Event]) -> None:
        self._pool = pool
        self._con: _T_Conn | None = None

        self._timeout: float | None = None
        self._generation: int | None = None

        self._release_event = self._event_class()
        self._release_event.set()

    @abc.abstractmethod
    async def close(
        self,
        *,
        wait: bool = True,
        timeout: float | None = None,
    ) -> None: ...

    @abc.abstractmethod
    async def wait_until_released(
        self, timeout: float | None = None
    ) -> None: ...

    async def connect(self) -> None:
        if self._con is not None:
            raise errors.InternalClientError(
                "PoolConnectionHolder.connect() called while another "
                "connection already exists"
            )

        self._con = await self._pool._get_new_connection()
        assert self._con._holder is None
        self._con._holder = self
        self._generation = self._pool._generation

    async def acquire(self) -> _T_Conn:
        if self._con is None or self._con.is_closed():
            self._con = None
            await self.connect()

        elif self._generation != self._pool._generation:
            # Connections have been expired, re-connect the holder.
            self._con._holder = None  # don't release the connection
            await self.close(wait=False)
            self._con = None
            await self.connect()

        self._release_event.clear()

        assert self._con is not None
        return self._con

    async def release(self, timeout: float | None) -> None:
        if self._release_event.is_set():
            raise errors.InternalClientError(
                "PoolConnectionHolder.release() called on "
                "a free connection holder"
            )

        if self._con is not None and self._con.is_closed():
            # This is usually the case when the connection is broken rather
            # than closed by the user, so we need to call _release_on_close()
            # here to release the holder back to the queue, because
            # self._con._cleanup() was never called. On the other hand, it is
            # safe to call self._release() twice - the second call is no-op.
            self._release_on_close()
            return

        self._timeout = None

        if self._generation != self._pool._generation:
            # The connection has expired because it belongs to
            # an older generation (BasePoolImpl.expire_connections() has
            # been called.)
            await self.close()
            return

        # Free this connection holder and invalidate the
        # connection proxy.
        self._release()

    def terminate(self) -> None:
        if self._con is not None:
            # AsyncIOConnection.terminate() will call _release_on_close() to
            # finish holder cleanup.
            self._con.terminate()

    def _release_on_close(self) -> None:
        self._release()
        self._con = None

    def _release(self) -> None:
        """Release this connection holder."""
        if self._release_event.is_set():
            # The holder is not checked out.
            return

        self._release_event.set()

        # Put ourselves back to the pool queue.
        self._pool._queue.put_nowait(self)


@dataclasses.dataclass
class ConnectionInfo:
    host: str
    port: int
    params: con_utils.ResolvedConnectConfig
    config: con_utils.ClientConfiguration


class BasePoolImpl(abc.ABC, Generic[_T_Conn, _T_Event]):
    __slots__ = (
        "_connect_args",
        "_codecs_registry",
        "_query_cache",
        "_tx_needs_serializable_cache",
        "_connection_factory",
        "_queue",
        "_user_max_concurrency",
        "_max_concurrency",
        "_first_connect_lock",
        "_working_addr",
        "_working_config",
        "_working_params",
        "_holders",
        "_initialized",
        "_initializing",
        "_closing",
        "_closed",
        "_generation",
    )

    _holder_class: type[PoolConnectionHolder[_T_Conn, _T_Event]]

    def __init__(
        self,
        connect_args: dict[str, Any],
        connection_factory: typing.Callable[..., _T_Conn],
        *,
        max_concurrency: int | None,
    ) -> None:
        self._connection_factory = connection_factory
        self._connect_args = connect_args
        self._codecs_registry = protocol.CodecsRegistry()
        self._query_cache = protocol.LRUMapping(maxsize=QUERY_CACHE_SIZE)
        # Whether a transaction() call from a particular source location
        # needs to use Serializable. See transaction.BaseRetry for details.
        self._tx_needs_serializable_cache = protocol.LRUMapping(
            maxsize=QUERY_CACHE_SIZE
        )

        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError(
                "max_concurrency is expected to be greater than zero"
            )

        self._user_max_concurrency = max_concurrency
        self._max_concurrency = max_concurrency if max_concurrency else 1

        self._holders: list[PoolConnectionHolder[_T_Conn, _T_Event]] = []
        self._queue: Any = None

        self._first_connect_lock: Any = None
        self._working_addr: str | tuple[str, int] | None = None
        self._working_config: con_utils.ClientConfiguration | None = None
        self._working_params: con_utils.ResolvedConnectConfig | None = None

        self._closing = False
        self._closed = False
        self._generation = 0

    @abc.abstractmethod
    def _ensure_initialized(self) -> None: ...

    @abc.abstractmethod
    def _set_queue_maxsize(self, maxsize: int) -> None: ...

    @abc.abstractmethod
    async def _maybe_get_first_connection(self) -> _T_Conn | None: ...

    @abc.abstractmethod
    async def acquire(self, timeout: float | None = None) -> _T_Conn: ...

    @abc.abstractmethod
    async def _release(
        self, connection: PoolConnectionHolder[_T_Conn, _T_Event]
    ) -> None: ...

    @abc.abstractmethod
    async def close(self, timeout: float | None = None) -> None: ...

    @property
    def codecs_registry(self) -> Any:
        return self._codecs_registry

    @property
    def query_cache(self) -> Any:
        return self._query_cache

    def _resize_holder_pool(self) -> None:
        resize_diff = self._max_concurrency - len(self._holders)

        if resize_diff > 0:
            if (
                self._queue is not None
                and self._queue.maxsize != self._max_concurrency
            ):
                self._set_queue_maxsize(self._max_concurrency)

            for _ in range(resize_diff):
                ch = self._holder_class(self)

                self._holders.append(ch)
                if self._queue is not None:
                    self._queue.put_nowait(ch)
        elif resize_diff < 0:
            # TODO: shrink the pool
            pass

    def get_max_concurrency(self) -> int:
        return self._max_concurrency

    def get_free_size(self) -> int:
        if self._queue is None:
            # Queue has not been initialized yet
            return self._max_concurrency

        return self._queue.qsize()  # type: ignore [no-any-return]

    def set_connect_args(
        self, dsn: str | None = None, **connect_kwargs: Any
    ) -> None:
        r"""Set the new connection arguments for this pool.

        The new connection arguments will be used for all subsequent
        new connection attempts.  Existing connections will remain until
        they expire. Use BasePoolImpl.expire_connections() to expedite
        the connection expiry.

        :param str dsn:
            Connection arguments specified using as a single string in
            the following format:
            ``gel://user:pass@host:port/database?option=value``.

        :param \*\*connect_kwargs:
            Keyword arguments for the
            :func:`~gel.asyncio_client.create_async_client` function.
        """

        connect_kwargs["dsn"] = dsn
        self._connect_args = connect_kwargs
        self._codecs_registry = protocol.CodecsRegistry()
        self._query_cache = protocol.LRUMapping(maxsize=QUERY_CACHE_SIZE)
        self._working_addr = None
        self._working_config = None
        self._working_params = None

    async def _get_first_connection(self) -> _T_Conn:
        # First connection attempt on this pool.
        connect_config, client_config = con_utils.parse_connect_arguments(
            **self._connect_args,
            # ToDos
            command_timeout=None,
            server_settings=None,
        )
        con = self._connection_factory(
            [connect_config.address], client_config, connect_config
        )
        await con.connect()
        self._working_addr = con.connected_addr()
        self._working_config = client_config
        self._working_params = connect_config

        if self._user_max_concurrency is None:
            suggested_concurrency = con.get_settings().get(
                "suggested_pool_concurrency"
            )
            if suggested_concurrency:
                self._max_concurrency = suggested_concurrency
                self._resize_holder_pool()
        return con

    async def _get_new_connection(self) -> _T_Conn:
        con = None
        if self._working_addr is None:
            con = await self._maybe_get_first_connection()
        if con is None:
            assert self._working_addr is not None
            # We've connected before and have a resolved address,
            # and parsed options and config.
            con = self._connection_factory(
                [self._working_addr],
                self._working_config,
                self._working_params,
            )
            await con.connect()

        return con

    async def release(self, connection: _T_Conn) -> None:
        if not isinstance(connection, BaseConnection):
            raise errors.InterfaceError(
                f"BasePoolImpl.release() received invalid connection: "
                f"{connection!r} does not belong to any connection pool"
            )

        ch = connection._holder
        if ch is None:
            # Already released, do nothing.
            return

        if ch._pool is not self:
            raise errors.InterfaceError(
                f"BasePoolImpl.release() received invalid connection: "
                f"{connection!r} is not a member of this pool"
            )

        return await self._release(ch)  # pyright: ignore [reportArgumentType]

    def terminate(self) -> None:
        """Terminate all connections in the pool."""
        if self._closed:
            return
        for ch in self._holders:
            ch.terminate()
        self._closed = True

    def expire_connections(self) -> None:
        """Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next query.
        """
        self._generation += 1

    async def ensure_connected(self) -> ConnectionInfo:
        self._ensure_initialized()

        for ch in self._holders:
            if ch._con is not None and not ch._con.is_closed():
                return self._make_connection_info()

        ch = self._holders[0]
        ch._con = None
        await ch.connect()

        return self._make_connection_info()

    def _make_connection_info(self) -> ConnectionInfo:
        assert self._working_addr is not None
        assert self._working_config is not None
        assert self._working_params is not None
        assert self._working_addr is not None
        assert self._working_config is not None
        assert self._working_params is not None
        if isinstance(self._working_addr, tuple):
            host, port = self._working_addr
        else:
            host = self._working_addr
            port = 5656  # default port
        return ConnectionInfo(
            host=host,
            port=port,
            config=self._working_config,
            params=self._working_params,
        )


class BaseClient(
    abstract.BaseReadOnlyExecutor,
    _options._OptionsMixin,
    Generic[_T_Conn, _T_Event],
):
    __slots__ = ("_impl", "_options")
    _impl_class: type[BasePoolImpl[_T_Conn, _T_Event]]

    # Number of stack frames that the Retry objects used by
    # transaction() need to look up to find their caller from for
    # caching purposes.  We define this in a variable on BaseClient
    # basically to make it possible for a user to hackily override it
    # if they always have some wrapper they need to skip past...
    _TRANSACTION_FRAME_OFFSET = 2

    def __init__(
        self,
        *,
        connection_class: type[_T_Conn],
        max_concurrency: int | None,
        dsn: str | None = None,
        host: str | None = None,
        port: int | None = None,
        credentials: str | None = None,
        credentials_file: str | None = None,
        user: str | None = None,
        password: str | None = None,
        secret_key: str | None = None,
        database: str | None = None,
        branch: str | None = None,
        tls_ca: str | None = None,
        tls_ca_file: str | None = None,
        tls_security: str | None = None,
        tls_server_name: str | None = None,
        wait_until_available: int = 30,
        timeout: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        connect_args = {
            "dsn": dsn,
            "host": host,
            "port": port,
            "credentials": credentials,
            "credentials_file": credentials_file,
            "user": user,
            "password": password,
            "secret_key": secret_key,
            "database": database,
            "branch": branch,
            "timeout": timeout,
            "tls_ca": tls_ca,
            "tls_ca_file": tls_ca_file,
            "tls_security": tls_security,
            "tls_server_name": tls_server_name,
            "wait_until_available": wait_until_available,
        }

        self._impl = self._impl_class(
            connect_args,
            connection_factory=connection_class,
            max_concurrency=max_concurrency,
            **kwargs,
        )

    def _shallow_clone(self) -> Self:  # pyright: ignore [reportIncompatibleMethodOverride]
        new_client = self.__class__.__new__(self.__class__)
        new_client._impl = self._impl
        return new_client

    def _get_query_cache(self) -> abstract.QueryCache:
        return abstract.QueryCache(
            codecs_registry=self._impl.codecs_registry,
            query_cache=self._impl.query_cache,
        )

    def _get_debug_options(self) -> _options.Debug:
        return self._options._debug  # type: ignore [no-any-return]

    def _get_retry_options(self) -> _options.RetryOptions | None:
        # This is overloaded in transaction.py to return None, to prevent
        # retrying *inside* a transaction.
        return self._options.retry_options  # type: ignore [no-any-return]

    def _get_active_tx_options(
        self,
    ) -> _options.TransactionOptions | None:
        # This is overloaded in transaction.py to return None, since
        # the tx options are applied at the *start* of transactions,
        # not inside them.
        return self._options.transaction_options  # type: ignore [no-any-return]

    def _get_state(self) -> _options.State:
        return self._options.state  # type: ignore [no-any-return]

    def _get_warning_handler(self) -> _options.WarningHandler:
        return self._options.warning_handler  # type: ignore [no-any-return]

    def _get_annotations(self) -> dict[str, str]:
        return self._options.annotations  # type: ignore [no-any-return]

    @property
    def max_concurrency(self) -> int:
        """Max number of connections in the pool."""

        return self._impl.get_max_concurrency()

    @property
    def free_size(self) -> int:
        """Number of available connections in the pool."""

        return self._impl.get_free_size()

    async def _query(
        self, query_context: abstract.BaseQueryContext[_T_co]
    ) -> Any:
        con = await self._impl.acquire()
        try:
            return await con.raw_query(query_context)
        finally:
            await self._impl.release(con)

    async def _execute(
        self, execute_context: abstract.ExecuteContext[_T_co]
    ) -> None:
        con = await self._impl.acquire()
        try:
            await con._execute(execute_context)
        finally:
            await self._impl.release(con)

    async def _describe(
        self, describe_context: abstract.DescribeContext
    ) -> abstract.DescribeResult:
        con = await self._impl.acquire()
        try:
            return await con.describe(describe_context)
        finally:
            await self._impl.release(con)

    def terminate(self) -> None:
        """Terminate all connections in the pool."""
        self._impl.terminate()
