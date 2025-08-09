#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, TypeVar

import contextlib
import collections
import dataclasses
import datetime
import queue
import socket
import ssl
import sys
import threading
import time
import typing

from . import abstract
from . import base_client
from . import con_utils
from . import errors
from . import transaction
from .protocol import blocking_proto  # type: ignore [attr-defined, unused-ignore]
from .protocol.protocol import InputLanguage, OutputFormat

from ._internal._save import make_save_executor_constructor

if typing.TYPE_CHECKING:
    from ._internal._qbmodel._pydantic import GelModel


DEFAULT_PING_BEFORE_IDLE_TIMEOUT = datetime.timedelta(seconds=5)
MINIMUM_PING_WAIT_TIME = datetime.timedelta(seconds=1)


_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)


@dataclasses.dataclass
class SaveDebug:
    queries: list[SaveQueryDebug]
    plan_time: float


@dataclasses.dataclass
class SaveQueryDebug:
    query: Any = ""
    max_args_number: int = 0
    total_execs: int = 0
    total_exec_time: float = 0
    analyze: str = ""
    analyze_args: object = None
    args_query: str = ""
    args_analyze: object = None


def iter_coroutine(coro: typing.Coroutine[None, None, _T]) -> _T:
    try:
        coro.send(None)
    except StopIteration as ex:
        return ex.value  # type: ignore [no-any-return]
    else:
        raise RuntimeError(
            f"coroutine {coro!r} did not stop after one iteration!"
        )
    finally:
        coro.close()


class BlockingIOConnection(base_client.BaseConnection[threading.Event]):
    __slots__ = ("_ping_wait_time",)

    async def connect_addr(
        self, addr: str | tuple[str, int], timeout: float
    ) -> None:
        deadline = time.monotonic() + timeout

        if isinstance(addr, str):
            if sys.platform == "win32":
                raise RuntimeError(
                    "connecting via Unix sockets is not supported on Windows"
                )
            # UNIX socket
            res_list: Any = [
                (socket.AF_UNIX, socket.SOCK_STREAM, -1, None, addr)
            ]
        else:
            host, port = addr
            try:
                # getaddrinfo() doesn't take timeout!!
                res_list = socket.getaddrinfo(
                    host, port, socket.AF_UNSPEC, socket.SOCK_STREAM
                )
            except socket.gaierror as e:
                # All name resolution errors are considered temporary
                err = errors.ClientConnectionFailedTemporarilyError(str(e))
                raise err from e

        for i, res in enumerate(res_list):
            af, socktype, proto, _, sa = res
            try:
                sock = socket.socket(af, socktype, proto)
            except OSError as e:
                sock.close()  # pyright: ignore[reportPossiblyUnboundVariable]
                if i < len(res_list) - 1:
                    continue
                else:
                    raise con_utils.wrap_error(e) from e
            try:
                await self._connect_addr(sock, addr, sa, deadline)
            except TimeoutError:
                raise
            except Exception:
                if i < len(res_list) - 1:
                    continue
                else:
                    raise
            else:
                break

    async def _connect_addr(
        self,
        sock: socket.socket,
        addr: str | tuple[str, int],
        sa: str | tuple[str, int],
        deadline: float,
    ) -> None:
        try:
            time_left = deadline - time.monotonic()
            if time_left <= 0:
                raise TimeoutError
            try:
                sock.settimeout(time_left)
                sock.connect(sa)
            except OSError as e:
                raise con_utils.wrap_error(e) from e

            if not isinstance(addr, str):
                time_left = deadline - time.monotonic()
                if time_left <= 0:
                    raise TimeoutError
                try:
                    # Upgrade to TLS
                    sock.settimeout(time_left)
                    try:
                        sock = self._params.ssl_ctx.wrap_socket(
                            sock,
                            server_hostname=(
                                self._params.tls_server_name or addr[0]
                            ),
                        )
                    except ssl.CertificateError as e:
                        raise con_utils.wrap_error(e) from e
                    except ssl.SSLError as e:
                        raise con_utils.wrap_error(e) from e
                    else:
                        con_utils.check_alpn_protocol(sock)
                except OSError as e:
                    raise con_utils.wrap_error(e) from e

            time_left = deadline - time.monotonic()
            if time_left <= 0:
                raise TimeoutError

            if not isinstance(addr, str):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            proto = blocking_proto.BlockingIOProtocol(self._params, sock)
            proto.set_connection(self)

            try:
                await proto.wait_for(proto.connect(), time_left)
            except TimeoutError:
                raise
            except OSError as e:
                raise con_utils.wrap_error(e) from e

            self._protocol = proto
            self._addr = addr
            settings = self.get_settings()
            system_config = settings.get("system_config") if settings else None
            session_idle_timeout = (
                system_config.session_idle_timeout
                if system_config
                else DEFAULT_PING_BEFORE_IDLE_TIMEOUT
            )
            self._ping_wait_time = max(
                (session_idle_timeout - DEFAULT_PING_BEFORE_IDLE_TIMEOUT),
                MINIMUM_PING_WAIT_TIME,
            ).total_seconds()

        except Exception:
            sock.close()
            raise

    async def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def is_closed(self) -> bool:
        proto = self._protocol
        return not (
            proto
            and proto.sock is not None
            and proto.sock.fileno() >= 0
            and proto.connected
        )

    async def close(self, timeout: float | None = None) -> None:
        """Send graceful termination message wait for connection to drop."""
        if not self.is_closed():
            try:
                self._protocol.terminate()
                if timeout is None:
                    await self._protocol.wait_for_disconnect()
                else:
                    await self._protocol.wait_for(
                        self._protocol.wait_for_disconnect(), timeout
                    )
            except TimeoutError:
                self.terminate()
                raise errors.QueryTimeoutError()
            except Exception:
                self.terminate()
                raise
            finally:
                self._cleanup()

    def _dispatch_log_message(self, msg: Any) -> None:
        for cb in self._log_listeners:
            cb(self, msg)  # type: ignore [arg-type]

    async def raw_query(
        self, query_context: abstract.BaseQueryContext[_T_co]
    ) -> Any:
        try:
            if (
                time.monotonic() - self._protocol.last_active_timestamp
                > self._ping_wait_time
            ):
                await self._protocol.ping()
        except (errors.IdleSessionTimeoutError, errors.ClientConnectionError):
            await self.connect()

        return await super().raw_query(query_context)


class _PoolConnectionHolder(
    base_client.PoolConnectionHolder[BlockingIOConnection, threading.Event]
):
    __slots__ = ()
    _event_class = threading.Event

    async def close(
        self, *, wait: bool = True, timeout: float | None = None
    ) -> None:
        if self._con is None:
            return
        await self._con.close(timeout=timeout)

    async def wait_until_released(self, timeout: float | None = None) -> None:
        result = self._release_event.wait(timeout)
        if timeout is not None and not result:
            raise TimeoutError


class _PoolImpl(
    base_client.BasePoolImpl[BlockingIOConnection, threading.Event]
):
    _holder_class = _PoolConnectionHolder

    def __init__(
        self,
        connect_args: Any,
        *,
        max_concurrency: int | None,
        connection_factory: type[BlockingIOConnection],
    ) -> None:
        if not issubclass(connection_factory, BlockingIOConnection):
            raise TypeError(
                f"connection_factory is expected to be a subclass of "
                f"gel.blocking_client.BlockingIOConnection, "
                f"got {connection_factory}"
            )
        super().__init__(
            connect_args,
            connection_factory,
            max_concurrency=max_concurrency,
        )

    def _ensure_initialized(self) -> None:
        if self._queue is None:
            self._queue: queue.LifoQueue[_PoolConnectionHolder] = (
                queue.LifoQueue(maxsize=self._max_concurrency)
            )
            self._first_connect_lock: threading.Lock = threading.Lock()
            self._resize_holder_pool()

    def _set_queue_maxsize(self, maxsize: int) -> None:
        with self._queue.mutex:
            self._queue.maxsize = maxsize

    async def _maybe_get_first_connection(
        self,
    ) -> BlockingIOConnection | None:
        with self._first_connect_lock:
            if self._working_addr is None:
                return await self._get_first_connection()
        return None

    async def acquire(
        self, timeout: float | None = None
    ) -> BlockingIOConnection:
        self._ensure_initialized()

        if self._closing:
            raise errors.InterfaceError("pool is closing")

        ch = self._queue.get(timeout=timeout)
        try:
            con = await ch.acquire()
        except Exception:
            self._queue.put_nowait(ch)
            raise
        else:
            # Record the timeout, as we will apply it by default
            # in release().
            ch._timeout = timeout
            return con

    async def _release(
        self,
        connection: base_client.PoolConnectionHolder[
            BlockingIOConnection, threading.Event
        ],
    ) -> None:
        if connection._con is not None and not isinstance(
            connection._con, BlockingIOConnection
        ):
            raise errors.InterfaceError(
                f"release() received invalid connection: "
                f"{connection._con!r} does not belong to any connection pool"
            )

        timeout = None
        await connection.release(timeout)

    async def close(self, timeout: float | None = None) -> None:
        if self._closed:
            return
        self._closing = True
        try:
            if timeout is None:
                for ch in self._holders:
                    await ch.wait_until_released()
                for ch in self._holders:
                    await ch.close()
            else:
                deadline = time.monotonic() + timeout
                for ch in self._holders:
                    secs = deadline - time.monotonic()
                    if secs <= 0:
                        raise TimeoutError
                    await ch.wait_until_released(timeout=secs)
                for ch in self._holders:
                    secs = deadline - time.monotonic()
                    if secs <= 0:
                        raise TimeoutError
                    await ch.close(timeout=secs)
        except TimeoutError as e:
            self.terminate()
            raise errors.InterfaceError(
                f"client is not fully closed in {timeout} seconds; "
                "terminating now."
            ) from e
        except Exception:
            self.terminate()
            raise
        finally:
            self._closed = True
            self._closing = False


class Iteration(transaction.BaseTransaction, abstract.Executor):
    __slots__ = ("_managed", "_lock")

    def __init__(self, retry: Retry, client: Client, iteration: int) -> None:
        super().__init__(retry, client, iteration)
        self._managed = False
        self._lock = threading.Lock()

    def __enter__(self) -> Iteration:
        with self._exclusive():
            if self._managed:
                raise errors.InterfaceError(
                    "cannot enter context: already in a `with` block"
                )
            self._managed = True
            return self

    def __exit__(
        self,
        extype: type[BaseException] | None,
        ex: BaseException | None,
        tb: Any | None,
    ) -> bool | None:
        with self._exclusive():
            self._managed = False
            return iter_coroutine(self._exit(extype, ex))  # type: ignore [no-any-return]

    async def _ensure_transaction(self) -> None:
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `with transaction:`"
            )
        await super()._ensure_transaction()

    def _query(self, query_context: abstract.BaseQueryContext[Any]) -> Any:
        with self._exclusive():
            return iter_coroutine(super()._query(query_context))  # type: ignore [arg-type]

    def _execute(self, execute_context: abstract.ExecuteContext[Any]) -> None:  # type: ignore[override]
        with self._exclusive():
            iter_coroutine(super()._execute(execute_context))

    @contextlib.contextmanager
    def _exclusive(self) -> typing.Generator[None, None, None]:
        if not self._lock.acquire(blocking=False):
            raise errors.InterfaceError(
                "concurrent queries within the same transaction "
                "are not allowed"
            )
        try:
            yield
        finally:
            self._lock.release()


class Retry(transaction.BaseRetry):
    def __iter__(self) -> Retry:
        return self

    def __next__(self) -> Iteration:
        # Note: when changing this code consider also
        # updating AsyncIORetry.__anext__.
        if self._done:
            raise StopIteration
        if self._next_backoff:
            time.sleep(self._next_backoff)
        self._done = True
        iteration = Iteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class BatchIteration(transaction.BaseTransaction):
    __slots__ = ("_managed", "_lock", "_batched_ops")

    def __init__(self, retry: Batch, client: Client, iteration: int) -> None:
        super().__init__(retry, client, iteration)
        self._managed = False
        self._lock = threading.Lock()
        self._batched_ops: list[
            abstract.BaseQueryContext[Any] | abstract.ExecuteContext[Any]
        ] = []

    def __enter__(self) -> BatchIteration:
        with self._exclusive():
            if self._managed:
                raise errors.InterfaceError(
                    "cannot enter context: already in a `with` block"
                )
            self._managed = True
            return self

    def __exit__(
        self,
        extype: type[BaseException] | None,
        ex: BaseException | None,
        tb: Any | None,
    ) -> bool | None:
        with self._exclusive():
            if extype is None:
                # Normal exit, wait for the remaining batched operations
                # to complete, discarding any results.
                try:
                    iter_coroutine(self._wait())
                except Exception as inner_ex:
                    # If an exception occurs while waiting, we need to
                    # ensure that the transaction is exited properly,
                    # including to consider that exception for retry.
                    self._managed = False
                    if iter_coroutine(self._exit(type(inner_ex), inner_ex)):
                        # Shall retry, mute the exception
                        return True
                    else:
                        # Shall not retry, re-raise the exception.
                        # Note: we cannot simply return False here,
                        # because the outer `extype` and `ex` are all None.
                        raise
            self._managed = False
            return iter_coroutine(self._exit(extype, ex))  # type: ignore [no-any-return]

    async def _ensure_transaction(self) -> None:
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `with transaction:`"
            )
        await super()._ensure_transaction()

    @contextlib.contextmanager
    def _exclusive(self) -> typing.Generator[None, None, None]:
        if not self._lock.acquire(blocking=False):
            raise errors.InterfaceError(
                "concurrent queries within the same transaction "
                "are not allowed"
            )
        try:
            yield
        finally:
            self._lock.release()

    def send_query(
        self,
        query: str | abstract.Queryable[Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._batched_ops.append(
            abstract.QueryContext(
                query=abstract.QueryWithArgs.from_query(query, args, kwargs),
                cache=self._client._get_query_cache(),
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    def send_query_single(
        self,
        query: str | abstract.Queryable[Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._batched_ops.append(
            abstract.QuerySingleContext(
                query=abstract.QueryWithArgs.from_query(query, args, kwargs),
                cache=self._client._get_query_cache(),
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    def send_query_required_single(
        self,
        query: str | abstract.Queryable[Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._batched_ops.append(
            abstract.QueryRequiredSingleContext(
                query=abstract.QueryWithArgs.from_query(query, args, kwargs),
                cache=self._client._get_query_cache(),
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    def send_execute(self, commands: str, *args: Any, **kwargs: Any) -> None:
        self._batched_ops.append(
            abstract.ExecuteContext(
                query=abstract.QueryWithArgs(commands, None, args, kwargs),
                cache=self._client._get_query_cache(),
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    def wait(self) -> list[Any]:
        with self._exclusive():
            return iter_coroutine(self._wait())

    async def _wait(self) -> list[Any]:
        await self._ensure_transaction()
        ops, self._batched_ops[:] = self._batched_ops[:], []
        if self._connection is not None:
            return await self._connection.batch_query(ops)  # type: ignore [no-any-return]
        return []


class Batch(transaction.BaseRetry):
    def __iter__(self) -> Batch:
        return self

    def __next__(self) -> BatchIteration:
        # Note: when changing this code consider also
        # updating AsyncIOBatch.__anext__.
        if self._done:
            raise StopIteration
        if self._next_backoff:
            time.sleep(self._next_backoff)
        self._done = True
        iteration = BatchIteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class Client(
    base_client.BaseClient[BlockingIOConnection, threading.Event],
    abstract.Executor,
):
    """A lazy connection pool.

    A Client can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Clients are created by calling
    :func:`~gel.blocking_client.create_client`.
    """

    __slots__ = ()
    _impl_class = _PoolImpl

    def _save_impl(
        self,
        *,
        refetch: bool,
        objs: tuple[GelModel, ...],
        warn_on_large_sync_set: bool = False,
    ) -> None:
        opts = self._get_debug_options()

        make_executor = make_save_executor_constructor(
            objs,
            refetch=refetch,
            save_postcheck=opts.save_postcheck,
            warn_on_large_sync_set=warn_on_large_sync_set,
        )

        for tx in self._batch():
            with tx:
                executor = make_executor()

                for batches in executor:
                    for batch in batches:
                        tx.send_query(batch.query, batch.args)
                    batch_ids = tx.wait()
                    for ids, batch in zip(batch_ids, batches, strict=True):
                        batch.feed_db_data(ids)

                if refetch:
                    ref_queries = executor.get_refetch_queries()
                    for ref in ref_queries:
                        tx.send_query(ref.query, **ref.args)

                    refetch_data = tx.wait()
                    for ref_data, ref in zip(
                        refetch_data, ref_queries, strict=True
                    ):
                        ref.feed_db_data(ref_data)

                executor.commit()

    def save(
        self,
        *objs: GelModel,
    ) -> None:
        """Persist objects without refetching updated data back.

        This is a subset of `sync()`, optimized to avoid refetching.
        """
        self._save_impl(refetch=False, objs=objs)

    def sync(
        self,
        *objs: GelModel,
        warn_on_large_sync: bool = True,
    ) -> None:
        """Persist objects and refetch updated data back into them."""
        self._save_impl(
            refetch=True,
            objs=objs,
            warn_on_large_sync_set=warn_on_large_sync,
        )

    def __debug_save__(self, *objs: GelModel) -> SaveDebug:
        ns = time.monotonic_ns()
        make_executor = make_save_executor_constructor(
            objs,
            refetch=False,  # TODO
        )
        plan_time = time.monotonic_ns() - ns

        queries: dict[Any, SaveQueryDebug] = collections.defaultdict(
            SaveQueryDebug
        )

        for tx in self._batch():
            with tx:
                executor = make_executor()

                for batches in executor:
                    for batch in batches:
                        qdebug = queries[batch.query]

                        qdebug.total_execs += 1
                        qdebug.query = batch.query
                        qdebug.args_query = batch.args_query

                        if not qdebug.analyze:
                            tx.send_query(f"ANALYZE {batch.query}", batch.args)
                            qdebug.analyze = tx.wait()[0][0]
                            tx.send_query(
                                f"ANALYZE {batch.args_query}", batch.args
                            )
                            qdebug.args_analyze = tx.wait()[0][0]
                            qdebug.analyze_args = batch.args

                        ns = time.monotonic_ns()
                        tx.send_query(batch.query, batch.args)
                        batch_ids = tx.wait()
                        qdebug.total_exec_time += time.monotonic_ns() - ns

                        qdebug.max_args_number = max(
                            qdebug.max_args_number, len(batch.args)
                        )

                        batch.feed_db_data(batch_ids[0])

                executor.commit()

        for qdebug in queries.values():
            qdebug.total_exec_time /= 1_000_000.0

        return SaveDebug(
            queries=sorted(queries.values(), key=lambda q: q.total_exec_time),
            plan_time=plan_time / 1_000_000.0,
        )

    def _query(self, query_context: abstract.BaseQueryContext[_T_co]) -> Any:
        return iter_coroutine(super()._query(query_context))

    def _execute(  # type: ignore [override]
        self, execute_context: abstract.ExecuteContext[_T_co]
    ) -> None:
        iter_coroutine(super()._execute(execute_context))

    def check_connection(self) -> base_client.ConnectionInfo:
        return iter_coroutine(self._impl.ensure_connected())

    def ensure_connected(self) -> Client:
        self.check_connection()
        return self

    def transaction(self) -> Retry:
        return Retry(self)

    def _batch(self) -> Batch:
        return Batch(
            self.with_config(
                # We only need to disable transaction idle timeout;
                # session idle timeouts can't interrupt transactions.
                session_idle_transaction_timeout=datetime.timedelta()
            )
        )

    def close(self, timeout: float | None = None) -> None:
        """Attempt to gracefully close all connections in the client.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        Client.terminate() .
        """
        iter_coroutine(self._impl.close(timeout))

    def __enter__(self) -> Client:
        return self.ensure_connected()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        self.close()

    def _describe_query(
        self,
        query: str,
        *,
        inject_type_names: bool = False,
        input_language: InputLanguage = InputLanguage.EDGEQL,
        output_format: OutputFormat = OutputFormat.BINARY,
        expect_one: bool = False,
    ) -> abstract.DescribeResult:
        return iter_coroutine(
            self._describe(
                abstract.DescribeContext(
                    query=query,
                    state=self._get_state(),
                    inject_type_names=inject_type_names,
                    input_language=input_language,
                    output_format=output_format,
                    expect_one=expect_one,
                )
            )
        )


def create_client(
    dsn: str | None = None,
    *,
    max_concurrency: int | None = None,
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
    wait_until_available: int = 30,
    timeout: int = 10,
) -> Client:
    return Client(
        connection_class=BlockingIOConnection,
        max_concurrency=max_concurrency,
        # connect arguments
        dsn=dsn,
        host=host,
        port=port,
        credentials=credentials,
        credentials_file=credentials_file,
        user=user,
        password=password,
        secret_key=secret_key,
        database=database,
        branch=branch,
        tls_ca=tls_ca,
        tls_ca_file=tls_ca_file,
        tls_security=tls_security,
        wait_until_available=wait_until_available,
        timeout=timeout,
    )
