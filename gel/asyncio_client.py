#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any
from typing_extensions import Self

import asyncio
import contextlib
import logging
import socket
import ssl
import typing

from . import abstract
from . import base_client
from . import con_utils
from . import errors
from . import transaction
from .protocol import asyncio_proto
from .protocol.protocol import InputLanguage, OutputFormat

from ._internal._save import make_save_executor_constructor

if typing.TYPE_CHECKING:
    from ._internal._qbmodel._pydantic import GelModel


__all__ = ("create_async_client", "AsyncIOClient")


logger = logging.getLogger(__name__)


class AsyncIOConnection(base_client.BaseConnection):
    __slots__ = ("_loop",)

    def __init__(self, loop, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loop = loop

    def is_closed(self):
        protocol = self._protocol
        return protocol is None or not protocol.connected

    async def connect_addr(self, addr, timeout):
        try:
            await asyncio.wait_for(self._connect_addr(addr), timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError from e

    async def sleep(self, seconds):
        await asyncio.sleep(seconds)

    async def aclose(self):
        """Send graceful termination message wait for connection to drop."""
        if not self.is_closed():
            try:
                self._protocol.terminate()
                await self._protocol.wait_for_disconnect()
            except (Exception, asyncio.CancelledError):
                self.terminate()
                raise
            finally:
                self._cleanup()

    def _protocol_factory(self):
        return asyncio_proto.AsyncIOProtocol(self._params, self._loop)

    async def _connect_addr(self, addr):
        tr = None

        try:
            if isinstance(addr, str):
                # UNIX socket
                tr, pr = await self._loop.create_unix_connection(
                    self._protocol_factory, addr
                )
            else:
                try:
                    tr, pr = await self._loop.create_connection(
                        self._protocol_factory,
                        *addr,
                        ssl=self._params.ssl_ctx,
                        server_hostname=(
                            self._params.tls_server_name or addr[0]
                        ),
                    )
                except ssl.CertificateError as e:
                    raise con_utils.wrap_error(e) from e
                except ssl.SSLError as e:
                    raise con_utils.wrap_error(e) from e
                else:
                    con_utils.check_alpn_protocol(
                        tr.get_extra_info("ssl_object")
                    )
        except socket.gaierror as e:
            # All name resolution errors are considered temporary
            raise errors.ClientConnectionFailedTemporarilyError(str(e)) from e
        except OSError as e:
            raise con_utils.wrap_error(e) from e
        except Exception:
            if tr is not None:
                tr.close()
            raise

        pr.set_connection(self)

        try:
            await pr.connect()
        except OSError as e:
            if tr is not None:
                tr.close()
            raise con_utils.wrap_error(e) from e
        except BaseException:
            if tr is not None:
                tr.close()
            raise

        self._protocol = pr
        self._addr = addr

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            self._loop.call_soon(cb, self, msg)


class _PoolConnectionHolder(base_client.PoolConnectionHolder):
    __slots__ = ()
    _event_class = asyncio.Event

    async def close(self, *, wait=True):
        if self._con is None:
            return
        if wait:
            await self._con.aclose()
        else:
            self._pool._loop.create_task(self._con.aclose())

    async def wait_until_released(self, timeout=None):
        await self._release_event.wait()


class _AsyncIOPoolImpl(base_client.BasePoolImpl):
    __slots__ = ("_loop",)
    _holder_class = _PoolConnectionHolder

    def __init__(
        self,
        connect_args,
        *,
        max_concurrency: typing.Optional[int],
        connection_class,
    ):
        if not issubclass(connection_class, AsyncIOConnection):
            raise TypeError(
                f"connection_class is expected to be a subclass of "
                f"gel.asyncio_client.AsyncIOConnection, "
                f"got {connection_class}"
            )
        self._loop = None
        super().__init__(
            connect_args,
            lambda *args: connection_class(self._loop, *args),
            max_concurrency=max_concurrency,
        )

    def _ensure_initialized(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
            self._queue = asyncio.LifoQueue(maxsize=self._max_concurrency)
            self._first_connect_lock = asyncio.Lock()
            self._resize_holder_pool()

    def _set_queue_maxsize(self, maxsize):
        self._queue._maxsize = maxsize

    async def _maybe_get_first_connection(self):
        async with self._first_connect_lock:
            if self._working_addr is None:
                return await self._get_first_connection()

    async def acquire(self, timeout=None):
        self._ensure_initialized()

        async def _acquire_impl():
            ch = await self._queue.get()  # type: _PoolConnectionHolder
            try:
                proxy = await ch.acquire()  # type: AsyncIOConnection
            except (Exception, asyncio.CancelledError):
                self._queue.put_nowait(ch)
                raise
            else:
                # Record the timeout, as we will apply it by default
                # in release().
                ch._timeout = timeout
                return proxy

        if self._closing:
            raise errors.InterfaceError("pool is closing")

        if timeout is None:
            return await _acquire_impl()
        else:
            return await asyncio.wait_for(_acquire_impl(), timeout=timeout)

    async def _release(self, holder):
        if not isinstance(holder._con, AsyncIOConnection):
            raise errors.InterfaceError(
                f"release() received invalid connection: "
                f"{holder._con!r} does not belong to any connection pool"
            )

        timeout = None

        # Use asyncio.shield() to guarantee that task cancellation
        # does not prevent the connection from being returned to the
        # pool properly.
        return await asyncio.shield(holder.release(timeout))

    async def aclose(self):
        """Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        _AsyncIOPoolImpl.terminate() .

        It is advisable to use :func:`python:asyncio.wait_for` to set
        a timeout.
        """
        if self._closed:
            return

        if not self._loop:
            self._closed = True
            return

        self._closing = True

        try:
            warning_callback = self._loop.call_later(
                60, self._warn_on_long_close
            )

            release_coros = [ch.wait_until_released() for ch in self._holders]
            await asyncio.gather(*release_coros)

            close_coros = [ch.close() for ch in self._holders]
            await asyncio.gather(*close_coros)

        except (Exception, asyncio.CancelledError):
            self.terminate()
            raise

        finally:
            warning_callback.cancel()
            self._closed = True
            self._closing = False

    def _warn_on_long_close(self):
        logger.warning(
            "AsyncIOClient.aclose() is taking over 60 seconds to complete. "
            "Check if you have any unreleased connections left. "
            "Use asyncio.wait_for() to set a timeout for "
            "AsyncIOClient.aclose()."
        )


class AsyncIOIteration(transaction.BaseTransaction, abstract.AsyncIOExecutor):
    __slots__ = ("_managed", "_locked")

    def __init__(self, retry, client, iteration):
        super().__init__(retry, client, iteration)
        self._managed = False
        self._locked = False

    async def __aenter__(self):
        if self._managed:
            raise errors.InterfaceError(
                "cannot enter context: already in an `async with` block"
            )
        self._managed = True
        return self

    async def __aexit__(self, extype, ex, tb):
        with self._exclusive():
            self._managed = False
            return await self._exit(extype, ex)

    async def _ensure_transaction(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `async with transaction:`"
            )
        await super()._ensure_transaction()

    async def _query(self, query_context: abstract.QueryContext):
        with self._exclusive():
            return await super()._query(query_context)

    async def _execute(self, execute_context: abstract.ExecuteContext) -> None:
        with self._exclusive():
            await super()._execute(execute_context)

    @contextlib.contextmanager
    def _exclusive(self):
        if self._locked:
            raise errors.InterfaceError(
                "concurrent queries within the same transaction "
                "are not allowed"
            )
        self._locked = True
        try:
            yield
        finally:
            self._locked = False


class AsyncIORetry(transaction.BaseRetry):
    def __aiter__(self):
        return self

    async def __anext__(self):
        # Note: when changing this code consider also
        # updating Retry.__next__.
        if self._done:
            raise StopAsyncIteration
        if self._next_backoff:
            await asyncio.sleep(self._next_backoff)
        self._done = True
        iteration = AsyncIOIteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class AsyncIOBatchIteration(transaction.BaseTransaction):
    __slots__ = ("_managed", "_locked", "_batched_ops")

    def __init__(self, retry, client, iteration):
        super().__init__(retry, client, iteration)
        self._managed = False
        self._locked = False
        self._batched_ops = []

    async def __aenter__(self):
        if self._managed:
            raise errors.InterfaceError(
                "cannot enter context: already in an `async with` block"
            )
        self._managed = True
        return self

    async def __aexit__(self, extype, ex, tb):
        if extype is None:
            try:
                await self.wait()
            except Exception as ex:
                with self._exclusive():
                    self._managed = False
                    if await self._exit(type(ex), ex):
                        return True
                    else:
                        raise
        with self._exclusive():
            self._managed = False
            return await self._exit(extype, ex)

    async def _ensure_transaction(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `async with transaction:`"
            )
        await super()._ensure_transaction()

    @contextlib.contextmanager
    def _exclusive(self):
        if self._locked:
            raise errors.InterfaceError(
                "concurrent queries within the same transaction "
                "are not allowed"
            )
        self._locked = True
        try:
            yield
        finally:
            self._locked = False

    async def send_query(self, query: str, *args, **kwargs) -> None:
        self._batched_ops.append(
            abstract.QueryContext(
                query=abstract.QueryWithArgs(query, None, args, kwargs),
                cache=self._client._get_query_cache(),
                query_options=abstract._query_opts,
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    async def send_query_single(self, query: str, *args, **kwargs) -> None:
        self._batched_ops.append(
            abstract.QueryContext(
                query=abstract.QueryWithArgs(query, None, args, kwargs),
                cache=self._client._get_query_cache(),
                query_options=abstract._query_single_opts,
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    async def send_query_required_single(
        self, query: str, *args, **kwargs
    ) -> None:
        self._batched_ops.append(
            abstract.QueryContext(
                query=abstract.QueryWithArgs(query, None, args, kwargs),
                cache=self._client._get_query_cache(),
                query_options=abstract._query_required_single_opts,
                retry_options=None,
                state=self._client._get_state(),
                transaction_options=None,
                warning_handler=self._client._get_warning_handler(),
                annotations=self._client._get_annotations(),
            )
        )

    async def send_execute(self, commands: str, *args, **kwargs) -> None:
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

    async def wait(self) -> list[Any]:
        with self._exclusive():
            await self._ensure_transaction()
            ops, self._batched_ops[:] = self._batched_ops[:], []
            return await self._connection.batch_query(ops)


class AsyncIOBatch(transaction.BaseRetry):
    def __aiter__(self):
        return self

    async def __anext__(self):
        # Note: when changing this code consider also
        # updating Batch.__next__.
        if self._done:
            raise StopAsyncIteration
        if self._next_backoff:
            await asyncio.sleep(self._next_backoff)
        self._done = True
        iteration = AsyncIOBatchIteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class AsyncIOClient(base_client.BaseClient, abstract.AsyncIOExecutor):
    """A lazy connection pool.

    A Client can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Clients are created by calling
    :func:`~gel.asyncio_client.create_async_client`.
    """

    __slots__ = ()
    _impl_class = _AsyncIOPoolImpl

    async def check_connection(self) -> base_client.ConnectionInfo:
        return await self._impl.ensure_connected()

    async def ensure_connected(self) -> Self:
        await self.check_connection()
        return self

    async def aclose(self) -> None:
        """Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``aclose()`` the pool will terminate by calling
        AsyncIOClient.terminate() .

        It is advisable to use :func:`python:asyncio.wait_for` to set
        a timeout.
        """
        await self._impl.aclose()

    def transaction(self) -> AsyncIORetry:
        return AsyncIORetry(self)

    def _batch(self) -> AsyncIOBatch:
        return AsyncIOBatch(self)

    async def save(self, *objs: GelModel) -> None:
        make_executor = make_save_executor_constructor(objs)

        async for tx in self._batch():
            async with tx:
                executor = make_executor()

                for batches in executor:
                    for batch in batches:
                        await tx.send_query(batch.query, batch.args)
                    batch_ids = await tx.wait()
                    for ids, batch in zip(batch_ids, batches, strict=True):
                        batch.feed_ids(ids)

                executor.commit()

    async def __aenter__(self):
        return await self.ensure_connected()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    async def _describe_query(
        self,
        query: str,
        *,
        inject_type_names: bool = False,
        input_language: InputLanguage = InputLanguage.EDGEQL,
        output_format: OutputFormat = OutputFormat.BINARY,
        expect_one: bool = False,
    ) -> abstract.DescribeResult:
        return await self._describe(
            abstract.DescribeContext(
                query=query,
                state=self._get_state(),
                inject_type_names=inject_type_names,
                input_language=input_language,
                output_format=output_format,
                expect_one=expect_one,
            )
        )


def create_async_client(
    dsn=None,
    *,
    max_concurrency=None,
    host: str = None,
    port: int = None,
    credentials: str = None,
    credentials_file: str = None,
    user: str = None,
    password: str = None,
    secret_key: str = None,
    database: str = None,
    branch: str = None,
    tls_ca: str = None,
    tls_ca_file: str = None,
    tls_security: str = None,
    wait_until_available: int = 30,
    timeout: int = 10,
):
    return AsyncIOClient(
        connection_class=AsyncIOConnection,
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
