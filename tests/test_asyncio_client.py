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

import asyncio
import random

import gel

from gel import _testbase as tb
from gel import errors
from gel import asyncio_client


class TestAsyncIOClient(tb.AsyncQueryTestCase):
    def create_client(self, **kwargs):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.get_database_name()
        conargs["timeout"] = 120
        conargs.update(kwargs)
        conargs.setdefault(
            "connection_class", asyncio_client.AsyncIOConnection
        )
        conargs.setdefault("max_concurrency", None)

        return tb.TestAsyncIOClient(**conargs)

    async def test_client_01(self):
        for n in {1, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = self.create_client(max_concurrency=10)

                async def worker():
                    self.assertEqual(await client.query_single("SELECT 1"), 1)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await client.aclose()

    async def test_client_02(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                async with self.create_client(max_concurrency=5) as client:

                    async def worker():
                        self.assertEqual(
                            await client.query_single("SELECT 1"), 1
                        )

                    tasks = [worker() for _ in range(n)]
                    await asyncio.gather(*tasks)

    async def test_client_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = self.create_client(max_concurrency=10)

                async def worker():
                    self.assertEqual(await client.query('SELECT 1'), [1])
                    self.assertEqual(await client.query_single('SELECT 1'), 1)
                    self.assertEqual(
                        await client.query_json('SELECT 1'), '[1]')
                    self.assertEqual(
                        await client.query_single_json('SELECT 1'), '1')

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await client.aclose()

    async def test_client_transaction(self):
        client = self.create_client(max_concurrency=1)

        async for tx in client.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 7*8"), 56)

        await client.aclose()

    async def test_client_options(self):
        client = self.create_client(max_concurrency=1)

        client.with_transaction_options(
            gel.TransactionOptions(readonly=True))
        client.with_retry_options(
            gel.RetryOptions(attempts=1, backoff=gel.default_backoff))
        async for tx in client.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 7*8"), 56)

        await client.aclose()

    async def test_client_init_run_until_complete(self):
        client = self.create_client()
        self.assertIsInstance(client, asyncio_client.AsyncIOClient)
        await client.aclose()

    async def test_client_no_acquire_deadlock(self):
        async with self.create_client(
            max_concurrency=1,
        ) as client:

            has_sleep = await client.query_single("""
                SELECT EXISTS(
                    SELECT schema::Function FILTER .name = 'sys::_sleep'
                )
            """)
            if not has_sleep:
                self.skipTest("No sys::_sleep function")

            async def sleep_and_release():
                await client.execute("SELECT sys::_sleep(1)")

            asyncio.ensure_future(sleep_and_release())
            await asyncio.sleep(0.5)

            await client.query_single("SELECT 1")

    async def test_client_config_persistence(self):
        N = 100

        class MyConnection(asyncio_client.AsyncIOConnection):
            async def raw_query(self, query_context):
                res = await super().raw_query(query_context)
                return res + 1

        async def test(client):
            async for tx in client.transaction():
                async with tx:
                    self.assertEqual(await tx.query_single("SELECT 1"), 2)

        async with self.create_client(
            max_concurrency=10,
            connection_class=MyConnection,
        ) as client:

            await asyncio.gather(*[test(client) for _ in range(N)])

            self.assertEqual(
                sum(
                    1
                    for ch in client._impl._holders
                    if ch._con and not ch._con.is_closed()
                ),
                10,
            )

    async def test_client_connection_methods(self):
        async def test_query(client):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await client.query("SELECT {}".format(i))
            self.assertEqual(list(r), [i])
            return 1

        async def test_query_single(client):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await client.query_single("SELECT {}".format(i))
            self.assertEqual(r, i)
            return 1

        async def test_execute(client):
            await asyncio.sleep(random.random() / 100)
            await client.execute("SELECT {1, 2, 3, 4}")
            return 1

        async def run(N, meth):
            async with self.create_client(max_concurrency=10) as client:

                coros = [meth(client) for _ in range(N)]
                res = await asyncio.gather(*coros)
                self.assertEqual(res, [1] * N)

        methods = [
            test_query,
            test_query_single,
            test_execute,
        ]

        with tb.silence_asyncio_long_exec_warning():
            for method in methods:
                with self.subTest(method=method.__name__):
                    await run(200, method)

    async def test_client_handles_transaction_exit_in_asyncgen_1(self):
        client = self.create_client(max_concurrency=1)

        async def iterate():
            async for tx in client.transaction():
                async with tx:
                    for record in await tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            agen = iterate()
            try:
                async for _ in agen:  # noqa
                    raise MyException()
            finally:
                await agen.aclose()

        await client.aclose()

    async def test_client_handles_transaction_exit_in_asyncgen_2(self):
        client = self.create_client(max_concurrency=1)

        async def iterate():
            async for tx in client.transaction():
                async with tx:
                    for record in await tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            iterator = iterate()
            try:
                async for _ in iterator:  # noqa
                    raise MyException()
            finally:
                await iterator.aclose()

            del iterator

        await client.aclose()

    async def test_client_handles_asyncgen_finalization(self):
        client = self.create_client(max_concurrency=1)

        async def iterate(tx):
            for record in await tx.query("SELECT {1, 2, 3}"):
                yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async for tx in client.transaction():
                async with tx:
                    agen = iterate(tx)
                    try:
                        async for _ in agen:  # noqa
                            raise MyException()
                    finally:
                        await agen.aclose()

        await client.aclose()

    async def test_client_close_waits_for_release(self):
        client = self.create_client(max_concurrency=1)

        flag = self.loop.create_future()
        conn_released = False

        async def worker():
            nonlocal conn_released

            async for tx in client.transaction():
                async with tx:
                    await tx.query("SELECT 42")
                    flag.set_result(True)
                    await asyncio.sleep(0.1)

            conn_released = True

        self.loop.create_task(worker())

        await flag
        await client.aclose()
        self.assertTrue(conn_released)

    async def test_client_close_timeout(self):
        client = self.create_client(max_concurrency=1)

        flag = self.loop.create_future()

        async def worker():
            async for tx in client.transaction():
                async with tx:
                    await tx.query_single("SELECT 42")
                    flag.set_result(True)
                    await asyncio.sleep(0.5)

        task = self.loop.create_task(worker())

        with self.assertRaises(asyncio.TimeoutError):
            await flag
            await asyncio.wait_for(client.aclose(), timeout=0.1)

        with self.assertRaises(errors.ClientConnectionClosedError):
            await task

    async def test_client_expire_connections(self):
        class SlowCloseConnection(asyncio_client.AsyncIOConnection):
            async def close(self, timeout=None):
                await asyncio.sleep(0.2)
                await super().close(timeout=timeout)

        client = self.create_client(
            max_concurrency=1, connection_class=SlowCloseConnection
        )

        async for tx in client.transaction():
            async with tx:
                await tx.query("SELECT 42")
                self.assertIsNotNone(client._impl._holders[0]._con)
                client._impl.expire_connections()

        self.assertIsNone(client._impl._holders[0]._con)

        await client.query("SELECT 42")
        self.assertIsNotNone(client._impl._holders[0]._con)

        client._impl.expire_connections()
        async for tx in client.transaction():
            async with tx:
                await tx.query("SELECT 42")
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.query("SELECT 42"), 1)

        await client.aclose()

    async def test_client_properties(self):
        max_concurrency = 2

        client = self.create_client(max_concurrency=max_concurrency)
        self.assertEqual(client.max_concurrency, max_concurrency)
        self.assertEqual(client.max_concurrency, max_concurrency)

        async for tx in client.transaction():
            async with tx:
                await tx.query("SELECT 42")
                self.assertEqual(client.free_size, max_concurrency - 1)

        self.assertEqual(client.free_size, max_concurrency)

        await client.aclose()

    async def _test_connection_broken(self, executor, broken_evt):
        broken_evt.set()

        with self.assertRaises(errors.ClientConnectionError):
            await executor.query_single("SELECT 123")

        broken_evt.clear()

        self.assertEqual(await executor.query_single("SELECT 123"), 123)
        broken_evt.set()
        with self.assertRaises(errors.ClientConnectionError):
            await executor.query_single("SELECT 123")
        broken_evt.clear()
        self.assertEqual(await executor.query_single("SELECT 123"), 123)
        broken_evt.set()
        broken_evt.clear()
        self.assertEqual(await executor.query_single("SELECT 123"), 123)

        tested = False
        async for tx in executor.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 123"), 123)
                if tested:
                    break
                tested = True
                broken_evt.set()
                try:
                    await tx.query_single("SELECT 123")
                except errors.ClientConnectionError:
                    broken_evt.clear()
                    raise
                else:
                    self.fail("ConnectionError not raised!")

    async def test_client_connection_broken(self):
        con_args = self.get_connect_args()
        broken = asyncio.Event()
        done = asyncio.Event()

        async def proxy(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            while True:
                reader = self.loop.create_task(r.read(65536))
                waiter = self.loop.create_task(broken.wait())
                await asyncio.wait(
                    [reader, waiter],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if waiter.done():
                    reader.cancel()
                    w.close()
                    break
                else:
                    waiter.cancel()
                    data = await reader
                    if not data:
                        w.close()
                        break
                    w.write(data)

        async def cb(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            ur, uw = await asyncio.open_connection(
                con_args['host'], con_args['port']
            )
            done.clear()
            task = self.loop.create_task(proxy(r, uw))
            try:
                await proxy(ur, w)
            finally:
                try:
                    await task
                finally:
                    done.set()
                    w.close()
                    uw.close()

        server = await asyncio.start_server(
            cb, '127.0.0.1', 0
        )
        port = server.sockets[0].getsockname()[1]
        client = self.create_client(
            host='127.0.0.1',
            port=port,
            max_concurrency=1,
            wait_until_available=5,
        )
        try:
            await self._test_connection_broken(client, broken)
        finally:
            await asyncio.wait_for(client.aclose(), 5)
            server.close()
            await server.wait_closed()
            broken.set()
            await done.wait()

    async def test_client_suggested_concurrency(self):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.get_database_name()
        conargs["timeout"] = 120

        client = gel.create_async_client(**conargs)

        self.assertEqual(client.max_concurrency, 1)

        await client.ensure_connected()
        self.assertGreater(client.max_concurrency, 1)

        await client.aclose()

        client = gel.create_async_client(**conargs, max_concurrency=5)

        self.assertEqual(client.max_concurrency, 5)

        await client.ensure_connected()
        self.assertEqual(client.max_concurrency, 5)

        await client.aclose()

    def test_client_with_different_loop(self):
        conargs = self.get_connect_args()
        client = gel.create_async_client(**conargs)

        async def test():
            self.assertIsNot(asyncio.get_event_loop(), self.loop)
            result = await client.query_single("SELECT 42")
            self.assertEqual(result, 42)
            await asyncio.gather(
                client.query_single("SELECT 42"),
                client.query_single("SELECT 42"),
            )
            await client.aclose()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(test())
        asyncio.set_event_loop(self.loop)
