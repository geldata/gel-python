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
import queue
import random
import threading
import time

import gel

from gel._internal import _testbase as tb
from gel import errors
from gel import blocking_client


class TestBlockingClient(tb.SyncQueryTestCase):
    def create_client(self, **kwargs):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.get_database_name()
        conargs["timeout"] = 120
        conargs.update(kwargs)
        conargs.setdefault(
            "connection_class", blocking_client.BlockingIOConnection
        )
        conargs.setdefault("max_concurrency", None)

        return tb.TestClient(**conargs)

    def test_client_01(self):
        for n in {1, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = self.create_client(max_concurrency=10)

                def worker():
                    self.assertEqual(client.query_single("SELECT 1"), 1)

                tasks = [threading.Thread(target=worker) for _ in range(n)]
                for task in tasks:
                    task.start()
                for task in tasks:
                    task.join()
                client.close()

    def test_client_02(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                with self.create_client(max_concurrency=5) as client:

                    def worker():
                        self.assertEqual(client.query_single("SELECT 1"), 1)

                    tasks = [threading.Thread(target=worker) for _ in range(n)]
                    for task in tasks:
                        task.start()
                    for task in tasks:
                        task.join()

    def test_client_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = self.create_client(max_concurrency=10)

                def worker():
                    self.assertEqual(client.query('SELECT 1'), [1])
                    self.assertEqual(client.query_single('SELECT 1'), 1)
                    self.assertEqual(client.query_json('SELECT 1'), '[1]')
                    self.assertEqual(client.query_single_json('SELECT 1'), '1')

                tasks = [threading.Thread(target=worker) for _ in range(n)]
                for task in tasks:
                    task.start()
                for task in tasks:
                    task.join()
                client.close()

    def test_client_transaction(self):
        client = self.create_client(max_concurrency=1)

        for tx in client.transaction():
            with tx:
                self.assertEqual(tx.query_single("SELECT 7*8"), 56)

        client.close()

    def test_client_options(self):
        client = self.create_client(max_concurrency=1)

        client.with_transaction_options(
            gel.TransactionOptions(readonly=True))
        client.with_retry_options(
            gel.RetryOptions(attempts=1, backoff=gel.default_backoff))
        for tx in client.transaction():
            with tx:
                self.assertEqual(tx.query_single("SELECT 7*8"), 56)

        client.close()

    def test_client_init_run_until_complete(self):
        client = self.create_client()
        self.assertIsInstance(client, blocking_client.Client)
        client.close()

    def test_client_no_acquire_deadlock(self):
        with self.create_client(
            max_concurrency=1,
        ) as client:

            has_sleep = client.query_single("""
                SELECT EXISTS(
                    SELECT schema::Function FILTER .name = 'sys::_sleep'
                )
            """)
            if not has_sleep:
                self.skipTest("No sys::_sleep function")

            def sleep_and_release():
                client.execute("SELECT sys::_sleep(1)")

            task = threading.Thread(target=sleep_and_release)
            task.start()
            time.sleep(0.5)

            client.query_single("SELECT 1")
            task.join()

    def test_client_config_persistence(self):
        N = 100

        class MyConnection(blocking_client.BlockingIOConnection):
            async def raw_query(self, query_context):
                res = await super().raw_query(query_context)
                return res + 1

        def test():
            for tx in client.transaction():
                with tx:
                    self.assertEqual(tx.query_single("SELECT 1"), 2)
                    # make this test more reliable by spending more time in
                    # the transaction to hit max_concurrency sooner
                    time.sleep(random.random() / 100)

        with self.create_client(
            max_concurrency=10,
            connection_class=MyConnection,
        ) as client:

            tasks = [threading.Thread(target=test) for _ in range(N)]
            for task in tasks:
                task.start()
            for task in tasks:
                task.join()

            self.assertEqual(
                sum(
                    1
                    for ch in client._impl._holders
                    if ch._con and not ch._con.is_closed()
                ),
                10,
            )

    def test_client_connection_methods(self):
        def test_query(client, q):
            i = random.randint(0, 20)
            time.sleep(random.random() / 100)
            r = client.query("SELECT {}".format(i))
            self.assertEqual(list(r), [i])
            q.put(1)

        def test_query_single(client, q):
            i = random.randint(0, 20)
            time.sleep(random.random() / 100)
            r = client.query_single("SELECT {}".format(i))
            self.assertEqual(r, i)
            q.put(1)

        def test_execute(client, q):
            time.sleep(random.random() / 100)
            client.execute("SELECT {1, 2, 3, 4}")
            q.put(1)

        def wrapper(func, client, q):
            try:
                func(client, q)
            except Exception as e:
                q.put(e)
                raise

        def run(N, meth):
            with self.create_client(max_concurrency=10) as client:
                q = queue.Queue()
                coros = [
                    threading.Thread(target=wrapper, args=(meth, client, q))
                    for _ in range(N)
                ]
                for coro in coros:
                    coro.start()
                for coro in coros:
                    coro.join()
                res = []
                while not q.empty():
                    res.append(q.get_nowait())
                for el in res:
                    if isinstance(el, Exception):
                        raise el
                self.assertEqual(res, [1] * N)

        methods = [
            test_query,
            test_query_single,
            test_execute,
        ]

        for method in methods:
            with self.subTest(method=method.__name__):
                run(200, method)

    def test_client_handles_transaction_exit_in_gen_1(self):
        client = self.create_client(max_concurrency=1)

        def iterate():
            for tx in client.transaction():
                with tx:
                    for record in tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            agen = iterate()
            try:
                for _ in agen:  # noqa
                    raise MyException()
            finally:
                agen.close()

        client.close()

    def test_client_handles_transaction_exit_in_gen_2(self):
        client = self.create_client(max_concurrency=1)

        def iterate():
            for tx in client.transaction():
                with tx:
                    for record in tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            iterator = iterate()
            try:
                for _ in iterator:  # noqa
                    raise MyException()
            finally:
                iterator.close()

            del iterator

        client.close()

    def test_client_handles_gen_finalization(self):
        client = self.create_client(max_concurrency=1)

        def iterate(tx):
            for record in tx.query("SELECT {1, 2, 3}"):
                yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            for tx in client.transaction():
                with tx:
                    agen = iterate(tx)
                    try:
                        for _ in agen:  # noqa
                            raise MyException()
                    finally:
                        agen.close()

        client.close()

    def test_client_close_waits_for_release(self):
        client = self.create_client(max_concurrency=1)

        flag = threading.Event()
        conn_released = False

        def worker():
            nonlocal conn_released

            for tx in client.transaction():
                with tx:
                    tx.query("SELECT 42")
                    flag.set()
                    time.sleep(0.1)

            conn_released = True

        task = threading.Thread(target=worker)
        task.start()

        flag.wait()
        client.close()
        self.assertTrue(conn_released)
        task.join()

    def test_client_close_timeout(self):
        client = self.create_client(max_concurrency=1)

        flag = threading.Event()

        def worker():
            with self.assertRaises(errors.ClientConnectionClosedError):
                for tx in client.transaction():
                    with tx:
                        tx.query_single("SELECT 42")
                        flag.set()
                        time.sleep(0.5)

        task = threading.Thread(target=worker)
        task.start()

        with self.assertRaises(errors.InterfaceError):
            flag.wait()
            client.close(timeout=0.1)

        task.join()

    def test_client_expire_connections(self):
        client = self.create_client(max_concurrency=1)

        for tx in client.transaction():
            with tx:
                tx.query("SELECT 42")
                client._impl.expire_connections()

        self.assertIsNone(client._impl._holders[0]._con)
        client.close()

    def test_client_properties(self):
        max_concurrency = 2

        client = self.create_client(max_concurrency=max_concurrency)
        self.assertEqual(client.max_concurrency, max_concurrency)
        self.assertEqual(client.max_concurrency, max_concurrency)

        for tx in client.transaction():
            with tx:
                tx.query("SELECT 42")
                self.assertEqual(client.free_size, max_concurrency - 1)

        self.assertEqual(client.free_size, max_concurrency)

        client.close()

    def _test_connection_broken(self, executor, broken_evt):
        self.loop.call_soon_threadsafe(broken_evt.set)

        with self.assertRaises(errors.ClientConnectionError):
            executor.query_single("SELECT 123")

        self.loop.call_soon_threadsafe(broken_evt.clear)

        self.assertEqual(executor.query_single("SELECT 123"), 123)
        self.loop.call_soon_threadsafe(broken_evt.set)
        with self.assertRaises(errors.ClientConnectionError):
            executor.query_single("SELECT 123")
        self.loop.call_soon_threadsafe(broken_evt.clear)
        self.assertEqual(executor.query_single("SELECT 123"), 123)
        self.loop.call_soon_threadsafe(broken_evt.set)
        self.loop.call_soon_threadsafe(broken_evt.clear)
        self.assertEqual(executor.query_single("SELECT 123"), 123)

        tested = False
        for tx in executor.transaction():
            with tx:
                self.assertEqual(tx.query_single("SELECT 123"), 123)
                if tested:
                    break
                tested = True
                self.loop.call_soon_threadsafe(broken_evt.set)
                try:
                    tx.query_single("SELECT 123")
                except errors.ClientConnectionError:
                    self.loop.call_soon_threadsafe(broken_evt.clear)
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
            await self.loop.run_in_executor(
                None, self._test_connection_broken, client, broken
            )
        finally:
            await self.loop.run_in_executor(None, client.close, 5)
            server.close()
            await server.wait_closed()
            broken.set()
            await done.wait()

    def test_client_suggested_concurrency(self):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.get_database_name()
        conargs["timeout"] = 120

        client = gel.create_client(**conargs)

        self.assertEqual(client.max_concurrency, 1)

        client.ensure_connected()
        self.assertGreater(client.max_concurrency, 1)

        client.close()

        client = gel.create_client(**conargs, max_concurrency=5)

        self.assertEqual(client.max_concurrency, 5)

        client.ensure_connected()
        self.assertEqual(client.max_concurrency, 5)

        client.close()
