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


import asyncio
import threading
import queue
import unittest.mock
from concurrent import futures

import gel
from gel import _testbase as tb


class Barrier:
    def __init__(self, number):
        self._counter = number
        self._cond = threading.Condition()

    def ready(self):
        if self._counter == 0:
            return
        with self._cond:
            self._counter -= 1
            assert self._counter >= 0, self._counter
            if self._counter == 0:
                self._cond.notify_all()
            else:
                self._cond.wait_for(lambda: self._counter == 0)


class TestSyncRetry(tb.SyncQueryTestCase):

    SETUP = '''
        CREATE TYPE test::Counter EXTENDING std::Object {
            CREATE PROPERTY name -> std::str {
                CREATE CONSTRAINT std::exclusive;
            };
            CREATE PROPERTY value -> std::int32 {
                SET default := 0;
            };
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::Counter;
    '''

    def test_sync_retry_01(self):
        for tx in self.client.transaction():
            with tx:
                tx.execute('''
                    INSERT test::Counter {
                        name := 'counter1'
                    };
                ''')

    def test_sync_retry_02(self):
        with self.assertRaises(ZeroDivisionError):
            for tx in self.client.transaction():
                with tx:
                    tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_02'
                        };
                    ''')
                    1 / 0
        with self.assertRaises(gel.NoDataError):
            self.client.query_required_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            ''')
        self.assertEqual(
            self.client.query_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            '''),
            None
        )

    def test_sync_retry_begin(self):
        patcher = unittest.mock.patch(
            "gel.base_client.BaseConnection.privileged_execute"
        )
        _start = patcher.start()

        def cleanup():
            try:
                patcher.stop()
            except RuntimeError:
                pass

        self.addCleanup(cleanup)

        _start.side_effect = gel.BackendUnavailableError()

        with self.assertRaises(gel.BackendUnavailableError):
            for tx in self.client.transaction():
                with tx:
                    tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_begin'
                        };
                    ''')
        with self.assertRaises(gel.NoDataError):
            self.client.query_required_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_begin'
            ''')
        self.assertEqual(
            self.client.query_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_begin'
            '''),
            None
        )

        def recover_after_first_error(*_, **__):
            patcher.stop()
            raise gel.BackendUnavailableError()

        _start.side_effect = recover_after_first_error
        call_count = _start.call_count

        for tx in self.client.transaction():
            with tx:
                tx.execute('''
                    INSERT test::Counter {
                        name := 'counter_retry_begin'
                    };
                ''')
        self.assertEqual(_start.call_count, call_count + 1)
        self.client.query_single('''
            SELECT test::Counter
            FILTER .name = 'counter_retry_begin'
        ''')

    def test_sync_retry_conflict(self):
        self.execute_conflict('counter2')

    def test_sync_conflict_no_retry(self):
        with self.assertRaises(gel.TransactionSerializationError):
            self.execute_conflict(
                'counter3',
                gel.RetryOptions(attempts=1, backoff=gel.default_backoff)
            )

    def execute_conflict(self, name='counter2', options=None):
        con_args = self.get_connect_args().copy()
        con_args.update(database=self.get_database_name())
        client2 = gel.create_client(**con_args)
        self.addCleanup(client2.close)

        barrier = Barrier(2)
        lock = threading.Lock()

        iterations = 0

        def transaction1(client):
            for tx in client.transaction():
                nonlocal iterations
                iterations += 1
                with tx:
                    # This magic query makes the test more reliable for some
                    # reason. I guess this is because starting a transaction
                    # in EdgeDB (and/or Postgres) is accomplished somewhat
                    # lazily, i.e. only start transaction on the first query
                    # rather than on the `START TRANSACTION`.
                    tx.query("SELECT 1")

                    # Start both transactions at the same initial data.
                    # One should succeed other should fail and retry.
                    # On next attempt, the latter should succeed
                    barrier.ready()

                    lock.acquire()
                    res = tx.query_single('''
                        SELECT (
                            INSERT test::Counter {
                                name := <str>$name,
                                value := 1,
                            } UNLESS CONFLICT ON .name
                            ELSE (
                                UPDATE test::Counter
                                SET { value := .value + 1 }
                            )
                        ).value
                    ''', name=name)
                lock.release()
            return res

        client = self.client
        if options:
            client = client.with_retry_options(options)
            client2 = client2.with_retry_options(options)

        with futures.ThreadPoolExecutor(2) as pool:
            f1 = pool.submit(transaction1, client)
            f2 = pool.submit(transaction1, client2)
            results = {f1.result(), f2.result()}

        self.assertEqual(results, {1, 2})
        self.assertEqual(iterations, 3)

    def test_sync_batch_retry_conflict(self):
        self.batch_conflict('counter4')
        self.assertEqual(
            self.client.query_single(
                '''
                SELECT (
                    SELECT test::Counter
                    FILTER .name = <str>$name
                ).value
                '''
                , name='counter4'
            ),
            2,
        )

    def test_sync_batch_conflict_no_retry(self):
        with self.assertRaises(gel.TransactionSerializationError):
            self.batch_conflict(
                'counter5',
                gel.RetryOptions(attempts=1, backoff=gel.default_backoff)
            )
        self.assertEqual(
            self.client.query_single(
                '''
                SELECT (
                    SELECT test::Counter
                    FILTER .name = <str>$name
                ).value
                '''
                , name='counter5'
            ),
            1,
        )

    def batch_conflict(self, name, options=None):
        con_args = self.get_connect_args().copy()
        con_args.update(database=self.get_database_name())
        client2 = gel.create_client(**con_args)
        self.addCleanup(client2.close)

        barrier = Barrier(2)
        lock = threading.Lock()

        iterations = 0

        def transaction1(client):
            for tx in client._batch():
                nonlocal iterations
                iterations += 1
                with tx:
                    # Actually ensure the transaction is started in backend
                    tx.send_query("SELECT 1")
                    tx.wait()

                    # Start both transactions at the same initial data.
                    # One should succeed other should fail and retry.
                    # On next attempt, the latter should succeed
                    barrier.ready()

                    lock.acquire()
                    tx.send_query_single('''
                        SELECT (
                            INSERT test::Counter {
                                name := <str>$name,
                                value := 1,
                            } UNLESS CONFLICT ON .name
                            ELSE (
                                UPDATE test::Counter
                                SET { value := .value + 1 }
                            )
                        ).value
                    ''', name=name)
                lock.release()

        client = self.client
        if options:
            client = client.with_retry_options(options)
            client2 = client2.with_retry_options(options)

        with futures.ThreadPoolExecutor(2) as pool:
            f1 = pool.submit(transaction1, client)
            f2 = pool.submit(transaction1, client2)
        f1.result()
        f2.result()

        self.assertEqual(iterations, 3)

    def test_sync_transaction_interface_errors(self):
        with self.assertRaisesRegex(
            AttributeError,
            "'Iteration' object has no attribute 'start'",
        ):
            for tx in self.client.transaction():
                with tx:
                    tx.start()

        with self.assertRaisesRegex(
            AttributeError,
            "'Iteration' object has no attribute 'rollback'",
        ):
            for tx in self.client.transaction():
                with tx:
                    tx.rollback()

        with self.assertRaisesRegex(
            AttributeError,
            "'Iteration' object has no attribute 'start'",
        ):
            for tx in self.client.transaction():
                tx.start()

        with self.assertRaisesRegex(gel.InterfaceError,
                                    r'.*Use `with transaction:`'):
            for tx in self.client.transaction():
                tx.execute("SELECT 123")

        with self.assertRaisesRegex(
            gel.InterfaceError,
            r"already in a `with` block",
        ):
            for tx in self.client.transaction():
                with tx:
                    with tx:
                        pass

    def test_sync_retry_parse(self):
        loop = asyncio.new_event_loop()
        q = queue.Queue()

        async def init():
            return asyncio.Event(), asyncio.Event()

        reconnect, terminate = loop.run_until_complete(init())

        async def proxy(r, w):
            try:
                while True:
                    buf = await r.read(65536)
                    if not buf:
                        w.close()
                        break
                    w.write(buf)
            except asyncio.CancelledError:
                pass

        async def cb(ri, wi):
            try:
                args = self.get_connect_args()
                ro, wo = await asyncio.open_connection(
                    args["host"], args["port"]
                )
                try:
                    fs = [
                        asyncio.create_task(proxy(ri, wo)),
                        asyncio.create_task(proxy(ro, wi)),
                        asyncio.create_task(terminate.wait()),
                    ]
                    if not reconnect.is_set():
                        fs.append(asyncio.create_task(reconnect.wait()))
                    _, pending = await asyncio.wait(
                        fs, return_when=asyncio.FIRST_COMPLETED
                    )
                    for f in pending:
                        f.cancel()
                finally:
                    wo.close()
            finally:
                wi.close()

        async def proxy_server():
            srv = await asyncio.start_server(cb, host="127.0.0.1", port=0)
            try:
                q.put(srv.sockets[0].getsockname()[1])
                await terminate.wait()
            finally:
                srv.close()
                await srv.wait_closed()

        with futures.ThreadPoolExecutor(1) as pool:
            pool.submit(loop.run_until_complete, proxy_server())
            try:
                client = self.make_test_client(
                    host="127.0.0.1",
                    port=q.get(),
                    database=self.get_database_name(),
                )

                # Fill the connection pool with a healthy connection
                self.assertEqual(client.query_single("SELECT 42"), 42)

                # Cut the connection to simulate an Internet interruption
                loop.call_soon_threadsafe(reconnect.set)

                # Run a new query that was never compiled, retry should work
                self.assertEqual(client.query_single("SELECT 1*2+3-4"), 1)
            finally:
                loop.call_soon_threadsafe(terminate.set)
