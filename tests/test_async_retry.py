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

import logging
import unittest.mock

import edgedb
from edgedb import errors
from edgedb import RetryOptions
from gel import _testbase as tb

log = logging.getLogger(__name__)


class Barrier:
    def __init__(self, number):
        self._counter = number
        self._cond = asyncio.Condition()

    async def ready(self):
        if self._counter == 0:
            return
        async with self._cond:
            self._counter -= 1
            assert self._counter >= 0, self._counter
            if self._counter == 0:
                self._cond.notify_all()
            else:
                await self._cond.wait_for(lambda: self._counter == 0)


class TestAsyncRetry(tb.AsyncQueryTestCase):

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

    async def test_async_retry_01(self):
        async for tx in self.client.transaction():
            async with tx:
                await tx.execute('''
                    INSERT test::Counter {
                        name := 'counter1'
                    };
                ''')

    async def test_async_retry_02(self):
        with self.assertRaises(ZeroDivisionError):
            async for tx in self.client.transaction():
                async with tx:
                    await tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_02'
                        };
                    ''')
                    1 / 0
        with self.assertRaises(edgedb.NoDataError):
            await self.client.query_required_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            ''')

    async def test_async_retry_begin(self):
        patcher = unittest.mock.patch(
            "edgedb.base_client.BaseConnection.privileged_execute"
        )
        _start = patcher.start()

        def cleanup():
            try:
                patcher.stop()
            except RuntimeError:
                pass

        self.addCleanup(cleanup)

        _start.side_effect = errors.BackendUnavailableError()

        with self.assertRaises(errors.BackendUnavailableError):
            async for tx in self.client.transaction():
                async with tx:
                    await tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_begin'
                        };
                    ''')
        with self.assertRaises(edgedb.NoDataError):
            await self.client.query_required_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_begin'
            ''')

        async def recover_after_first_error(*_, **__):
            patcher.stop()
            raise errors.BackendUnavailableError()

        _start.side_effect = recover_after_first_error
        call_count = _start.call_count

        async for tx in self.client.transaction():
            async with tx:
                await tx.execute('''
                    INSERT test::Counter {
                        name := 'counter_retry_begin'
                    };
                ''')
        self.assertEqual(_start.call_count, call_count + 1)
        await self.client.query_single('''
            SELECT test::Counter
            FILTER .name = 'counter_retry_begin'
        ''')

    async def test_async_retry_conflict(self):
        await self.execute_conflict('counter2')

    async def test_async_conflict_no_retry(self):
        with self.assertRaises(edgedb.TransactionSerializationError):
            await self.execute_conflict(
                'counter3',
                RetryOptions(attempts=1, backoff=edgedb.default_backoff)
            )

    async def execute_conflict(self, name='counter2', options=None):
        client2 = self.make_test_client(database=self.get_database_name())
        self.addCleanup(client2.aclose)

        barrier = Barrier(2)
        lock = asyncio.Lock()
        iterations = 0

        async def transaction1(client):
            async for tx in client.transaction():
                nonlocal iterations
                iterations += 1
                async with tx:
                    # This magic query makes the test more reliable for some
                    # reason. I guess this is because starting a transaction
                    # in EdgeDB (and/or Postgres) is accomplished somewhat
                    # lazily, i.e. only start transaction on the first query
                    # rather than on the `START TRANSACTION`.
                    await tx.query("SELECT 1")

                    # Start both transactions at the same initial data.
                    # One should succeed other should fail and retry.
                    # On next attempt, the latter should succeed
                    await barrier.ready()

                    await lock.acquire()
                    res = await tx.query_single('''
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

        results = await asyncio.wait_for(asyncio.gather(
            transaction1(client),
            transaction1(client2),
            return_exceptions=True,
        ), 10)
        for e in results:
            if isinstance(e, BaseException):
                raise e

        self.assertEqual(set(results), {1, 2})
        self.assertEqual(iterations, 3)

    async def test_async_retry_conflict_nontx_01(self):
        await self.execute_nontx_conflict(
            'counter_nontx_01',
            lambda client, *args, **kwargs: client.query(*args, **kwargs)
        )

    async def test_async_retry_conflict_nontx_02(self):
        await self.execute_nontx_conflict(
            'counter_nontx_02',
            lambda client, *args, **kwargs: client.execute(*args, **kwargs)
        )

    async def execute_nontx_conflict(self, name, func):
        # Test retries on conflicts in a non-tx setting.  We do this
        # by having conflicting upserts that are made long-running by
        # adding a sys::_sleep call.
        #
        # Unlike for the tx ones, we don't assert that a retry
        # actually was necessary, since that feels fragile in a
        # timing-based test like this.

        client1 = self.client
        client2 = self.make_test_client(database=self.get_database_name())
        self.addCleanup(client2.aclose)

        await client1.query("SELECT 1")
        await client2.query("SELECT 1")

        query = '''
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
            ORDER BY sys::_sleep(<int64>$sleep)
            THEN <int64>$nonce
        '''

        await func(client1, query, name=name, sleep=0, nonce=0)

        task1 = asyncio.create_task(
            func(client1, query, name=name, sleep=5, nonce=1)
        )
        task2 = asyncio.create_task(
            func(client2, query, name=name, sleep=5, nonce=2)
        )

        results = await asyncio.wait_for(asyncio.gather(
            task1,
            task2,
            return_exceptions=True,
        ), 20)

        excs = [e for e in results if isinstance(e, BaseException)]
        if excs:
            raise excs[0]
        val = await client1.query_single('''
            select (select test::Counter filter .name = <str>$name).value
        ''', name=name)

        self.assertEqual(val, 3)

    async def test_async_transaction_interface_errors(self):
        with self.assertRaisesRegex(
            AttributeError,
            "'AsyncIOIteration' object has no attribute 'start'",
        ):
            async for tx in self.client.transaction():
                async with tx:
                    await tx.start()

        with self.assertRaisesRegex(
            AttributeError,
            "'AsyncIOIteration' object has no attribute 'rollback'",
        ):
            async for tx in self.client.transaction():
                async with tx:
                    await tx.rollback()

        with self.assertRaisesRegex(
            AttributeError,
            "'AsyncIOIteration' object has no attribute 'start'",
        ):
            async for tx in self.client.transaction():
                await tx.start()

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*Use `async with transaction:`'):
            async for tx in self.client.transaction():
                await tx.execute("SELECT 123")

        with self.assertRaisesRegex(
            edgedb.InterfaceError,
            r"already in an `async with` block",
        ):
            async for tx in self.client.transaction():
                async with tx:
                    async with tx:
                        pass
