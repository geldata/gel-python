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

from gel._internal import _testbase as tb
import gel

import array
import math
import unittest


from gel._internal import _testbase


# An array.array subtype where indexing doesn't work.
# We use this to verify that the non-boxing memoryview based
# fast path works, since the slow path won't work on this object.
class brokenarray(array.array):
    def __getitem__(self, i):
        raise AssertionError("the fast path wasn't used!")


class TestVector(tb.SyncQueryTestCase):

    PGVECTOR_VER = None
    # no parallelizing to avoid extension setup hassle
    PARALLELISM_GRANULARITY = "suite"

    @classmethod
    async def set_up_class_once(cls, ui: _testbase.UI) -> None:
        await super().set_up_class_once(ui)

        dbname = cls.get_base_database_name()

        async with cls.async_test_client(database=dbname) as client:
            cls.PGVECTOR_VER = await client.query_single('''
                select assert_single((
                    select sys::ExtensionPackage filter .name = 'pgvector'
                )).version
            ''')

            if cls.PGVECTOR_VER is None:
                raise unittest.SkipTest("feature not implemented")

            try:
                await client.execute('''
                    create extension pgvector;
                ''')
            except gel.SchemaError:
                if cls.db_cache_enabled:
                    pass
                else:
                    raise

    @classmethod
    async def tear_down_class_once(cls, ui: _testbase.UI) -> None:
        try:
            dbname = cls.get_base_database_name()
            async with cls.async_test_client(database=dbname) as client:
                await client.execute('''
                    drop extension pgvector;
                ''')
        finally:
            await super().tear_down_class_once(ui)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.PGVECTOR_VER = cls.client.query_single('''
            select assert_single((
              select sys::ExtensionPackage filter .name = 'pgvector'
            )).version
        ''')

        if cls.PGVECTOR_VER is None:
            raise unittest.SkipTest("feature not implemented")

    def test_vector_01(self):
        val = self.client.query_single('''
            select <ext::pgvector::vector>[1.5,2.0,3.8]
        ''')
        self.assertTrue(isinstance(val, array.array))
        self.assertEqual(val, array.array('f', [1.5, 2.0, 3.8]))

        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            [3.0, 9.0, -42.5],
        )
        self.assertEqual(val, '[3, 9, -42.5]')

        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            array.array('f', [3.0, 9.0, -42.5])
        )
        self.assertEqual(val, '[3, 9, -42.5]')

        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            array.array('i', [1, 2, 3]),
        )
        self.assertEqual(val, '[1, 2, 3]')

        # Test that the fast-path works: if the encoder tries to
        # call __getitem__ on this brokenarray, it will fail.
        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            brokenarray('f', [3.0, 9.0, -42.5])
        )
        self.assertEqual(val, '[3, 9, -42.5]')

        # I don't think it's worth adding a dependency to test this,
        # but this works too:
        # import numpy as np
        # val = self.client.query_single(
        #     '''
        #     select <json><ext::pgvector::vector>$0
        #     ''',
        #     np.asarray([3.0, 9.0, -42.5], dtype=np.float32),
        # )
        # self.assertEqual(val, '[3,9,-42.5]')

        # Some sad path tests
        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::vector>$0
                ''',
                [3.0, None, -42.5],
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::vector>$0
                ''',
                [3.0, 'x', -42.5],
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::vector>$0
                ''',
                'foo',
            )

    def test_vector_02(self):
        if self.PGVECTOR_VER < (0, 7):
            self.skipTest("need at least pgvector 0.7.4 for sparsevec")

        val = self.client.query_single(
            '''
            select <ext::pgvector::sparsevec>
                <ext::pgvector::vector>[0, 1.5, 2.0, 3.8, 0, 0]
            ''',
        )
        self.assertEqual(val['dim'], 6)
        self.assertEqual(val[1], 1.5)
        self.assertEqual(val[2], 2)
        self.assertTrue(math.isclose(val[3], 3.8, abs_tol=1e-6))

        val = self.client.query_single(
            '''
            select <array<float32>><ext::pgvector::vector>
                <ext::pgvector::sparsevec>$0
            ''',
            {'dim': 6, 1: 1.5, 2: 2, 3: 3.8},
        )
        self.assertEqual(len(val), 6)
        self.assertEqual(val[0], 0)
        self.assertEqual(val[1], 1.5)
        self.assertEqual(val[2], 2)
        self.assertTrue(math.isclose(val[3], 3.8, abs_tol=1e-6))
        self.assertEqual(val[4], 0)
        self.assertEqual(val[5], 0)

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::sparsevec>$0
                ''',
                {'dim': 1, 1: 1.5, 2: 2, 3: 3.8},
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::sparsevec>$0
                ''',
                {'dims': 1, 1: 1.5, 2: 2, 3: 3.8},
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::sparsevec>$0
                ''',
                {'dim': 6, 1: 1.5, 2: 2, 3: '3.8'},
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::sparsevec>$0
                ''',
                {'dim': 6, 1: 1.5, 2: 2, '3': 3.8},
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::sparsevec>$0
                ''',
                {'dim': 6, 1: 1.5, 2: 2, 3: 0},
            )

    def test_vector_03(self):
        if self.PGVECTOR_VER < (0, 7):
            self.skipTest("need at least pgvector 0.7.4 for halfvec")

        val = self.client.query_single(
            '''
            select <ext::pgvector::halfvec>
                [1.5, 2.0, 3.8, 0, 3.4575e-3, 65000,
                 6.0975e-5, 2.2345e-7, -5.96e-8]
            ''',
        )

        self.assertTrue(isinstance(val, array.array))
        self.assertEqual(val[0], 1.5)
        self.assertEqual(val[1], 2)
        self.assertTrue(math.isclose(val[2], 3.80, rel_tol=1e-3))
        self.assertEqual(val[3], 0)
        self.assertTrue(math.isclose(val[4], 3.457e-3, rel_tol=1e-3))
        self.assertTrue(math.isclose(val[5], 65000, rel_tol=1e-3))
        # These values are sub-normal so they don't map perfectly onto f32
        self.assertTrue(math.isclose(val[6], 6.0975e-5, rel_tol=1e-2))
        self.assertTrue(math.isclose(val[7], 2.38e-7, rel_tol=1e-2))
        self.assertTrue(math.isclose(val[8], -5.96e-8, rel_tol=1e-2))

        val = self.client.query_single(
            '''
            select <array<float32>><ext::pgvector::halfvec>$0
            ''',
            [1.5, 2.0, 3.8, 0, 3.4575e-3, 65000,
             6.0975e-5, 2.385e-7, -5.97e-8],
        )

        self.assertEqual(val[0], 1.5)
        self.assertEqual(val[1], 2)
        self.assertTrue(math.isclose(val[2], 3.80, rel_tol=1e-3))
        self.assertEqual(val[3], 0)
        self.assertTrue(math.isclose(val[4], 3.457e-3, rel_tol=1e-3))
        self.assertTrue(math.isclose(val[5], 65000, rel_tol=1e-3))
        # These values are sub-normal so they don't map perfectly onto f32
        self.assertTrue(math.isclose(val[6], 6.0975e-5, rel_tol=1e-2))
        self.assertTrue(math.isclose(val[7], 2.38e-7, rel_tol=1e-2))
        self.assertTrue(math.isclose(val[8], -5.96e-8, rel_tol=1e-2))

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::halfvec>$0
                ''',
                [3.0, None, -42.5],
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::halfvec>$0
                ''',
                [3.0, 'x', -42.5],
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::halfvec>$0
                ''',
                'foo',
            )

        with self.assertRaises(gel.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::halfvec>$0
                ''',
                [1_000_000],
            )
