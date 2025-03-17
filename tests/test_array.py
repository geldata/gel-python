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


from gel import _testbase as tb


class TestArrayOfArray(tb.SyncQueryTestCase):
    def setUp(self):
        super().setUp()

        if self.client.query_required_single('''
            select sys::get_version().major < 7
        '''):
            self.skipTest("Test needs nested arrays")

    async def test_array_of_array_01(self):
        # basic array of array
        self.assertEqual(
            self.client.query_single(
                'select <array<array<int64>>>[]'
            ),
            [[]],
        )
        self.assertEqual(
            self.client.query_single(
                'select [[1]]'
            ),
            [[1]],
        )
        self.assertEqual(
            self.client.query_single(
                'select [[[1]]]'
            ),
            [[[1]]],
        )
        self.assertEqual(
            self.client.query_single(
                'select [[[[1]]]]'
            ),
            [[[[1]]]],
        )
        self.assertEqual(
            self.client.query_single(
                'select [[1], [2, 3], [4, 5, 6, 7]]'
            ),
            [[1], [2, 3], [4, 5, 6, 7]],
        )

    async def test_array_of_array_02(self):
        # check that array tuple array still works
        self.assertEqual(
            self.client.query_single(
                'select [([1],)]'
            ),
            [([1],)],
        )

    async def test_array_of_array_03(self):
        # check encoding array of array
        self.assertEqual(
            self.client.query_single(
                'select <array<array<int64>>>$0',
                [[1]],
            ),
            [[1]],
        )
