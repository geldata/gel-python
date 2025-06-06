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


from collections import namedtuple, UserDict

import gel
from gel import _testbase as tb


class TestNamedTupleTypes(tb.SyncQueryTestCase):

    async def test_namedtuple_01(self):
        NT1 = namedtuple('NT2', ['x', 'y'])
        NT2 = namedtuple('NT2', ['y', 'x'])

        ctors = [dict, UserDict, NT1, NT2]
        for ctor in ctors:
            val = ctor(x=10, y='y')
            res = self.client.query_single('''
                select <tuple<x: int64, y: str>>$0
            ''', val)

            self.assertEqual(res, (10, 'y'))

    async def test_namedtuple_02(self):
        NT1 = namedtuple('NT2', ['x', 'z'])

        with self.assertRaisesRegex(gel.InvalidArgumentError, 'is missing'):
            self.client.query_single('''
                select <tuple<x: int64, y: str>>$0
            ''', dict(x=20, z='test'))

        with self.assertRaisesRegex(gel.InvalidArgumentError, 'is missing'):
            self.client.query_single('''
                select <tuple<x: int64, y: str>>$0
            ''', NT1(x=20, z='test'))
