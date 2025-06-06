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


import pickle
import random
import unittest

import uuid
from uuid import UUID as std_UUID

from gel.protocol.protocol import UUID as c_UUID


special_uuids = frozenset({
    std_UUID('00000000-0000-0000-0000-000000000000'),
    std_UUID('00000000-0000-0000-0000-000000000001'),
    std_UUID('10000000-0000-0000-0000-000000000000'),
    std_UUID('10000000-0000-0000-0000-000000000001'),
    std_UUID('FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'),
    std_UUID('0F0F0F0F-0F0F-0F0F-0F0F-0F0F0F0F0F0F'),
    std_UUID('F0F0F0F0-F0F0-F0F0-F0F0-F0F0F0F0F0F0'),
})

test_uuids = tuple(
    special_uuids |
    frozenset({uuid.uuid4() for _ in range(1000)})
)


class TestUuid(unittest.TestCase):

    def ensure_equal(self, uuid1, uuid2):
        self.assertEqual(uuid1.bytes_le, uuid2.bytes_le)
        self.assertEqual(uuid1.clock_seq, uuid2.clock_seq)
        self.assertEqual(uuid1.fields, uuid2.fields)
        self.assertEqual(uuid1.bytes, uuid2.bytes)
        self.assertEqual(uuid1.int, uuid2.int)
        self.assertEqual(uuid1.hex, uuid2.hex)
        self.assertEqual(uuid1.variant, uuid2.variant)
        self.assertEqual(uuid1.version, uuid2.version)
        self.assertEqual(uuid1.urn, uuid2.urn)
        self.assertEqual(uuid1, uuid2)
        self.assertEqual(int(uuid1), int(uuid2))
        self.assertEqual(str(uuid1), str(uuid2))
        self.assertEqual(repr(uuid1), repr(uuid2))
        self.assertEqual(hash(uuid1), hash(uuid2))
        self.assertGreaterEqual(uuid1, uuid2)
        self.assertGreaterEqual(uuid2, uuid1)
        self.assertLessEqual(uuid1, uuid2)
        self.assertLessEqual(uuid2, uuid1)

    def test_uuid_ctr_01(self):
        with self.assertRaisesRegex(ValueError, r'invalid UUID.*got 4'):
            c_UUID('test')

        with self.assertRaisesRegex(ValueError,
                                    r'invalid UUID.*decodes to less'):
            c_UUID('49e3b4e4-4761-11e9-9160-2f38d067497')

        for v in {'49e3b4e4476111e991602f38d067497aaaaa',
                  '49e3b4e4476111e991602f38d067497aaaa',
                  '49e3b4e4476111e991602f38d067497aaa',
                  '49e3b4e4476111e991602f38d067497aa',
                  '49e3b4e4476111e-991602f-38d067497aa'}:
            with self.assertRaisesRegex(ValueError,
                                        r'invalid UUID.*decodes to more'):
                print(c_UUID(v))

        with self.assertRaisesRegex(ValueError,
                                    r"invalid UUID.*unexpected.*'x'"):
            c_UUID('49e3b4e4-4761-11e9-9160-2f38dx67497a')

        with self.assertRaisesRegex(ValueError,
                                    r"invalid UUID.*unexpected"):
            c_UUID('49e3b4e4-4761-11160-2f😱3867497a')

        with self.assertRaisesRegex(ValueError,
                                    r"invalid UUID.*unexpected"):
            c_UUID('49e3b4e4-4761-11eE-\xAA60-2f38dx67497a')

    def test_uuid_ctr_02(self):
        for py_u in test_uuids:
            c_u = c_UUID(py_u.bytes)
            self.ensure_equal(py_u, c_u)

        for py_u in test_uuids:
            c_u = c_UUID(str(py_u))
            self.ensure_equal(py_u, c_u)

    def test_uuid_pickle(self):
        u = c_UUID('de197476-4763-11e9-91bf-7311c6dc588e')
        d = pickle.dumps(u)
        u2 = pickle.loads(d)
        self.assertEqual(u, u2)
        self.assertEqual(str(u), str(u2))
        self.assertEqual(u.bytes, u2.bytes)

    def test_uuid_instance(self):
        u = c_UUID('de197476-4763-11e9-91bf-7311c6dc588e')
        self.assertTrue(isinstance(u, uuid.UUID))
        self.assertTrue(issubclass(c_UUID, uuid.UUID))

    def test_uuid_compare(self):
        u = c_UUID('de197476-4763-11e9-91bf-7311c6dc588e')
        us = uuid.UUID(bytes=u.bytes)

        for us2 in test_uuids:
            u2 = c_UUID(us2.bytes)

            if us < us2:
                self.assertLess(u, u2)
                self.assertGreater(u2, u)
            elif us > us2:
                self.assertGreater(u, u2)
                self.assertLess(u2, u)

        u3 = c_UUID('de197476-4763-11e9-91bf-7311c6dc588e')
        self.assertTrue(u == u3)
        self.assertFalse(u != u3)
        self.assertTrue(u >= u3)
        self.assertTrue(u <= u3)

        a = c_UUID('10000000-0000-0000-0000-000000000001')
        b = c_UUID('10000000-0000-0000-0000-000000000000')
        self.assertGreater(a, b)
        self.assertLess(b, a)

    def test_uuid_comp(self):
        for _ in range(1000):
            ll = random.choice(test_uuids)
            rr = random.choice(test_uuids)

            # Testing comparison operators.
            # Explicitly use > and not assertGreater;
            # we want all comparisons to be explicit in this test.

            if ll > rr:
                self.assertTrue(c_UUID(ll.bytes) > rr)
                self.assertTrue(c_UUID(ll.bytes) > c_UUID(rr.bytes))
                self.assertTrue(ll > c_UUID(rr.bytes))

            if ll < rr:
                self.assertTrue(c_UUID(ll.bytes) < rr)
                self.assertTrue(c_UUID(ll.bytes) < c_UUID(rr.bytes))
                self.assertTrue(ll < c_UUID(rr.bytes))

            if ll >= rr:
                self.assertTrue(c_UUID(ll.bytes) >= rr)
                self.assertTrue(c_UUID(ll.bytes) >= c_UUID(rr.bytes))
                self.assertTrue(ll >= c_UUID(rr.bytes))

            if ll <= rr:
                self.assertTrue(c_UUID(ll.bytes) <= rr)
                self.assertTrue(c_UUID(ll.bytes) <= c_UUID(rr.bytes))
                self.assertTrue(ll <= c_UUID(rr.bytes))

            if ll != rr:
                self.assertTrue(c_UUID(ll.bytes) != rr)
                self.assertTrue(c_UUID(ll.bytes) != c_UUID(rr.bytes))
                self.assertTrue(ll != c_UUID(rr.bytes))

            if ll == rr:
                self.assertTrue(c_UUID(ll.bytes) == rr)
                self.assertTrue(c_UUID(ll.bytes) == c_UUID(rr.bytes))
                self.assertTrue(ll == c_UUID(rr.bytes))

            self.assertTrue(c_UUID(ll.bytes) >= ll)
            self.assertTrue(c_UUID(ll.bytes) <= ll)

            self.assertTrue(c_UUID(ll.bytes) == ll)
            self.assertTrue(c_UUID(ll.bytes) == ll)
