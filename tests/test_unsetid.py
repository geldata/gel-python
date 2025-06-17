import unittest

import copy
import uuid
import pickle

from gel._internal._unsetid import UNSET_UUID, _UnsetUUID


class TestUnsetID(unittest.TestCase):
    def test_unsetid_singleton(self):
        i1 = _UnsetUUID()
        self.assertIs(UNSET_UUID, i1)

    def test_unsetid_hash(self):
        with self.assertRaisesRegex(TypeError, "UNSET_UUID is unhashable"):
            hash(UNSET_UUID)

    def test_unsetid_eq(self):
        u = uuid.uuid4()
        self.assertNotEqual(UNSET_UUID, u)
        self.assertEqual(UNSET_UUID, UNSET_UUID)
        self.assertNotEqual(UNSET_UUID, None)
        self.assertNotEqual(UNSET_UUID, 123)

    def test_unsetid_copy(self):
        self.assertIs(copy.copy(UNSET_UUID), UNSET_UUID)
        self.assertIs(copy.deepcopy(UNSET_UUID), UNSET_UUID)

    def test_unsetid_pickle(self):
        p = pickle.dumps(UNSET_UUID)
        unp = pickle.loads(p)
        self.assertIs(UNSET_UUID, unp)
