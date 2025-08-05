from typing import Any
from collections.abc import MutableSequence
import unittest

from gel._internal._tracked_list import Mode
from gel._internal._qbmodel._abstract._link_set import LinkSet
from gel._internal._qbmodel._abstract import AbstractGelSourceModel

from gel import _testbase as tb


# A concrete LinkSet that accepts any object
class AnyList(LinkSet[object]):
    # XXX fix this class - should use BoxedInt or something

    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)


class BoxedInt(AbstractGelSourceModel):
    def __init__(self, value: int):
        self.value = value

    @classmethod
    def __gel_validate__(cls, value: Any):
        if not isinstance(value, int):
            return int(value)
        return value


class IntList(LinkSet[BoxedInt]):
    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)


# Helper class whose hashability can be toggled
class ToggleHash:
    def __init__(self) -> None:
        self._id: int | None = None

    def __hash__(self) -> int:
        if self._id is None:
            raise TypeError("unhashable")
        return self._id

    def __eq__(self, other) -> bool:
        if not isinstance(other, ToggleHash):
            return NotImplemented

        if self._id is None or other._id is None:
            return id(self) == id(other)
        else:
            return self._id == other._id

    def make_hashable(self, id_: int | None = None) -> None:
        if id_ is None:
            id_ = id(self)
        self._id = id_


class TestLinkSet(unittest.TestCase):
    # Core behaviors
    @tb.xfail
    def test_dlist_append_hashable_and_duplicates(self):
        lst = AnyList()
        lst.append(1)
        self.assertEqual(list(lst), [1])
        lst.append(1)
        self.assertEqual(list(lst), [1])

    def test_dlist_constructor_iterable_and_duplicates(self):
        lst = AnyList([1, 1, 2])
        self.assertEqual(list(lst), [1, 2])

    def test_dlist_len_and_contains(self):
        lst = AnyList([1, 2, 3])
        self.assertEqual(len(lst), 3)
        self.assertTrue(2 in lst)
        self.assertFalse(5 in lst)

    @tb.xfail
    def test_dlist_remove_and_pop(self):
        lst = AnyList([1, 2, 3])
        lst.remove(2)
        self.assertEqual(list(lst), [1, 3])
        with self.assertRaises(ValueError):
            lst.remove(2)
        self.assertEqual(lst.pop(), 3)
        self.assertEqual(lst.pop(0), 1)
        self.assertEqual(list(lst), [])

    @tb.xfail
    def test_dlist_clear_index_count(self):
        lst = AnyList([10, 20, 30])
        lst.clear()
        self.assertEqual(list(lst), [])
        lst = AnyList([1, 2, 3, 2])
        self.assertEqual(lst.index(3), 2)
        u = ToggleHash()
        lst2 = AnyList([u])
        self.assertTrue(u in lst2)
        self.assertEqual(lst2.count(u), 1)

    # Insertion, extension, slicing
    @tb.xfail
    def test_dlist_insert_hashable(self):
        lst = AnyList([1, 3])
        lst.insert(1, 2)
        self.assertEqual(list(lst), [1, 2, 3])
        lst.insert(10, 4)
        self.assertEqual(list(lst), [1, 2, 3, 4])
        lst.insert(-1, 5)
        self.assertEqual(list(lst), [1, 2, 3, 4, 5])
        # Inserting 5 again has no effect.
        lst.insert(-1, 5)
        self.assertEqual(list(lst), [1, 2, 3, 4, 5])

    @tb.xfail
    def test_dlist_insert_unhashable(self):
        lst = AnyList([1, 3])
        u = ToggleHash()
        u2 = ToggleHash()
        u3 = ToggleHash()
        lst.insert(1, u)
        self.assertEqual(list(lst), [1, u, 3])
        lst.insert(10, u2)
        self.assertEqual(list(lst), [1, u, 3, u2])
        lst.insert(-1, u3)
        self.assertEqual(list(lst), [1, u, 3, u2, u3])
        # Inserting u has no effect.
        lst.insert(-1, u)
        self.assertEqual(list(lst), [1, u, 3, u2, u3])

    @tb.xfail
    def test_dlist_extend_and_iadd_and_add(self):
        lst = AnyList([1])
        lst.extend([2, 3])
        self.assertEqual(list(lst), [1, 2, 3])
        new = lst + [3, 4]
        self.assertIsInstance(new, AnyList)
        self.assertEqual(list(new), [1, 2, 3, 4])
        lst += [4, 5]
        self.assertEqual(list(lst), [1, 2, 3, 4, 5])
        lst2 = AnyList([1, 2])
        lst2.extend(lst2)
        self.assertEqual(list(lst2), [1, 2])

    # Indexing and assignment
    @tb.xfail
    def test_dlist_getitem_and_slicing(self):
        lst = AnyList([1, 2, 3, 4])
        self.assertEqual(lst[0], 1)
        self.assertEqual(lst[-1], 4)
        sub = lst[1:3]
        self.assertIsInstance(sub, AnyList)
        self.assertEqual(list(sub), [2, 3])
        sub.append(5)
        self.assertTrue(5 in sub and 5 not in lst)

    @tb.xfail
    def test_dlist_setitem_index_hashable_and_duplicate(self):
        lst = AnyList([1, 2, 3])
        lst[1] = 5
        self.assertEqual(list(lst), [1, 5, 3])
        lst = AnyList([1, 2, 3])
        lst[0] = 3
        self.assertEqual(list(lst), [2, 3])

    @tb.xfail
    def test_dlist_setitem_index_unhashable(self):
        lst = AnyList([1])
        u = ToggleHash()
        lst[0] = u
        self.assertEqual(list(lst), [u])
        self.assertIn(id(u), lst._unhashables)

    @tb.xfail
    def test_dlist_setitem_slice_and_errors(self):
        lst = AnyList([1, 2, 3, 4])
        lst[1:3] = [7, 8]
        self.assertEqual(list(lst), [1, 7, 8, 4])
        with self.assertRaises(ValueError):
            lst[::2] = [9, 10]
        lst2 = AnyList([1, 2, 3])
        u1 = ToggleHash()
        lst2[1:2] = [u1, 4]
        self.assertEqual(list(lst2), [1, u1, 4, 3])
        self.assertIn(id(u1), lst2._unhashables)

    # Deletion via __delitem__
    @tb.xfail
    def test_dlist_del_index_and_slice(self):
        u = ToggleHash()
        u2 = ToggleHash()
        lst = AnyList([1, u, 3, u2])
        del lst[1]
        self.assertEqual(list(lst), [1, 3, u2])
        del lst[1:3]
        self.assertEqual(list(lst), [1])
        del lst[0]
        self.assertEqual(list(lst), [])

    # Equality, ordering, repr, hash
    @tb.xfail
    def test_dlist_eq_lt_repr_and_hash_of_list(self):
        a = AnyList([1, 2])
        b = AnyList([1, 2])
        c = AnyList([1, 3])
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertEqual(a, [1, 2])
        self.assertTrue(a < c)
        self.assertTrue(a < [1, 3])
        self.assertEqual(repr(a), "[1, 2]")
        with self.assertRaises(TypeError):
            hash(a)

    # MutableSequence registration
    @tb.xfail
    def test_dlist_mutable_sequence_registration(self):
        lst = AnyList()
        self.assertIsInstance(lst, MutableSequence)

    # Type enforcement errors
    @tb.xfail
    def test_dlist_type_enforcement(self):
        il = IntList()
        il.append(1)
        with self.assertRaises(ValueError):
            il.append("a")
        with self.assertRaises(ValueError):
            il.insert(0, "b")
        with self.assertRaises(ValueError):
            il[0] = "c"

    # index missing element
    @tb.xfail
    def test_dlist_index_missing(self):
        lst = AnyList([1, 2, 3])
        with self.assertRaises(ValueError):
            lst.index(5)

    # Additional tests for missing branches
    @tb.xfail
    def test_dlist_remove_unhashable(self):
        u = ToggleHash()
        lst = AnyList([u])
        # stored as unhashable
        self.assertIn(id(u), lst._unhashables)
        lst.remove(u)
        # removed from both items and unhashables
        self.assertNotIn(id(u), lst._unhashables)
        self.assertNotIn(u, lst)
        with self.assertRaises(ValueError):
            lst.remove(u)

    def test_dlist_pop_unhashable(self):
        u = ToggleHash()
        lst = AnyList([u])
        popped = lst.pop()
        self.assertIs(popped, u)
        self.assertEqual(list(lst), [])
        self.assertNotIn(id(u), lst._unhashables)

    @tb.xfail
    def test_dlist_count_unhashable(self):
        u1, u2 = ToggleHash(), ToggleHash()
        lst = AnyList([u1])
        # u1 present, u2 absent
        self.assertEqual(lst.count(u1), 1)
        self.assertEqual(lst.count(u2), 0)

    @tb.xfail
    def test_dlist_setitem_same_noop(self):
        lst = AnyList([1, 2, 3])
        lst[1] = 2
        # unchanged when assigning same value
        self.assertEqual(list(lst), [1, 2, 3])

    @tb.xfail
    def test_dlist_index_with_start_stop(self):
        lst = AnyList([1, 2, 3, 2, 1])
        # first 2 after index 1 but before 4
        self.assertEqual(lst.index(2, 1, 4), 1)
        # element outside slice bounds triggers ValueError
        with self.assertRaises(ValueError):
            lst.index(1, 1, 2)

    def test_dlist_eq_and_lt_with_other_type(self):
        lst = AnyList([1, 2])
        self.assertFalse(lst == 123)
        with self.assertRaises(TypeError):
            _ = lst < 123

    @tb.xfail
    def test_dlist_slice_insert_unhashable_and_hashable(self):
        # slice assignment with mixed types
        u = ToggleHash()
        lst = AnyList([1, 2, 3])
        lst[1:2] = [u, 2]
        self.assertEqual(list(lst), [1, u, 2, 3])
        self.assertIn(id(u), lst._unhashables)

    @tb.xfail
    def test_dlist_count_hashable(self):
        # count should return 1 for hashed duplicates
        lst = AnyList([1, 2, 2, 3])
        self.assertEqual(lst.count(2), 1)

    @tb.xfail
    def test_dlist_iadd_and_add_behaviors(self):
        # iadd skips duplicates, add creates new list
        lst = AnyList([1, 2])
        lst += [2, 3]
        self.assertEqual(list(lst), [1, 2, 3])
        new = lst + [3, 4]
        self.assertEqual(list(new), [1, 2, 3, 4])

    def test_dlist_wrap_list_reuse(self):
        x = [1, 2, 3, 4]
        lst = IntList(x, __wrap_list__=True)
        x.append(6)
        lst2 = list(lst)
        self.assertEqual(lst2, [1, 2, 3, 4, 6])

    @tb.xfail
    def test_dlist_wrap_list_deferred_validation(self):
        lst = IntList([1, 2, 3, 4], __wrap_list__=True)
        lst.append(1)
        self.assertEqual(lst, [1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
