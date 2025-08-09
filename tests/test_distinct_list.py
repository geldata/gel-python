from typing import Any, Optional
import unittest

from gel._internal._tracked_list import Mode
from gel._internal._qbmodel._abstract._link_set import LinkSet, _MT_co
from gel._internal._qbmodel._abstract import AbstractGelSourceModel

from gel import _testbase as tb


# A concrete LinkSet that accepts any model
class AnyList(LinkSet[AbstractGelSourceModel]):
    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)


class BoxedInt(AbstractGelSourceModel):
    def __init__(self, value: int, *, __gel_new__: bool = True):
        self.value = value
        self.__gel_new__ = __gel_new__

    @classmethod
    def __gel_validate__(cls, value: Any):
        if not isinstance(value, int):
            return BoxedInt(int(value))
        return BoxedInt(value)


class BoxedStr(AbstractGelSourceModel):
    def __init__(self, value: int, *, __gel_new__: bool = True):
        self.value = value
        self.__gel_new__ = __gel_new__

    @classmethod
    def __gel_validate__(cls, value: Any):
        return BoxedInt(str(value))


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
    def test_dlist_constructor_01(self):
        # Empty
        lst = AnyList()
        self.assertEqual(list(lst), [])

    def test_dlist_constructor_02(self):
        # With unique items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        lst = AnyList([box_a, box_b])
        self.assertEqual(list(lst), [box_a, box_b])

    def test_dlist_constructor_03(self):
        # With duplicates
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        lst = AnyList([box_a, box_a, box_b])
        self.assertEqual(list(lst), [box_a, box_b])

    def test_dlist_len_01(self):
        # Length counts the number of items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        lst = AnyList([box_a, box_b, box_c])
        self.assertEqual(len(lst), 3)

    def test_dlist_contains_01(self):
        # Contains correct for contained and not-contained
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(5)
        lst = AnyList([box_a, box_b, box_c, box_b])
        self.assertTrue(box_a in lst)
        self.assertTrue(box_b in lst)
        self.assertTrue(box_c in lst)
        self.assertFalse(box_d in lst)

    # Comparison behaviors
    def test_dlist_eq_01(self):
        # Compare to AbstractLinkSet
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        # Both in ReadWrite mode
        self.assertEqual(
            AnyList(),
            AnyList(),
        )
        self.assertEqual(
            AnyList([box_a, box_b]),
            AnyList([box_a, box_b]),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b]),
            AnyList([box_a, box_b, box_c]),
        )

        # Left in Write mode
        self.assertNotEqual(
            AnyList([], __mode__=Mode.Write),
            AnyList([]),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            AnyList([box_a, box_b]),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            AnyList([box_a, box_b, box_c]),
        )

        # Right in Write mode
        self.assertEqual(
            AnyList([]),
            AnyList([], __mode__=Mode.Write),
        )
        self.assertEqual(
            AnyList([box_a, box_b]),
            AnyList([box_a, box_b], __mode__=Mode.Write),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b]),
            AnyList([box_a, box_b, box_c], __mode__=Mode.Write),
        )

        # Both in Write mode
        self.assertNotEqual(
            AnyList([], __mode__=Mode.Write),
            AnyList([], __mode__=Mode.Write),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            AnyList([box_a, box_b], __mode__=Mode.Write),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            AnyList([box_a, box_b, box_c], __mode__=Mode.Write),
        )

    def test_dlist_eq_02(self):
        # Compare to set
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        # In ReadWrite mode
        self.assertEqual(
            AnyList(),
            set(),
        )
        self.assertEqual(
            AnyList([box_a, box_b]),
            {box_a, box_b},
        )
        self.assertEqual(
            AnyList([box_b, box_a]),
            {box_b, box_a},
        )
        self.assertNotEqual(
            AnyList([box_a, box_b]),
            {box_a, box_b, box_c},
        )

        # In Write mode
        self.assertNotEqual(
            AnyList([], __mode__=Mode.Write),
            set(),
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            {box_a, box_b},
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            {box_b, box_a},
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            {box_a, box_b, box_c},
        )

    def test_dlist_eq_03(self):
        # Compare to list
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        # In ReadWrite mode
        self.assertEqual(
            AnyList(),
            [],
        )
        self.assertEqual(
            AnyList([box_a, box_b]),
            [box_a, box_b],
        )
        self.assertEqual(
            AnyList([box_b, box_a]),
            [box_b, box_a],
        )
        self.assertNotEqual(
            AnyList([box_a, box_b]),
            [box_a, box_b, box_c],
        )

        # In Write mode
        self.assertNotEqual(
            AnyList([], __mode__=Mode.Write),
            [],
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            [box_a, box_b],
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            [box_b, box_a],
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            [box_a, box_b, box_c],
        )

    def test_dlist_eq_04(self):
        # Compare to something weird
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)

        # In ReadWrite mode
        self.assertNotEqual(
            AnyList(),
            None,
        )
        self.assertNotEqual(
            AnyList([box_a, box_b]),
            1,
        )
        self.assertNotEqual(
            AnyList([box_b, box_a]),
            [1, 2, 3],
        )

        # In Write mode
        self.assertNotEqual(
            AnyList(__mode__=Mode.Write),
            None,
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            1,
        )
        self.assertNotEqual(
            AnyList([box_a, box_b], __mode__=Mode.Write),
            [1, 2, 3],
        )

    # Modifying behaviors
    def _check_list[T](
        self,
        actual: LinkSet[T],
        expected: list[T],
        all_items: Optional[list[T]] = None,
    ):
        self.assertEqual(list(actual), expected)
        self.assertEqual(len(actual), len(expected))
        if all_items:
            for item in expected:
                self.assertEqual(item in actual, item in expected)
        else:
            for item in expected:
                self.assertTrue(item in actual)

    def test_dlist_add_01(self):
        # Add appends new items in order.
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList()
        lst.add(box_a)
        self._check_list(lst, [box_a], all_items)
        lst.add(box_b)
        self._check_list(lst, [box_a, box_b], all_items)
        lst.add(box_c)
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_add_02(self):
        # Add ignores duplicate, list order not changed
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.add(box_a)
        self._check_list(lst, [box_a, box_b, box_c], all_items)
        lst.add(box_b)
        self._check_list(lst, [box_a, box_b, box_c], all_items)
        lst.add(box_c)
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_remove_01(self):
        # Remove contained items until empty
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.remove(box_b)
        self._check_list(lst, [box_a, box_c], all_items)
        lst.remove(box_c)
        self._check_list(lst, [box_a], all_items)
        lst.remove(box_a)
        self._check_list(lst, [], all_items)

    def test_dlist_remove_02(self):
        # Remove non-contained item, error raised
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        with self.assertRaises(KeyError):
            lst.remove(box_d)
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_remove_03(self):
        # Remove already removed item, error raised
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.remove(box_b)
        self._check_list(lst, [box_a, box_c], all_items)
        with self.assertRaises(KeyError):
            lst.remove(box_b)
        self._check_list(lst, [box_a, box_c], all_items)

    def test_dlist_discard_01(self):
        # Discard contained items until empty
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.discard(box_b)
        self._check_list(lst, [box_a, box_c], all_items)
        lst.discard(box_c)
        self._check_list(lst, [box_a], all_items)
        lst.discard(box_a)
        self._check_list(lst, [], all_items)

    def test_dlist_discard_02(self):
        # Discard non-contained item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        lst.discard(box_d)
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_discard_03(self):
        # Discard already discarded item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.discard(box_b)
        self._check_list(lst, [box_a, box_c], all_items)
        lst.discard(box_b)
        self._check_list(lst, [box_a, box_c], all_items)

    def test_dlist_clear_01(self):
        # Clear empty
        lst = AnyList()
        lst.clear()
        self._check_list(lst, [])

    def test_dlist_clear_02(self):
        # Clear non-empty
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.clear()
        self._check_list(lst, [], all_items)

    def test_dlist_update_01(self):
        # Update nothing
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.update([])
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_update_02(self):
        # Update with new unique items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a])
        lst.update([box_b, box_c])
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_update_03(self):
        # Update with new duplicate items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        lst.update([box_d, box_d, box_d])
        self._check_list(lst, [box_a, box_b, box_c, box_d])

    def test_dlist_update_04(self):
        # Update with existing items, in different order
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst.update([box_c, box_b, box_a, box_b])
        self._check_list(lst, [box_a, box_b, box_c])

    def test_dlist_update_05(self):
        # Update with new and existing items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        lst.update([box_c, box_b, box_a, box_d])
        self._check_list(lst, [box_a, box_b, box_c, box_d])

    def test_dlist_operator_iadd_01(self):
        # Operator add nothing
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst += []
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_operator_iadd_02(self):
        # Operator iadd new unique items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a])
        lst += [box_b, box_c]
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_operator_iadd_03(self):
        # Operator iadd new duplicate items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        lst += [box_d, box_d, box_d]
        self._check_list(lst, [box_a, box_b, box_c, box_d])

    def test_dlist_operator_iadd_04(self):
        # Operator iadd existing items, in different order
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst += [box_c, box_b, box_a, box_b]
        self._check_list(lst, [box_a, box_b, box_c])

    def test_dlist_operator_iadd_05(self):
        # Operator iadd new and existing items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        lst += [box_c, box_b, box_a, box_d]
        self._check_list(lst, [box_a, box_b, box_c, box_d])

    def test_dlist_operator_isub_01(self):
        # Operator isub nothing
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst -= []
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_operator_isub_02(self):
        # Operator isub a single existing item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_b]
        self._check_list(lst, [box_a, box_c], all_items)

    def test_dlist_operator_isub_03(self):
        # Operator isub many existing items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_b, box_c]
        self._check_list(lst, [box_a], all_items)

    def test_dlist_operator_isub_04(self):
        # Operator isub existing items with duplicates
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_b, box_c, box_b]
        self._check_list(lst, [box_a], all_items)

    def test_dlist_operator_isub_05(self):
        # Operator isub non-content item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_d]
        self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_dlist_operator_isub_06(self):
        # Operator isub overlapping list
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        box_e = BoxedInt(5)
        all_items = [box_a, box_b, box_c, box_d, box_e]

        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_a, box_c, box_d, box_e]
        self._check_list(lst, [box_b], all_items)

    # Iteration behaviors
    def test_dlist_iter_01(self):
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        lst = AnyList([box_a, box_b, box_c])

        values = [
            item.value
            for item in lst
            if isinstance(item, BoxedInt)
        ]
        self.assertEqual(list(values), [1, 2, 3])

    # Validation behavior
    def test_dlist_validation_01(self):
        # Validation from constructor
        lst = IntList([1, '2', BoxedInt(3)])
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_dlist_validation_02(self):
        # Validation from add 
        lst = IntList()
        lst.add(1)
        self.assertEqual([item.value for item in lst], [1])
        lst.add('2')
        self.assertEqual([item.value for item in lst], [1, 2])
        lst.add(BoxedInt(3))
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_dlist_validation_03(self):
        # Validation from remove
        box_c = BoxedInt(3)
        lst = IntList([1, '2', box_c])
        with self.assertRaises(KeyError):
            lst.remove(1) # This will become a different BoxedInt instance
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        with self.assertRaises(KeyError):
            lst.remove('2') # This will become a different BoxedInt instance
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        lst.remove(box_c)
        self.assertEqual([item.value for item in lst], [1, 2])

    def test_dlist_validation_04(self):
        # Validation from discard
        box_c = BoxedInt(3)
        lst = IntList([1, '2', box_c])
        lst.discard(1) # This will become a different BoxedInt instance
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        lst.discard('2') # This will become a different BoxedInt instance
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        lst.discard(box_c)
        self.assertEqual([item.value for item in lst], [1, 2])

    def test_dlist_validation_06(self):
        # Validation from update 
        lst = IntList()
        lst.update([1, '2', BoxedInt(3)])
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_dlist_validation_07(self):
        # Validation from operator iadd 
        lst = IntList()
        lst += [1, '2', BoxedInt(3)]
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_dlist_validation_08(self):
        # Validation from operator isub
        box_c = BoxedInt(3)
        lst = IntList([BoxedInt(1), BoxedInt(2), box_c])
        lst -= [1, '2', box_c]
        self.assertEqual([item.value for item in lst], [1, 2])

    def test_dlist_validation_05(self):
        # Validation error
        box_a = BoxedInt(1)
        box_b = BoxedStr('2')
        int_lst = IntList([box_a])
        with self.assertRaises(TypeError):
            int_lst.add(box_b)

    def test_dlist_wrap_list_01(self):
        # Wrap list stores a reference
        # Constructor validation is skipped
        # Note that it's supposed to store BoxedInts!
        nums = [1, 2, 3, 4]
        lst = IntList(nums, __wrap_list__=True)
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])

    def test_dlist_wrap_list_02(self):
        # Wrap list stores a references
        # Add validation is applied
        nums = [1, 2, 3, 4]
        lst = IntList(nums, __wrap_list__=True)
        box_e = BoxedInt(5)
        lst.add(box_e)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e, 6])

    def test_dlist_wrap_list_03(self):
        # Wrap list stores a references
        # Remove validation is applied
        box_e = BoxedInt(5)
        nums = [1, 2, 3, 4, box_e]
        lst = IntList(nums, __wrap_list__=True)
        lst.remove(box_e)
        self.assertEqual(list(lst), [1, 2, 3, 4])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])

    def test_dlist_wrap_list_04(self):
        # Wrap list stores a references
        # Discard validation is applied
        box_e = BoxedInt(5)
        nums = [1, 2, 3, 4, box_e]
        lst = IntList(nums, __wrap_list__=True)
        lst.discard(box_e)
        self.assertEqual(list(lst), [1, 2, 3, 4])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])


    def test_dlist_wrap_list_05(self):
        # Wrap list stores a references
        # Discard validation is applied
        nums = [1, 2, 3, 4]
        lst = IntList(nums, __wrap_list__=True)
        lst.clear()
        self.assertEqual(list(lst), [])
        nums.append(6)
        self.assertEqual(list(lst), [6])

    def test_dlist_wrap_list_06(self):
        # Wrap list stores a references
        # Update validation is applied
        nums = [1, 2, 3, 4]
        lst = IntList(nums, __wrap_list__=True)
        box_e = BoxedInt(5)
        lst.update([box_e])
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e, 6])

    def test_dlist_wrap_list_07(self):
        # Wrap list stores a references
        # Operator iadd validation is applied
        nums = [1, 2, 3, 4]
        lst = IntList(nums, __wrap_list__=True)
        box_e = BoxedInt(5)
        lst += [box_e]
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e, 6])

    # Tracking behavior
    def test_dlist_track_changes_01(self):
        # Track changes after constructor
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        lst = AnyList()
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

        # With items
        lst = AnyList([box_a, box_b, box_c])
        self.assertEqual(list(lst.__gel_get_added__()), [box_a, box_b, box_c])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_dlist_track_changes_02(self):
        # Track changes after add
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        lst = AnyList([box_a, box_b])
        lst.add(box_c)
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_c])

    @tb.xfail
    def test_dlist_track_changes_03a(self):
        # Track changes after remove
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        # successful remove
        lst = AnyList([box_a, box_b, box_c])
        lst.remove(box_b)
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_b])

    @tb.xfail
    def test_dlist_track_changes_03b(self):
        # Track changes after remove
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(3)

        # failed remove
        lst = AnyList([box_a, box_b, box_c])
        with self.assertRaises(KeyError):
            lst.remove(box_d)
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_dlist_track_changes_04a(self):
        # Track changes after discard
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        # successful discard
        lst = AnyList([box_a, box_b, box_c])
        lst.discard(box_b)
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_b])

    @tb.xfail
    def test_dlist_track_changes_04b(self):
        # Track changes after remove
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(3)

        # failed discard
        lst = AnyList([box_a, box_b, box_c])
        lst.discard(box_d)
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_dlist_track_changes_05(self):
        # Track changes after clear
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        lst = AnyList([box_a, box_b, box_c])
        lst.clear()
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_a, box_b, box_c])

    @tb.xfail
    def test_dlist_track_changes_06a(self):
        # Track changes after update
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        # No changes
        lst = AnyList([box_a, box_b])
        lst.update([box_a, box_b])
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_dlist_track_changes_06b(self):
        # Track changes after update
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        # Items added
        lst = AnyList([box_a, box_b])
        lst.update([box_b, box_c, box_d])
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_c, box_d])

    @tb.xfail
    def test_dlist_track_changes_07a(self):
        # Track changes after operator iadd
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        # No changes
        lst = AnyList([box_a, box_b])
        lst += [box_a, box_b]
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_dlist_track_changes_07b(self):
        # Track changes after operator iadd
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        # Items added
        lst = AnyList([box_a, box_b])
        lst += [box_b, box_c, box_d]
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_c, box_d])

    @tb.xfail
    def test_dlist_track_changes_08a(self):
        # Track changes after operator isub
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        # No changes
        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_d]
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_dlist_track_changes_08b(self):
        # Track changes after operator isub
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        # Items removed
        lst = AnyList([box_a, box_b, box_c])
        lst -= [box_b, box_c, box_d]
        self.assertEqual(list(lst.__gel_get_added__()), [])
        self.assertEqual(list(lst.__gel_get_removed__()), [box_b, box_c])

    def test_dlist_commit_01(self):
        # New items are only added to the _tracking_set after commiting
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        lst = AnyList([box_a, box_b, box_c, box_d])
        self.assertEqual(lst._tracking_set, {
            box_a: box_a,
            box_b: box_b,
        })
        lst.__gel_commit__()
        self.assertEqual(lst._tracking_set, {
            box_a: box_a,
            box_b: box_b,
            box_c: box_c,
            box_d: box_d,
        })


if __name__ == "__main__":
    unittest.main()
