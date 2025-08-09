from typing import Any
import unittest

from gel._internal._qbmodel._abstract import (
    AbstractGelModel,
)
from gel._internal._qbmodel._abstract._link_set import (
    LinkSet,
)
from gel._internal._tracked_list import Mode


# Some test models
class BoxedInt(AbstractGelModel):
    def __init__(self, value: int, *, __gel_new__: bool = True):
        self.value = value
        self.__gel_new__ = __gel_new__

    def __gel_not_abstract__(self) -> None:
        pass

    @classmethod
    def __gel_validate__(cls, value: Any):
        if not isinstance(value, int):
            return BoxedInt(int(value))
        return BoxedInt(value)


class BoxedStr(AbstractGelModel):
    def __init__(self, value: str, *, __gel_new__: bool = True):
        self.value = value
        self.__gel_new__ = __gel_new__

    def __gel_not_abstract__(self) -> None:
        pass

    @classmethod
    def __gel_validate__(cls, value: Any):
        return BoxedStr(str(value))


# A concrete LinkSet that only accepts BoxedInts
class BoxedIntList(LinkSet[BoxedInt]):
    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)


class TestLinkSet(unittest.TestCase):

    # Validation behavior
    def test_link_set_validate_constructor_01(self):
        # Validation from constructor
        lst = BoxedIntList([1, '2', BoxedInt(3)])
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_link_set_validate_constructor_02(self):
        # Validation from constructor with wrapped list
        # Validation is skipped
        nums = [1, 2, 3, 4]
        lst = BoxedIntList(nums, __wrap_list__=True)
        self.assertEqual(list(lst), [1, 2, 3, 4])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])

    def test_link_set_validate_constructor_03(self):
        # Validation from constructor raises error
        with self.assertRaises(TypeError):
            BoxedIntList([BoxedStr('1')])  # type: ignore

    def test_link_set_validate_add_01(self):
        # Validation from add
        lst = BoxedIntList()
        lst.add(1)  # type: ignore
        self.assertEqual([item.value for item in lst], [1])
        lst.add('2')  # type: ignore
        self.assertEqual([item.value for item in lst], [1, 2])
        lst.add(BoxedInt(3))
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_link_set_validate_add_02(self):
        # Validation from add with wrapped list
        # Validation is skipped
        nums = [1, 2, 3, 4]
        lst = BoxedIntList(nums, __wrap_list__=True)
        box_e = BoxedInt(5)
        lst.add(box_e)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e, 6])

    def test_link_set_validate_add_03(self):
        # Validation from add raises error
        int_lst = BoxedIntList()
        with self.assertRaises(TypeError):
            int_lst.add(BoxedStr('1'))  # type: ignore

    def test_link_set_validate_remove_01(self):
        # Validation from remove
        box_c = BoxedInt(3)
        lst = BoxedIntList([1, '2', box_c])
        with self.assertRaises(KeyError):
            # This will become a different BoxedInt instance
            lst.remove(1)  # type: ignore
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        with self.assertRaises(KeyError):
            # This will become a different BoxedInt instance
            lst.remove('2')  # type: ignore
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        lst.remove(box_c)
        self.assertEqual([item.value for item in lst], [1, 2])

    def test_link_set_validate_remove_02(self):
        # Validation from remove with wrapped list
        # Validation is skipped
        box_e = BoxedInt(5)
        nums = [1, 2, 3, 4, box_e]
        lst = BoxedIntList(nums, __wrap_list__=True)
        lst.remove(box_e)
        self.assertEqual(list(lst), [1, 2, 3, 4])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])

    def test_link_set_validate_remove_03(self):
        # Validation from remove raises error
        # Validation before key check
        int_lst = BoxedIntList()
        with self.assertRaises(TypeError):
            int_lst.remove(BoxedStr('1'))  # type: ignore

    def test_link_set_validate_discard_01(self):
        # Validation from discard
        box_c = BoxedInt(3)
        lst = BoxedIntList([1, '2', box_c])
        # This will become a different BoxedInt instance
        lst.discard(1)  # type: ignore
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        # This will become a different BoxedInt instance
        lst.discard('2')  # type: ignore
        self.assertEqual([item.value for item in lst], [1, 2, 3])
        lst.discard(box_c)
        self.assertEqual([item.value for item in lst], [1, 2])

    def test_link_set_validate_discard_02(self):
        # Validation from discard with wrapped list
        # Validation is skipped
        box_e = BoxedInt(5)
        nums = [1, 2, 3, 4, box_e]
        lst = BoxedIntList(nums, __wrap_list__=True)
        lst.discard(box_e)
        self.assertEqual(list(lst), [1, 2, 3, 4])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])

    def test_link_set_validate_discard_03(self):
        # Validation from discard raises error
        int_lst = BoxedIntList()
        with self.assertRaises(TypeError):
            int_lst.discard(BoxedStr('1'))  # type: ignore

    def test_link_set_validate_clear_02(self):
        # Validation from clear with wrapped list
        # Validation is skipped
        nums = [1, 2, 3, 4]
        lst = BoxedIntList(nums, __wrap_list__=True)
        lst.clear()
        self.assertEqual(list(lst), [])
        nums.append(6)
        self.assertEqual(list(lst), [6])

    def test_link_set_validate_update_01(self):
        # Validation from update
        lst = BoxedIntList()
        lst.update([1, '2', BoxedInt(3)])  # type: ignore
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_link_set_validate_update_02(self):
        # Validation from update with wrapped list
        # Validation is skipped
        nums = [1, 2, 3, 4]
        lst = BoxedIntList(nums, __wrap_list__=True)
        box_e = BoxedInt(5)
        lst.update([box_e])
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e, 6])

    def test_link_set_validate_update_03(self):
        # Validation from update raises error
        int_lst = BoxedIntList()
        with self.assertRaises(TypeError):
            int_lst.discard(BoxedStr('1'))  # type: ignore

    def test_link_set_validate_op_iadd_01(self):
        # Validation from operator iadd
        lst = BoxedIntList()
        lst += [1, '2', BoxedInt(3)]
        self.assertEqual([item.value for item in lst], [1, 2, 3])

    def test_link_set_validate_op_iadd_02(self):
        # Validation from operator iadd with wrapped list
        # Validation is skipped
        nums = [1, 2, 3, 4]
        lst = BoxedIntList(nums, __wrap_list__=True)
        box_e = BoxedInt(5)
        lst += [box_e]
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, box_e, 6])

    def test_link_set_validate_op_iadd_03(self):
        # Validation from iadd raises error
        int_lst = BoxedIntList()
        with self.assertRaises(TypeError):
            int_lst += [BoxedStr('1')]  # type: ignore

    def test_link_set_validate_op_isub_01(self):
        # Validation from operator isub
        box_c = BoxedInt(3)
        lst = BoxedIntList([BoxedInt(1), BoxedInt(2), box_c])
        lst -= [1, '2', box_c]
        self.assertEqual([item.value for item in lst], [1, 2])

    def test_link_set_validate_op_isub_02(self):
        # Wrap list stores a references
        # Operator isub validation is applied
        box_e = BoxedInt(5)
        nums = [1, 2, 3, 4, box_e]
        lst = BoxedIntList(nums, __wrap_list__=True)
        lst -= [box_e]
        self.assertEqual(list(lst), [1, 2, 3, 4])
        nums.append(6)
        self.assertEqual(list(lst), [1, 2, 3, 4, 6])

    def test_link_set_validate_op_isub_03(self):
        # Validation from isub raises error
        int_lst = BoxedIntList()
        with self.assertRaises(TypeError):
            int_lst -= [BoxedStr('1')]  # type: ignore


if __name__ == "__main__":
    unittest.main()
