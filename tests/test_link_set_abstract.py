from typing import Any, Iterable, Optional, Sequence, TypeVar
import unittest

from gel._internal._tracked_list import Mode
from gel._internal._qbmodel._abstract._link_set import (
    AbstractLinkSet,
    AbstractMutableLinkSet,
)
from gel._internal._qbmodel._abstract import AbstractGelModel

from gel import _testbase as tb


_T_test = TypeVar("_T_test", bound="AbstractGelModel", covariant=True)


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


class DummyAbstractLinkSet(AbstractLinkSet[AbstractGelModel]):
    type = AbstractGelModel

    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)

    @staticmethod
    def _pyid(item: AbstractGelModel) -> int:
        return id(item)

    def __gel_add__(self, value: AbstractGelModel) -> None:
        self._init_tracking()
        if self._is_tracked(value):
            return
        self._track_item(value)
        self._items.append(value)

    def __gel_extend__(self, it: Iterable[AbstractGelModel]):
        for v in it:
            self.__gel_add__(v)


class DummyAbstractMutableLinkSet(AbstractMutableLinkSet[AbstractGelModel]):
    type = AbstractGelModel

    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)

    @staticmethod
    def _pyid(item: AbstractGelModel) -> int:
        return id(item)

    def __gel_add__(
        self, value: AbstractGelModel
    ) -> None:
        self._ensure_snapshot_then_track(value)

    def __gel_remove__(
        self, value: AbstractGelModel
    ) -> AbstractGelModel | None:
        return self._ensure_snapshot_then_untrack(value)

    def __gel_extend__(self, it: Iterable[AbstractGelModel]):
        for v in it:
            self.__gel_add__(v)


def _get_single_permutations() -> list[tuple[Mode, bool]]:
    return [
        (mode, wrap)
        for mode in (Mode.ReadWrite, Mode.Write)
        for wrap in (
            (True, False) if mode == Mode.ReadWrite else (False,)
        )
    ]


def _get_double_permutations() -> list[tuple[Mode, bool, Mode, bool]]:
    return [
        (mode_left, wrap_left, mode_right, wrap_right)
        for mode_left in (Mode.ReadWrite, Mode.Write)
        for wrap_left in (
            (True, False) if mode_left == Mode.ReadWrite else (False,)
        )
        for mode_right in (Mode.ReadWrite, Mode.Write)
        for wrap_right in (
            (True, False) if mode_right == Mode.ReadWrite else (False,)
        )
    ]


class TestAbstractLinkSet(unittest.TestCase):

    # Core behaviors
    def test_abstract_link_set_constructor_01(self):
        # Empty
        lst = DummyAbstractLinkSet()
        self.assertEqual(list(lst), [])
        self.assertEqual(lst._mode, Mode.ReadWrite)

        lst = DummyAbstractLinkSet(__mode__=Mode.Write)
        self.assertEqual(list(lst), [])
        self.assertEqual(lst._mode, Mode.Write)

        with self.assertRaises(ValueError):
            lst = DummyAbstractLinkSet(
                __wrap_list__=True,
            )

    def test_abstract_link_set_constructor_02(self):
        # With unique items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)

        items = [box_a, box_b]

        lst = DummyAbstractLinkSet(
            items,
            __mode__=Mode.ReadWrite,
            __wrap_list__=False,
        )
        self.assertEqual(list(lst), [box_a, box_b])

        lst = DummyAbstractLinkSet(
            items,
            __mode__=Mode.ReadWrite,
            __wrap_list__=True,
        )
        self.assertEqual(list(lst), [box_a, box_b])

        lst = DummyAbstractLinkSet(
            items,
            __mode__=Mode.Write,
            __wrap_list__=False,
        )
        self.assertEqual(list(lst), [box_a, box_b])

    def test_abstract_link_set_constructor_03(self):
        # With duplicates
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)

        items = [box_a, box_a, box_b]

        lst = DummyAbstractLinkSet(
            items,
            __mode__=Mode.ReadWrite,
            __wrap_list__=False,
        )
        self.assertEqual(list(lst), [box_a, box_b])

        lst = DummyAbstractLinkSet(
            items,
            __mode__=Mode.ReadWrite,
            __wrap_list__=True,
        )
        self.assertEqual(list(lst), [box_a, box_a, box_b])

        lst = DummyAbstractLinkSet(
            items,
            __mode__=Mode.Write,
            __wrap_list__=False,
        )
        self.assertEqual(list(lst), [box_a, box_b])

    def test_abstract_link_set_len_01(self):
        # Length counts the number of items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            self.assertEqual(len(lst), 3)

    def test_abstract_link_set_contains_01(self):
        # Contains correct for contained and not-contained
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for wrap in (False, True):
            lst = DummyAbstractLinkSet(
                [box_a, box_b, box_d],
                __mode__=Mode.ReadWrite,
                __wrap_list__=wrap,
            )
            self.assertTrue(box_a in lst)
            self.assertTrue(box_b in lst)
            self.assertFalse(box_c in lst)
            self.assertTrue(box_d in lst)
            self.assertFalse(box_e in lst)

        # In not allowed in write mode
        lst = DummyAbstractLinkSet(
            [box_a, box_b, box_d],
            __mode__=Mode.Write,
        )
        with self.assertRaises(RuntimeError) as cm:
            self.assertTrue(box_a in lst)
        self.assertIn(
            "Cannot use `in` operator on the collection in write-only mode.",
            str(cm.exception),
        )

    def test_abstract_link_set_contains_02(self):
        # Contains returns false for wrong type
        lst = DummyAbstractLinkSet()
        self.assertFalse('wrong type' in lst)

    # Comparison behaviors
    def test_abstract_link_set_eq_01(self):
        # Compare to AbstractLinkSet
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for mode_left, wrap_left, mode_right, wrap_right in (
            _get_double_permutations()
        ):
            # Both empty
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                mode_left == Mode.ReadWrite,
            )

            # Equal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                mode_left == Mode.ReadWrite,
            )

            # Unequal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_a, box_b, box_c],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                False,
            )

            # Equal contents with only new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left and wrap_right,
            )

            # Equal contents with mixed new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left and wrap_right,
            )

    def test_abstract_link_set_eq_02(self):
        # Compare to AbstractMutableLinkSet
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for mode_left, wrap_left, mode_right, wrap_right in (
            _get_double_permutations()
        ):
            # Both empty
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Write DummyAbstractLinkSet will compare true with
                # ReadWrite DummyAbstractMutableLinkSet
                mode_right == Mode.ReadWrite,
            )

            # Equal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_a, box_b],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Write DummyAbstractLinkSet will compare true with
                # ReadWrite DummyAbstractMutableLinkSet
                mode_right == Mode.ReadWrite,
            )

            # Unequal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_a, box_b, box_c],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                False,
            )

            # Equal contents with only new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left and wrap_right,
            )

            # Equal contents with mixed new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left and wrap_right,
            )

    def test_abstract_link_set_eq_03(self):
        # Compare to set
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for mode_left, wrap_left in _get_single_permutations():
            # Both empty
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == set()
                ),
                mode_left == Mode.ReadWrite,
            )

            # Equal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == {box_a, box_b}
                ),
                # Write DummyAbstractLinkSet will compare true with
                # ReadWrite DummyAbstractMutableLinkSet
                mode_left == Mode.ReadWrite,
            )

            # Unequal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == {box_a, box_b, box_c}
                ),
                False,
            )

            # Equal contents with only new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == {box_d, box_e}
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left,
            )

            # Equal contents with mixed new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == {box_a, box_b, box_c, box_d, box_e}
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left,
            )

    def test_abstract_link_set_eq_04(self):
        # Compare to list
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for mode_left, wrap_left in _get_single_permutations():
            # Both empty
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == []
                ),
                mode_left == Mode.ReadWrite,
            )

            # Equal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == [box_a, box_b]
                ),
                # Write DummyAbstractLinkSet will compare true with
                # ReadWrite DummyAbstractMutableLinkSet
                mode_left == Mode.ReadWrite,
            )

            # Unequal contents
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == [box_a, box_b, box_c]
                ),
                False,
            )

            # Equal contents with only new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == [box_d, box_e]
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                mode_left != Mode.Write,
            )

            # Equal contents with mixed new items
            self.assertEqual(
                (
                    DummyAbstractLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == [box_a, box_b, box_c, box_d, box_e]
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                mode_left != Mode.Write,
            )

    def test_abstract_link_set_eq_05(self):
        # Compare to something weird
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)

        for mode_left, wrap_left in _get_single_permutations():
            self.assertNotEqual(
                DummyAbstractLinkSet(
                    [],
                    __mode__=mode_left,
                    __wrap_list__=wrap_left,
                ),
                None,
            )
            self.assertNotEqual(
                DummyAbstractLinkSet(
                    [box_a, box_b],
                    __mode__=mode_left,
                    __wrap_list__=wrap_left,
                ),
                1,
            )
            self.assertNotEqual(
                DummyAbstractLinkSet(
                    [box_b, box_a],
                    __mode__=mode_left,
                    __wrap_list__=wrap_left,
                ),
                [1, 2, 3],
            )

    # Iteration behaviors
    def test_abstract_link_set_iter_01(self):
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode_left, wrap_left in _get_single_permutations():
            lst = DummyAbstractLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode_left,
                __wrap_list__=wrap_left,
            )

            values = [
                item.value
                for item in lst
                if isinstance(item, BoxedInt)
            ]
            self.assertEqual(list(values), [1, 2, 3])


class TestAbstractMutableLinkSet(unittest.TestCase):

    # Comparison behaviors
    def test_abstract_mutable_link_set_eq_01(self):
        # Compare to AbstractLinkSet
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for mode_left, wrap_left, mode_right, wrap_right in (
            _get_double_permutations()
        ):
            # Both empty
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                mode_left == Mode.ReadWrite,
            )

            # Equal contents
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_a, box_b],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                mode_left == Mode.ReadWrite,
            )

            # Unequal contents
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_a, box_b, box_c],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                False,
            )

            # Equal contents with only new items
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left and wrap_right,
            )

            # Equal contents with mixed new items
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                # Wrapped arrays do not init tracking when adding items in
                # constructor.
                wrap_left and wrap_right,
            )

    def test_abstract_mutable_link_set_eq_02(self):
        # Compare to AbstractMutableLinkSet
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3, __gel_new__=False)
        box_d = BoxedInt(4, __gel_new__=True)
        box_e = BoxedInt(5, __gel_new__=True)

        for mode_left, wrap_left, mode_right, wrap_right in (
            _get_double_permutations()
        ):
            # Both empty
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                mode_left == Mode.ReadWrite and mode_right == Mode.ReadWrite,
            )

            # Equal contents
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_a, box_b],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                mode_left == Mode.ReadWrite and mode_right == Mode.ReadWrite,
            )

            # Unequal contents
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_a, box_b],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_a, box_b, box_c],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                False,
            )

            # Equal contents with only new items
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                (
                    # Wrapped arrays do not init tracking when adding items in
                    # constructor.
                    wrap_left == wrap_right
                    and mode_left == Mode.ReadWrite
                    and mode_right == Mode.ReadWrite
                ),
            )

            # Equal contents with mixed new items
            self.assertEqual(
                (
                    DummyAbstractMutableLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_left,
                        __wrap_list__=wrap_left,
                    )
                    == DummyAbstractMutableLinkSet(
                        [box_a, box_b, box_c, box_d, box_e],
                        __mode__=mode_right,
                        __wrap_list__=wrap_right,
                    )
                ),
                (
                    # Wrapped arrays do not init tracking when adding items in
                    # constructor.
                    wrap_left == wrap_right
                    and mode_left == Mode.ReadWrite
                    and mode_right == Mode.ReadWrite
                ),
            )

    # Modifying behaviors
    def _check_list(
        self,
        actual: AbstractLinkSet[_T_test],
        expected: list[_T_test],
        all_items: Optional[Sequence[_T_test]] = None,
    ):
        if actual._mode == Mode.ReadWrite:
            self.assertEqual(list(actual), expected)
            self.assertEqual(len(actual), len(expected))

            if all_items:
                for item in expected:
                    self.assertEqual(item in actual, item in expected)
            else:
                for item in expected:
                    self.assertTrue(item in actual)

        self.assertEqual(list(actual.unsafe_iter()), expected)
        self.assertEqual(actual.unsafe_len(), len(expected))

    def test_abstract_mutable_link_set_add_01(self):
        # Add appends new items in order.
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.add(box_a)
            self._check_list(lst, [box_a], all_items)
            lst.add(box_b)
            self._check_list(lst, [box_a, box_b], all_items)
            lst.add(box_c)
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_add_02(self):
        # Add ignores duplicate, list order not changed
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.add(box_a)
            self._check_list(lst, [box_a, box_b, box_c], all_items)
            lst.add(box_b)
            self._check_list(lst, [box_a, box_b, box_c], all_items)
            lst.add(box_c)
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_remove_01(self):
        # Remove contained items until empty
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.remove(box_b)
            self._check_list(lst, [box_a, box_c], all_items)
            lst.remove(box_c)
            self._check_list(lst, [box_a], all_items)
            lst.remove(box_a)
            self._check_list(lst, [], all_items)

    def test_abstract_mutable_link_set_remove_02(self):
        # Remove non-contained item, error raised
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            with self.assertRaises(KeyError):
                lst.remove(box_d)
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_remove_03(self):
        # Remove already removed item, error raised
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.remove(box_b)
            self._check_list(lst, [box_a, box_c], all_items)
            with self.assertRaises(KeyError):
                lst.remove(box_b)
            self._check_list(lst, [box_a, box_c], all_items)

    def test_abstract_mutable_link_set_discard_01(self):
        # Discard contained items until empty
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.discard(box_b)
            self._check_list(lst, [box_a, box_c], all_items)
            lst.discard(box_c)
            self._check_list(lst, [box_a], all_items)
            lst.discard(box_a)
            self._check_list(lst, [], all_items)

    def test_abstract_mutable_link_set_discard_02(self):
        # Discard non-contained item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.discard(box_d)
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_discard_03(self):
        # Discard already discarded item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.discard(box_b)
            self._check_list(lst, [box_a, box_c], all_items)
            lst.discard(box_b)
            self._check_list(lst, [box_a, box_c], all_items)

    def test_abstract_mutable_link_set_clear_01(self):
        # Clear empty
        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.clear()
            self._check_list(lst, [])

    def test_abstract_mutable_link_set_clear_02(self):
        # Clear non-empty
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.clear()
            self._check_list(lst, [], all_items)

    def test_abstract_mutable_link_set_update_01(self):
        # Update nothing
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([])
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_update_02(self):
        # Update with new unique items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([box_b, box_c])
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_update_03(self):
        # Update with new duplicate items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([box_d, box_d, box_d])
            self._check_list(lst, [box_a, box_b, box_c, box_d], all_items)

    def test_abstract_mutable_link_set_update_04(self):
        # Update with existing items, in different order
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([box_c, box_b, box_a, box_b])
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_update_05(self):
        # Update with new and existing items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([box_c, box_b, box_a, box_d])
            self._check_list(lst, [box_a, box_b, box_c, box_d], all_items)

    def test_abstract_mutable_link_set_operator_iadd_01(self):
        # Operator add nothing
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += []
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_operator_iadd_02(self):
        # Operator iadd new unique items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [box_b, box_c]
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_operator_iadd_03(self):
        # Operator iadd new duplicate items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [box_d, box_d, box_d]
            self._check_list(lst, [box_a, box_b, box_c, box_d], all_items)

    def test_abstract_mutable_link_set_operator_iadd_04(self):
        # Operator iadd existing items, in different order
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [box_c, box_b, box_a, box_b]
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_operator_iadd_05(self):
        # Operator iadd new and existing items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [box_c, box_b, box_a, box_d]
            self._check_list(lst, [box_a, box_b, box_c, box_d], all_items)

    def test_abstract_mutable_link_set_operator_isub_01(self):
        # Operator isub nothing
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= []
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_operator_isub_02(self):
        # Operator isub a single existing item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_b]
            self._check_list(lst, [box_a, box_c], all_items)

    def test_abstract_mutable_link_set_operator_isub_03(self):
        # Operator isub many existing items
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_b, box_c]
            self._check_list(lst, [box_a], all_items)

    def test_abstract_mutable_link_set_operator_isub_04(self):
        # Operator isub existing items with duplicates
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        all_items = [box_a, box_b, box_c]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_b, box_c, box_b]
            self._check_list(lst, [box_a], all_items)

    def test_abstract_mutable_link_set_operator_isub_05(self):
        # Operator isub non-content item
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        all_items = [box_a, box_b, box_c, box_d]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_d]
            self._check_list(lst, [box_a, box_b, box_c], all_items)

    def test_abstract_mutable_link_set_operator_isub_06(self):
        # Operator isub overlapping list
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)
        box_e = BoxedInt(5)
        all_items = [box_a, box_b, box_c, box_d, box_e]

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_a, box_c, box_d, box_e]
            self._check_list(lst, [box_b], all_items)

    # Tracking behavior
    def test_abstract_mutable_link_set_track_changes_01(self):
        # Track changes after constructor
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [])

            # With items
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            if wrap:
                self.assertEqual(list(lst.__gel_get_added__()), [])
                self.assertEqual(list(lst.__gel_get_removed__()), [])
            else:
                self.assertEqual(
                    list(lst.__gel_get_added__()),
                    [box_a, box_b, box_c],
                )
                self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_02(self):
        # Track changes after add
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.add(box_c)
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [box_c])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_03a(self):
        # Track changes after remove
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            # successful remove
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.remove(box_b)
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [box_b])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_03b(self):
        # Track changes after remove
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            # failed remove
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            with self.assertRaises(KeyError):
                lst.remove(box_d)
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_04a(self):
        # Track changes after discard
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            # successful discard
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.discard(box_b)
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [box_b])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_04b(self):
        # Track changes after remove
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            # failed discard
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.discard(box_d)
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_05(self):
        # Track changes after clear
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.clear()
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(
                list(lst.__gel_get_removed__()),
                [box_a, box_b, box_c],
            )

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_06a(self):
        # Track changes after update
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)

        for mode, wrap in _get_single_permutations():
            # No changes
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([box_a, box_b])
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_06b(self):
        # Track changes after update
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        for mode, wrap in _get_single_permutations():
            # Items added
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([box_b, box_c, box_d])
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [box_c, box_d])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_07a(self):
        # Track changes after operator iadd
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)

        for mode, wrap in _get_single_permutations():
            # No changes
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [box_a, box_b]
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_07b(self):
        # Track changes after operator iadd
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        for mode, wrap in _get_single_permutations():
            # Items added
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [box_b, box_c, box_d]
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [box_c, box_d])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_08a(self):
        # Track changes after operator isub
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        for mode, wrap in _get_single_permutations():
            # No changes
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_d]
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [])

    @tb.xfail
    def test_abstract_mutable_link_set_track_changes_08b(self):
        # Track changes after operator isub
        box_a = BoxedInt(1)
        box_b = BoxedInt(2)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        for mode, wrap in _get_single_permutations():
            # Items removed
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [box_b, box_c, box_d]
            self.assertEqual(list(lst.__gel_get_added__()), [])
            self.assertEqual(list(lst.__gel_get_removed__()), [box_b, box_c])

    def test_abstract_mutable_link_set_commit_01(self):
        # New items are only added to the _tracking_set after commiting
        box_a = BoxedInt(1, __gel_new__=False)
        box_b = BoxedInt(2, __gel_new__=False)
        box_c = BoxedInt(3)
        box_d = BoxedInt(4)

        for mode, wrap in _get_single_permutations():
            lst = DummyAbstractMutableLinkSet(
                [box_a, box_b, box_c, box_d],
                __mode__=mode,
                __wrap_list__=wrap,
            )

            if wrap:
                self.assertIsNone(lst._tracking_set)
            else:
                self.assertEqual(lst._tracking_set, {
                    box_a: box_a,
                    box_b: box_b,
                })

            lst.__gel_commit__()

            if wrap:
                self.assertIsNone(lst._tracking_set)
            else:
                self.assertEqual(lst._tracking_set, {
                    box_a: box_a,
                    box_b: box_b,
                    box_c: box_c,
                    box_d: box_d,
                })


if __name__ == "__main__":
    unittest.main()
