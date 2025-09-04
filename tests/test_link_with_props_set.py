from __future__ import annotations
import dataclasses
from typing import Any
import unittest

from gel._internal import _qb
from gel._internal._edgeql._schema import PointerKind, Cardinality
from gel._internal._schemapath import SchemaPath
from gel._internal._qbmodel import _abstract
from gel._internal._qbmodel._abstract import (
    AbstractGelLinkModel,
    AbstractGelModel,
    AbstractGelProxyModel,
)
from gel._internal._qbmodel._abstract._link_set import (
    LinkSet,
    LinkWithPropsSet,
)
from gel._internal._qbmodel._abstract._descriptors import (
    GelLinkModelDescriptor,
)
from gel._internal._tracked_list import Mode


# Testing behavior of LinkWithPropsSet without reference to pydantic

class DummyInt(AbstractGelModel):
    def __init__(self, value: int, *, __gel_new__: bool = True):
        self.value = value
        self.__gel_new__ = __gel_new__

    def __gel_not_abstract__(self) -> None:
        pass

    @classmethod
    def __gel_validate__(cls, value: Any):
        if not isinstance(value, int):
            return DummyInt(int(value))
        return DummyInt(value)


class DummyIntList(LinkSet[DummyInt]):
    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)


class DummyLinkModel(AbstractGelLinkModel):
    class __gel_reflection__(AbstractGelLinkModel.__gel_reflection__):  # noqa: N801
        pointers = {
            "comment": _qb.GelPointerReflection(
                name="comment",
                type=SchemaPath("std", "str"),
                typexpr="std::str",
                kind=PointerKind.Property,
                cardinality=Cardinality.AtMostOne,
                computed=False,
                readonly=False,
                has_default=False,
                properties=None,
                mutable=True,
            ),
        }

    __gel_has_mutable_props__ = True

    def __init__(self, comment: str | None = None):
        self.comment = comment

    @classmethod
    def __gel_model_construct__(
        cls, __dict__: dict[str, Any] | None
    ) -> DummyLinkModel:
        self = object.__new__(cls)
        if __dict__ is None:
            __dict__ = {}
        if 'comment' not in __dict__:
            __dict__['comment'] = None
        object.__setattr__(self, "__dict__", __dict__)
        return self


class DummyLinkDescriptor(GelLinkModelDescriptor[DummyLinkModel]):
    pass


class DummyProxyModel(AbstractGelProxyModel[DummyInt, DummyLinkModel]):

    __proxy_of__ = DummyInt
    __linkprops__ = DummyLinkDescriptor()

    def __init__(
        self,
        obj: DummyInt,
        link_props: DummyLinkModel,
        linked: bool,
        *,
        __gel_new__: bool = True,
    ):
        object.__setattr__(self, "_p__obj__", obj)
        object.__setattr__(self, "__linkprops__", link_props)
        object.__setattr__(self, "__gel_linked__", linked)
        self.__gel_new__ = __gel_new__

    def __gel_not_abstract__(self) -> None:
        pass

    @classmethod
    def __gel_model_construct__(
        cls, __dict__: dict[str, Any] | None
    ) -> DummyProxyModel:
        self = object.__new__(cls)
        if __dict__ is None:
            __dict__ = {"__gel_new__": True}
        object.__setattr__(self, "__dict__", __dict__)
        return self

    @classmethod
    def __gel_proxy_construct__(
        cls,
        obj: DummyInt,
        lprops: dict[str, Any] | DummyLinkModel,
        *,
        linked: bool = False,
    ) -> AbstractGelProxyModel[DummyInt, DummyLinkModel]:
        pnv = cls.__gel_model_construct__(None)
        object.__setattr__(pnv, "_p__obj__", obj)

        if type(lprops) is dict:
            lp_obj = cls.__linkprops__.__gel_model_construct__(lprops)
        else:
            lp_obj = lprops

        object.__setattr__(pnv, "__linkprops__", lp_obj)
        object.__setattr__(pnv, "__gel_linked__", linked)
        return pnv

    def without_linkprops(self) -> DummyInt:
        return object.__getattribute__(self, "_p__obj__")

    def __gel_merge_other_proxy__(
        self,
        other: AbstractGelProxyModel[DummyInt, DummyLinkModel],
    ) -> None:
        cls = type(self)
        assert type(other) is cls

        if not _abstract.is_proxy_linked(other):
            # `other` isn't link, so it must be a new proxy created
            # by instantiating the model or calling the `ProxyModel.link()`
            # classmethod.
            lp = _abstract.copy_or_ref_lprops(other.__linkprops__)
        else:
            lp = cls.__linkprops__.__gel_model_construct__({})

        object.__setattr__(self, "__linkprops__", lp)

    def __gel_replace_wrapped_model__(
        self,
        new: DummyInt,
    ) -> None:
        object.__setattr__(self, "_p__obj__", new)

    def __gel_replace_linkprops__(
        self,
        new: DummyLinkModel,
    ) -> None:
        object.__setattr__(self, "__linkprops__", new)


class DummyIntWithPropList(LinkWithPropsSet[DummyProxyModel, DummyInt]):
    def __init__(self, *args, **kwargs) -> None:
        if "__mode__" not in kwargs:
            super().__init__(*args, __mode__=Mode.ReadWrite, **kwargs)
        else:
            super().__init__(*args, **kwargs)


@dataclasses.dataclass(frozen=True, kw_only=True)
class LinkWithPropsSetEntry:
    """Inputs to add to DummyIntWithPropList during tests and expected test
    outputs.
    """
    input: DummyInt | DummyProxyModel
    expected_value: int
    expected_comment: str | None
    expected_linked: bool

    def model(self) -> DummyInt:
        return (
            self.input
            if isinstance(self.input, DummyInt) else
            self.input.without_linkprops()
        )

    @staticmethod
    def get_basic_entries() -> list["LinkWithPropsSetEntry"]:
        """A set of entries which cover a variety of permutations of things
        to add to a LinkWithPropSet.
        """
        model_a = DummyInt(1, __gel_new__=True)
        proxy_a = DummyProxyModel(
            model_a,
            DummyLinkModel('A'),
            True,
        )
        model_b = DummyInt(2, __gel_new__=False)
        proxy_b = DummyProxyModel(
            model_b,
            DummyLinkModel('B'),
            True,
        )
        model_c = DummyInt(3, __gel_new__=True)
        proxy_c = DummyProxyModel(
            model_c,
            DummyLinkModel('C'),
            False,
        )
        model_d = DummyInt(4, __gel_new__=False)
        proxy_d = DummyProxyModel(
            model_d,
            DummyLinkModel('D'),
            False,
        )
        model_e = DummyInt(5, __gel_new__=True)
        proxy_e = DummyProxyModel(
            model_e,
            DummyLinkModel(),
            False,
        )
        model_f = DummyInt(6, __gel_new__=False)
        proxy_f = DummyProxyModel(
            model_f,
            DummyLinkModel(),
            False,
        )

        model_g = DummyInt(7, __gel_new__=True)
        model_h = DummyInt(8, __gel_new__=False)

        return [
            LinkWithPropsSetEntry(
                input=proxy_a,
                expected_value=1,
                expected_comment=None,
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_b,
                expected_value=2,
                expected_comment=None,
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_c,
                expected_value=3,
                expected_comment='C',
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_d,
                expected_value=4,
                expected_comment='D',
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_e,
                expected_value=5,
                expected_comment=None,
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_f,
                expected_value=6,
                expected_comment=None,
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=model_g,
                expected_value=7,
                expected_comment=None,
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=model_h,
                expected_value=8,
                expected_comment=None,
                expected_linked=True,
            ),
        ]

    @staticmethod
    def get_proxied_entries(
        entries: list["LinkWithPropsSetEntry"],
    ) -> list["LinkWithPropsSetEntry"]:
        """Creates new link models with different link props."""
        proxy_entries = [
            LinkWithPropsSetEntry(
                input=DummyProxyModel(
                    e.model(),
                    DummyLinkModel(chr(ord('a') + i)),
                    False,
                ),
                expected_value=e.expected_value,
                expected_comment=chr(ord('a') + i),
                expected_linked=e.expected_linked,
            )
            for i, e in enumerate(entries)
        ]

        return proxy_entries

    @staticmethod
    def get_wrap_list_entries(
        after_init: bool
    ) -> list["LinkWithPropsSetEntry"]:
        """Get entries to test __wrap_list__.

        When the flag is True:
        - directly adding non-proxy models is not allowed.
        - linked models do not have their props resets

        The after_init parameter modifies some of the expected values, since
        wrapped lists will still do _descriptors.proxy_link on new values.
        """
        model_a = DummyInt(1, __gel_new__=True)
        proxy_a = DummyProxyModel(
            model_a,
            DummyLinkModel('A'),
            True,
        )
        model_b = DummyInt(2, __gel_new__=False)
        proxy_b = DummyProxyModel(
            model_b,
            DummyLinkModel('B'),
            True,
        )
        model_c = DummyInt(3, __gel_new__=True)
        proxy_c = DummyProxyModel(
            model_c,
            DummyLinkModel('C'),
            False,
        )
        model_d = DummyInt(4, __gel_new__=False)
        proxy_d = DummyProxyModel(
            model_d,
            DummyLinkModel('D'),
            False,
        )
        model_e = DummyInt(5, __gel_new__=True)
        proxy_e = DummyProxyModel(
            model_e,
            DummyLinkModel(),
            False,
        )
        model_f = DummyInt(6, __gel_new__=False)
        proxy_f = DummyProxyModel(
            model_f,
            DummyLinkModel(),
            False,
        )

        return [
            LinkWithPropsSetEntry(
                input=proxy_a,
                expected_value=1,
                expected_comment=(None if after_init else 'A'),
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_b,
                expected_value=2,
                expected_comment=(None if after_init else 'B'),
                expected_linked=True,
            ),
            LinkWithPropsSetEntry(
                input=proxy_c,
                expected_value=3,
                expected_comment='C',
                expected_linked=after_init,
            ),
            LinkWithPropsSetEntry(
                input=proxy_d,
                expected_value=4,
                expected_comment='D',
                expected_linked=after_init,
            ),
            LinkWithPropsSetEntry(
                input=proxy_e,
                expected_value=5,
                expected_comment=None,
                expected_linked=after_init,
            ),
            LinkWithPropsSetEntry(
                input=proxy_f,
                expected_value=6,
                expected_comment=None,
                expected_linked=after_init,
            ),
        ]


def _get_single_permutations() -> list[tuple[Mode, bool]]:
    return [
        (mode, wrap)
        for mode in (Mode.ReadWrite, Mode.Write)
        for wrap in (
            (True, False) if mode == Mode.ReadWrite else (False,)
        )
    ]


class TestLinkWithPropsSet(unittest.TestCase):

    def _check_list_matches_entries(
        self,
        lst: DummyIntWithPropList,
        entries: list[LinkWithPropsSetEntry],
    ) -> None:
        self.assertEqual(
            lst.unsafe_len() if lst._mode == Mode.Write else len(lst),
            len(entries)
        )

        lst_items = list(lst.unsafe_iter() if lst._mode == Mode.Write else lst)
        self.assertEqual(
            [
                item.without_linkprops()
                for item in lst_items
            ],
            [e.model() for e in entries],
        )
        self.assertEqual(
            [
                item.without_linkprops().value
                for item in lst_items
            ],
            [e.expected_value for e in entries],
        )
        self.assertEqual(
            [
                _abstract.get_proxy_linkprops(item).comment
                for item in lst_items
            ],
            [e.expected_comment for e in entries],
        )
        self.assertEqual(
            [
                _abstract.is_proxy_linked(item)
                for item in lst_items
            ],
            [e.expected_linked for e in entries],
        )

    def test_link_with_props_set_constructor_01(self):
        # Empty list
        lst = DummyIntWithPropList(__mode__=Mode.ReadWrite)
        self._check_list_matches_entries(lst, [])

        lst = DummyIntWithPropList(__mode__=Mode.Write)
        self._check_list_matches_entries(lst, [])

        with self.assertRaises(ValueError):
            lst = DummyIntWithPropList(
                __wrap_list__=True,
            )

    def test_link_with_props_set_constructor_02(self):
        # Constructor with inputs
        entries = LinkWithPropsSetEntry.get_basic_entries()
        for mode in (Mode.ReadWrite, Mode.Write):
            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
            )
            self._check_list_matches_entries(lst, entries)

    def test_link_with_props_set_add_01(self):
        # Add inserts new items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(True)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList([], __mode__=mode, __wrap_list__=wrap)
            for i, entry in enumerate(entries):
                lst.add(entry.input)
                self._check_list_matches_entries(lst, entries[:i + 1])

    def test_link_with_props_set_add_02(self):
        # Add updates link props
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()
            proxy_entries = LinkWithPropsSetEntry.get_proxied_entries(entries)

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            for i, entry in enumerate(proxy_entries):
                lst.add(entry.input)
                self._check_list_matches_entries(
                    lst,
                    proxy_entries[:i + 1] + entries[i + 1:],
                )

    def test_link_with_props_set_remove_01(self):
        # Removes items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            for i, entry in enumerate(entries):
                lst.remove(entry.input)
                self._check_list_matches_entries(lst, entries[i + 1:])

    def test_link_with_props_set_remove_02(self):
        # Removes items with different link props
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()
            proxy_entries = LinkWithPropsSetEntry.get_proxied_entries(entries)

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            for i, entry in enumerate(proxy_entries):
                lst.remove(entry.input)
                self._check_list_matches_entries(lst, entries[i + 1:])

    def test_link_with_props_set_discard_01(self):
        # Discards items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            for i, entry in enumerate(entries):
                lst.discard(entry.input)
                self._check_list_matches_entries(lst, entries[i + 1:])

    def test_link_with_props_set_discard_02(self):
        # Discards items with different link props
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()
            proxy_entries = LinkWithPropsSetEntry.get_proxied_entries(entries)

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            for i, entry in enumerate(proxy_entries):
                lst.discard(entry.input)
                self._check_list_matches_entries(lst, entries[i + 1:])

    def test_link_with_props_set_clear_01(self):
        # Clears items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(True)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.clear()
            self._check_list_matches_entries(lst, [])

    def test_link_with_props_set_update_01(self):
        # Updates new items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = (
                    LinkWithPropsSetEntry.get_wrap_list_entries(False)[:3]
                    + LinkWithPropsSetEntry.get_wrap_list_entries(True)[3:]
                )
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList(
                [e.input for e in entries[:3]],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([e.input for e in entries[3:]])
            self._check_list_matches_entries(lst, entries)

    def test_link_with_props_set_update_02(self):
        # Updates items with different link props
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
                linked_entries = (
                    entries[:5]
                    + [
                        dataclasses.replace(e, expected_linked=True)
                        for e in entries[5:]
                    ]
                )
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()
                linked_entries = entries
            proxy_entries = LinkWithPropsSetEntry.get_proxied_entries(
                linked_entries
            )

            lst = DummyIntWithPropList(
                [e.input for e in entries[:5]],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst.update([e.input for e in proxy_entries[3:]])
            self._check_list_matches_entries(
                lst,
                entries[:3] + proxy_entries[3:],
            )

    def test_link_with_props_set_operator_iadd_01(self):
        # Operator iadd updates new items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = (
                    LinkWithPropsSetEntry.get_wrap_list_entries(False)[:3]
                    + LinkWithPropsSetEntry.get_wrap_list_entries(True)[3:]
                )
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList(
                [e.input for e in entries[:3]],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [e.input for e in entries[3:]]
            self._check_list_matches_entries(lst, entries)

    def test_link_with_props_set_operator_iadd_02(self):
        # Operator iadd updates items with different link props
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
                linked_entries = (
                    entries[:5]
                    + [
                        dataclasses.replace(e, expected_linked=True)
                        for e in entries[5:]
                    ]
                )
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()
                linked_entries = entries
            proxy_entries = LinkWithPropsSetEntry.get_proxied_entries(
                linked_entries
            )

            lst = DummyIntWithPropList(
                [e.input for e in entries[:5]],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst += [e.input for e in proxy_entries[3:]]
            self._check_list_matches_entries(
                lst,
                entries[:3] + proxy_entries[3:],
            )

    def test_link_with_props_set_operator_isub_01(self):
        # Operator isub removes items
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [e.input for e in entries[3:]]
            self._check_list_matches_entries(lst, entries[:3])

    def test_link_with_props_set_operator_isub_02(self):
        # Operator isub removes items with different link props
        for mode, wrap in _get_single_permutations():
            if wrap:
                entries = LinkWithPropsSetEntry.get_wrap_list_entries(False)
            else:
                entries = LinkWithPropsSetEntry.get_basic_entries()
            proxy_entries = LinkWithPropsSetEntry.get_proxied_entries(entries)

            lst = DummyIntWithPropList(
                [e.input for e in entries],
                __mode__=mode,
                __wrap_list__=wrap,
            )
            lst -= [e.input for e in proxy_entries[3:]]
            self._check_list_matches_entries(lst, entries[:3])


if __name__ == "__main__":
    unittest.main()
