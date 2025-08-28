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


#    ███    ██  ██████  ████████ ███████
#    ████   ██ ██    ██    ██    ██
#    ██ ██  ██ ██    ██    ██    █████
#    ██  ██ ██ ██    ██    ██    ██
#    ██   ████  ██████     ██    ███████
#
#    Run `python tools/gen_models.py` to generate the models
#    module from the `orm.gel` schema and get your IDE to
#    recognize `from models.orm import default`.
#
#    Don't forget to re-run if you are messing with codegen
#    implementation or testing different versions of Gel.

from __future__ import annotations
import typing

import copy
import os

from gel import _testbase as tb
from gel._internal import _tracked_list
from gel._internal._qbmodel._pydantic._models import GelModel

from tests import nested_collections


_T = typing.TypeVar("_T")


class TestModelSync(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = os.path.join(
        os.path.dirname(__file__), "dbsetup", "chemistry.gel"
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), "dbsetup", "chemistry.esdl"
    )

    def test_model_sync_new_obj_computed_01(self):
        # Computeds from backlinks but no links set

        from models.chemistry import default

        # Create new objects
        reactor = default.Reactor()

        # Sync
        self.client.sync(reactor)

        # Check that value is initialized
        self.assertEqual(reactor.total_weight, 0)
        self.assertEqual(reactor.atom_weights, ())

    def test_model_sync_new_obj_computed_02(self):
        # Computeds from links to existing object

        from models.chemistry import default

        # Create new objects
        hydrogen = self.client.query_required_single(
            default.Element.filter(symbol="H").limit(1)
        )
        ref_atom = default.RefAtom(element=hydrogen)

        # Sync
        self.client.sync(ref_atom)

        # Check that computed values are fetched
        self.assertEqual(ref_atom.weight, 1.008)

    @tb.xfail
    def test_model_sync_new_obj_computed_03(self):
        # Computed from links to existing and new items

        from models.chemistry import default

        # Existing objects
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        # Create new objects
        reactor = default.Reactor()
        new_atom = default.Atom(reactor=reactor, element=helium)

        # Sync
        self.client.sync(reactor, new_atom)

        # Check that values are fetched
        self.assertEqual(new_atom.weight, 4.0026)
        self.assertEqual(new_atom.total_bond_count, 0)
        self.assertEqual(new_atom.total_bond_weight, 0)
        self.assertEqual(reactor.total_weight, 4.0026)  # Failing
        self.assertEqual(reactor.atom_weights, [4.0026])  # Failing

    @tb.xfail
    def test_model_sync_new_obj_computed_04(self):
        # Computed from links to existing and new items

        from models.chemistry import default

        # Existing objects
        hydrogen = self.client.query_required_single(
            default.Element.select(weight=True).filter(symbol="H").limit(1)
        )
        oxygen = self.client.query_required_single(
            default.Element.select(weight=True).filter(symbol="O").limit(1)
        )

        # Create new objects
        reactor = default.Reactor()

        o_1 = default.Atom(reactor=reactor, element=oxygen)
        h_1 = default.Atom(reactor=reactor, element=hydrogen)
        h_2 = default.Atom(reactor=reactor, element=hydrogen)
        o_1.bonds = [
            default.Atom.bonds.link(h_1, count=1),
            default.Atom.bonds.link(h_2, count=1),
        ]
        h_1.bonds = [default.Atom.bonds.link(o_1, count=1)]
        h_2.bonds = [default.Atom.bonds.link(o_1, count=1)]

        hydrogen_atoms = [h_1, h_2]
        oxygen_atoms = [h_1, h_2]

        # Sync
        self.client.sync(*hydrogen_atoms, *oxygen_atoms)

        # Check that values are fetched
        self.assertEqual(
            [atom.weight for atom in hydrogen_atoms],
            [1.008, 1.008],
        )
        self.assertEqual(  # Failing
            [atom.total_bond_count for atom in hydrogen_atoms],
            [1, 1],
        )
        self.assertEqual(  # Failing
            [atom.total_bond_weight for atom in hydrogen_atoms],
            [15.999, 15.999],
        )

        self.assertEqual(
            [atom.weight for atom in oxygen_atoms],
            [15.999],
        )
        self.assertEqual(  # Failing
            [atom.total_bond_count for atom in oxygen_atoms],
            [2],
        )
        self.assertEqual(  # Failing
            [atom.total_bond_weight for atom in oxygen_atoms],
            [1.008 * 2],
        )

        self.assertEqual(  # Failing
            reactor.total_weight,
            1.008 * 2 + 15.999,
        )
        self.assertEqual(  # Failing
            list(sorted(reactor.atom_weights)),
            (1.008, 1.008, 15.999),
        )


class TestModelSyncBasic(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type O;

        type A;
        type B {
            a: A;
        };
        type C {
            b: B;
        };
    """

    def test_model_sync_basic_01(self):
        # Sync applies ids to new objects

        from models.TestModelSyncBasic import default

        synced = default.O()
        unsynced = default.O()

        self.client.sync(synced)

        self.assertTrue(hasattr(synced, "id"))
        self.assertFalse(hasattr(unsynced, "id"))

    def test_model_sync_basic_02(self):
        # Sync applies ids to new objects in graph

        from models.TestModelSyncBasic import default

        # C(new) -> B(new) -> A(new)
        # sync all
        # all objects get ids
        a = default.A()
        b = default.B(a=a)
        c = default.C(b=b)

        self.client.sync(a, b, c)

        self.assertTrue(hasattr(a, "id"))
        self.assertTrue(hasattr(b, "id"))
        self.assertTrue(hasattr(c, "id"))

        # C(new) -> B(new) -> A(new)
        # sync only C
        # all objects get ids
        a = default.A()
        b = default.B(a=a)
        c = default.C(b=b)

        self.client.sync(c)

        self.assertTrue(hasattr(a, "id"))
        self.assertTrue(hasattr(b, "id"))
        self.assertTrue(hasattr(c, "id"))

        # C(new) -> B(new) -> A(new)
        # sync only A
        # only A gets id
        a = default.A()
        b = default.B(a=a)
        c = default.C(b=b)

        self.client.sync(a)

        self.assertTrue(hasattr(a, "id"))
        self.assertFalse(hasattr(b, "id"))
        self.assertFalse(hasattr(c, "id"))

        # C(new) -> B(existing) -> A(new)
        # sync only C
        # A and C get ids
        b = default.B()
        self.client.save(b)
        self.assertTrue(hasattr(b, "id"))

        a = default.A()
        b.a = a
        c = default.C(b=b)

        self.client.sync(c)

        self.assertTrue(hasattr(a, "id"))
        self.assertTrue(hasattr(c, "id"))


class TestModelSyncSingleProp(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type A {
            val: int64;
        };
        type B {
            val: array<int64>;
        };
        type C {
            val: tuple<str, int64>;
        };
        # save D for array<array<...>>
        type E {
            val: array<tuple<str, int64>>;
        };
        type F {
            val: tuple<str, array<int64>>;
        };
        type G {
            val: tuple<str, tuple<str, int64>>;
        };
        type H {
            val: array<tuple<str, array<int64>>>;
        };
        type I {
            val: tuple<str, array<tuple<str, int64>>>;
        };
    """

    def test_model_sync_single_prop_01(self):
        # Insert new object with single prop

        from models.TestModelSyncSingleProp import default

        def _testcase(
            model_type: typing.Type[GelModel],
            val: typing.Any,
        ) -> None:
            with_val = model_type(val=val)
            without_val = model_type()

            self.client.sync(with_val, without_val)

            self.assertEqual(with_val.val, val)
            self.assertIsNone(without_val.val)

            # cleanup
            self.client.query(model_type.delete())

        _testcase(default.A, None)
        _testcase(default.A, 1)
        _testcase(default.B, [1, 2, 3])
        _testcase(default.C, ("x", 1))
        _testcase(default.E, [("x", 1), ("y", 2), ("z", 3)])
        _testcase(default.F, ("x", [1, 2, 3]))
        _testcase(default.G, ("x", ("x", 1)))
        _testcase(
            default.H,
            [("x", [1, 2, 3]), ("y", [4, 5, 6]), ("z", [7, 8, 9])],
        )
        _testcase(
            default.I,
            ("w", [("x", 1), ("y", 2), ("z", 3)]),
        )

    def test_model_sync_single_prop_02(self):
        # Updating existing objects with single props

        from models.TestModelSyncSingleProp import default

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Any,
            changed_val: typing.Any,
        ) -> None:
            original = model_type(val=initial_val)
            self.client.save(original)

            mirror_1 = self.client.query_required_single(
                model_type.select(val=True).limit(1)
            )
            mirror_2 = self.client.query_required_single(
                model_type.select(val=True).limit(1)
            )
            mirror_3 = self.client.query_required_single(
                model_type.select(val=False).limit(1)
            )

            self.assertEqual(original.val, initial_val)
            self.assertEqual(mirror_1.val, initial_val)
            self.assertEqual(mirror_2.val, initial_val)
            self.assertFalse(hasattr(mirror_3, "val"))

            # change a value
            original.val = changed_val

            # sync some of the objects
            self.client.sync(original, mirror_1, mirror_3)

            # only synced objects with value set get update
            self.assertEqual(original.val, changed_val)
            self.assertEqual(mirror_1.val, changed_val)
            self.assertEqual(mirror_2.val, initial_val)
            # self.assertFalse(hasattr(mirror_3, 'val'))  # Fail

            # cleanup
            self.client.query(model_type.delete())

        # Change to a new value
        _testcase(default.A, 1, 2)
        _testcase(default.B, [1], [2])
        _testcase(default.C, ("a", 1), ("b", 2))
        _testcase(default.E, [("a", 1)], [("b", 2)])
        _testcase(default.F, ("a", [1]), ("b", [2]))
        _testcase(default.G, ("a", ("a", 1)), ("b", ("b", 2)))
        _testcase(default.H, [("a", [1])], [("b", [2])])
        _testcase(default.I, ("a", [("a", 1)]), ("b", [("b", 2)]))

        # Change to the same value
        _testcase(default.A, 1, 1)
        _testcase(default.B, [1], [1])
        _testcase(default.C, ("a", 1), ("a", 1))
        _testcase(default.E, [("a", 1)], [("a", 1)])
        _testcase(default.F, ("a", [1]), ("a", [1]))
        _testcase(default.G, ("a", ("a", 1)), ("a", ("a", 1)))
        _testcase(default.H, [("a", [1])], [("a", [1])])
        _testcase(default.I, ("a", [("a", 1)]), ("a", [("a", 1)]))

        # Change from None to value
        _testcase(default.A, None, 1)
        _testcase(default.B, None, [1])
        _testcase(default.C, None, ("a", 1))
        _testcase(default.E, None, [("a", 1)])
        _testcase(default.F, None, ("a", [1]))
        _testcase(default.G, None, ("a", ("a", 1)))
        _testcase(default.H, None, [("a", [1])])
        _testcase(default.I, None, ("a", [("a", 1)]))

        # Change from value to None
        _testcase(default.A, 1, None)
        _testcase(default.B, [1], None)
        _testcase(default.C, ("a", 1), None)
        _testcase(default.E, [("a", 1)], None)
        _testcase(default.F, ("a", [1]), None)
        _testcase(default.G, ("a", ("a", 1)), None)
        _testcase(default.H, [("a", [1])], None)
        _testcase(default.I, ("a", [("a", 1)]), None)

        # Change from None to None
        _testcase(default.A, None, None)
        _testcase(default.B, None, None)
        _testcase(default.C, None, None)
        _testcase(default.E, None, None)
        _testcase(default.F, None, None)
        _testcase(default.G, None, None)
        _testcase(default.H, None, None)
        _testcase(default.I, None, None)

    def test_model_sync_single_prop_03(self):
        # Reconciling different changes to single props

        from models.TestModelSyncSingleProp import default

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Any,
            changed_val_0: typing.Any,
            changed_val_1: typing.Any,
            changed_val_2: typing.Any,
        ) -> None:
            original = model_type(val=initial_val)
            self.client.save(original)

            mirror_1 = self.client.query_required_single(
                model_type.select(val=True).limit(1)
            )
            mirror_2 = self.client.query_required_single(
                model_type.select(val=True).limit(1)
            )
            mirror_3 = self.client.query_required_single(
                model_type.select(val=False).limit(1)
            )

            self.assertEqual(original.val, initial_val)
            self.assertEqual(mirror_1.val, initial_val)
            self.assertEqual(mirror_2.val, initial_val)
            self.assertFalse(hasattr(mirror_3, "val"))

            # change a value
            original.val = changed_val_0
            mirror_1.val = changed_val_1
            mirror_2.val = changed_val_2

            # sync some of the objects
            self.client.sync(original, mirror_1, mirror_3)

            # only synced objects are updated
            self.assertEqual(original.val, changed_val_0)
            self.assertEqual(mirror_1.val, changed_val_0)
            self.assertEqual(mirror_2.val, changed_val_2)
            # self.assertFalse(hasattr(mirror_3, 'val'))  # Fail

            # cleanup
            self.client.query(model_type.delete())

        _testcase(default.A, 1, 2, 3, 4)
        _testcase(default.B, [1], [2], [3], [4])
        _testcase(default.C, ("a", 1), ("b", 2), ("c", 3), ("d", 4))
        _testcase(default.E, [("a", 1)], [("b", 2)], [("c", 3)], [("d", 4)])
        _testcase(default.F, ("a", [1]), ("b", [2]), ("c", [3]), ("d", [4]))
        _testcase(
            default.G,
            ("a", ("a", 1)),
            ("b", ("b", 2)),
            ("c", ("c", 3)),
            ("d", ("d", 4)),
        )
        _testcase(
            default.H,
            [("a", [1])],
            [("b", [2])],
            [("c", [3])],
            [("d", [4])],
        )
        _testcase(
            default.I,
            ("a", [("a", 1)]),
            ("b", [("b", 2)]),
            ("c", [("c", 3)]),
            ("d", [("d", 4)]),
        )

    @tb.xfail
    def test_model_sync_single_prop_04(self):
        # Changing elements of collection single props
        # Checks deeply nested collections as well

        from models.TestModelSyncSingleProp import default

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Any,
        ) -> None:
            original = model_type(val=initial_val)
            self.client.save(original)

            mirror_1 = self.client.query_required_single(
                model_type.select(val=True).limit(1)
            )

            self.assertEqual(original.val, initial_val)
            self.assertEqual(mirror_1.val, initial_val)

            expected_val = copy.deepcopy(initial_val)

            # Iterate through all indexes of the collection
            # eg. [(1, 2), (3, 4)] will go through the indexes:
            # - [0, 0]
            # - [0, 1]
            # - [0]
            # - [1, 0]
            # - [1, 1]
            # - [1]
            visiting_indexes = nested_collections.first_indexes(expected_val)
            while visiting_indexes:
                # Create a new value with the same "shape" as it was initially
                changed_val = nested_collections.different_values_same_shape(
                    nested_collections.get_value(
                        initial_val,
                        visiting_indexes,
                    )
                )

                # Modify the prop
                maybe_modified_prop = nested_collections.set_prop_value(
                    original.val,
                    visiting_indexes,
                    changed_val,
                )
                if maybe_modified_prop:
                    # modifying tuples requires setting the updated value
                    original.val = maybe_modified_prop

                # Modify the expected val
                expected_val = nested_collections.replace_value(
                    expected_val,
                    visiting_indexes,
                    changed_val,
                )

                # sync and check objects are updated
                self.client.sync(original, mirror_1)
                self.assertEqual(original.val, expected_val)
                self.assertEqual(mirror_1.val, expected_val)

                # visit next index
                visiting_indexes = nested_collections.increment_indexes(
                    expected_val, visiting_indexes
                )

            # cleanup
            self.client.query(model_type.delete())

        _testcase(default.B, [1, 2, 3])
        _testcase(default.C, ("x", 1))
        _testcase(default.E, [("x", 1), ("y", 2), ("z", 3)])
        _testcase(default.F, ("x", [1, 2, 3]))  # Fail
        _testcase(default.G, ("x", ("x", 1)))
        _testcase(
            default.H,
            [("x", [1, 2, 3]), ("y", [4, 5, 6]), ("z", [7, 8, 9])],
        )  # Fail
        _testcase(
            default.I,
            ("w", [("x", 1), ("y", 2), ("z", 3)]),
        )  # Fail

    @tb.xfail
    def test_model_sync_single_prop_05(self):
        # Existing object without prop should not have it fetched

        from models.TestModelSyncSingleProp import default

        original = default.A(val=1)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )
        original.val = 2
        self.client.save(original)
        self.client.sync(mirror_1)
        self.assertFalse(hasattr(mirror_1, "val"))

        # Sync alongside another object with the prop set
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        original.val = 3
        self.client.save(original)
        self.client.sync(mirror_1, mirror_2)
        self.assertFalse(hasattr(mirror_1, "val"))  # Fail


class TestModelSyncMultiProp(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type A {
            multi val: int64;
        };
        type B {
            multi val: array<int64>;
        };
        type C {
            multi val: tuple<str, int64>;
        };
    """

    def _base_change_testcase(
        self,
        model_type: typing.Type[GelModel],
        initial_val: typing.Collection[typing.Any],
        change_original: typing.Callable[[GelModel], None],
        expected_val: typing.Collection[typing.Any],
    ) -> None:
        expected_val = copy.deepcopy(expected_val)

        original = model_type(val=initial_val)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            model_type.select(val=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            model_type.select(val=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            model_type.select(val=False).limit(1)
        )

        self.assertEqual(original.val, initial_val)
        self.assertEqual(mirror_1.val, initial_val)
        self.assertEqual(mirror_2.val, initial_val)
        self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_3.val._items, [])

        # change a value
        change_original(original)

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects with value set get update
        self.assertEqual(original.val, expected_val)
        self.assertEqual(mirror_1.val, expected_val)
        self.assertEqual(mirror_2.val, initial_val)
        # self.assertEqual(mirror_3.val, [])  # Fail

        # cleanup
        self.client.query(model_type.delete())

    def test_model_sync_multi_prop_01(self):
        # Insert new object with multi prop

        from models.TestModelSyncMultiProp import default

        def _testcase(
            model_type: typing.Type[GelModel],
            val: typing.Any,
        ) -> None:
            with_val = model_type(val=val)
            without_val = model_type()

            self.client.sync(with_val, without_val)

            self.assertEqual(with_val.val, val)
            self.assertEqual(without_val.val, [])

            # cleanup
            self.client.query(model_type.delete())

        _testcase(default.A, [])
        _testcase(default.B, [])
        _testcase(default.C, [])

        _testcase(default.A, [1, 2, 3])
        _testcase(default.B, [[]])
        _testcase(default.B, [[1], [2, 2], [3, 3, 3]])
        _testcase(default.C, [("a", 1), ("b", 2), ("c", 3)])

    def test_model_sync_multi_prop_02(self):
        # Updating existing objects with multi props
        # Set prop to new value

        def _get_assign_val_func(
            changed_val: typing.Collection[typing.Any],
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.val = changed_val

            return change

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Collection[typing.Any],
            changed_val: typing.Collection[typing.Any],
        ) -> None:
            self._base_change_testcase(
                model_type,
                initial_val,
                _get_assign_val_func(changed_val),
                changed_val,
            )

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [], [])
        _testcase(default.A, [], [1, 2, 3])
        _testcase(default.A, [1, 2, 3], [])
        _testcase(default.A, [1, 2, 3], [2, 3, 4])
        _testcase(default.A, [1, 2, 3], [4, 5, 6])

        _testcase(default.B, [], [])
        _testcase(default.B, [], [[]])
        _testcase(default.B, [], [[1], [2, 2], [3, 3, 3]])
        _testcase(default.B, [[1], [2, 2], [3, 3, 3]], [])
        _testcase(default.B, [[1], [2, 2], [3, 3, 3]], [[]])
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [[2, 2], [3, 3, 3], [4, 4, 4, 4]],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [[4], [5, 5], [6, 6, 6]],
        )

        _testcase(default.C, [], [])
        _testcase(default.C, [], [("a", 1), ("b", 2), ("c", 3)])
        _testcase(default.C, [("a", 1), ("b", 2), ("c", 3)], [])
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [("b", 2), ("c", 3), ("d", 4)],
        )
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [("d", 4), ("e", 5), ("f", 6)],
        )

    def test_model_sync_multi_prop_03(self):
        # Updating existing objects with multi props
        # Tracked list insert

        def _get_insert_val_func(
            insert_pos: int,
            insert_val: typing.Collection[typing.Any],
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.val.insert(insert_pos, insert_val)

            return change

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Collection[typing.Any],
            insert_pos: int,
            insert_val: typing.Any,
            expected_val: typing.Collection[typing.Any],
        ) -> None:
            self._base_change_testcase(
                model_type,
                initial_val,
                _get_insert_val_func(insert_pos, insert_val),
                expected_val,
            )

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [], 0, 9, [9])
        _testcase(default.A, [1, 2, 3], 2, 9, [1, 2, 3, 9])

        _testcase(default.B, [], 0, [], [[]])
        _testcase(default.B, [], 0, [9], [[9]])
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            2,
            [],
            [[1], [2, 2], [3, 3, 3], []],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            2,
            [9],
            [[1], [2, 2], [3, 3, 3], [9]],
        )

        _testcase(default.C, [], 0, ("i", 9), [("i", 9)])
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            2,
            ("i", 9),
            [("a", 1), ("b", 2), ("c", 3), ("i", 9)],
        )

    def test_model_sync_multi_prop_04(self):
        # Updating existing objects with multi props
        # Tracked list extend

        def _get_extend_val_func(
            extend_vals: typing.Collection[typing.Any],
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.val.extend(extend_vals)

            return change

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Collection[typing.Any],
            extend_vals: typing.Collection[typing.Any],
            expected_val: typing.Collection[typing.Any],
        ) -> None:
            self._base_change_testcase(
                model_type,
                initial_val,
                _get_extend_val_func(extend_vals),
                expected_val,
            )

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [], [], [])
        _testcase(default.A, [], [1], [1])
        _testcase(default.A, [1, 2, 3], [], [1, 2, 3])
        _testcase(default.A, [1, 2, 3], [1, 2, 3], [1, 2, 3, 1, 2, 3])
        _testcase(default.A, [1, 2, 3], [2, 3, 4], [1, 2, 3, 2, 3, 4])
        _testcase(default.A, [1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6])

        _testcase(default.B, [], [], [])
        _testcase(default.B, [], [[]], [[]])
        _testcase(default.B, [], [[1]], [[1]])
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [],
            [[1], [2, 2], [3, 3, 3]],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [[]],
            [[1], [2, 2], [3, 3, 3], []],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [[1], [2, 2], [3, 3, 3]],
            [[1], [2, 2], [3, 3, 3], [1], [2, 2], [3, 3, 3]],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [[2, 2], [3, 3, 3], [4, 4, 4, 4]],
            [[1], [2, 2], [3, 3, 3], [2, 2], [3, 3, 3], [4, 4, 4, 4]],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [[4], [5], [6]],
            [[1], [2, 2], [3, 3, 3], [4], [5], [6]],
        )

        _testcase(default.C, [], [], [])
        _testcase(default.C, [], [("a", 1)], [("a", 1)])
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [],
            [("a", 1), ("b", 2), ("c", 3)],
        )
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [("a", 1), ("b", 2), ("c", 3)],
            [("a", 1), ("b", 2), ("c", 3), ("a", 1), ("b", 2), ("c", 3)],
        )
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [("b", 2), ("c", 3), ("d", 4)],
            [("a", 1), ("b", 2), ("c", 3), ("b", 2), ("c", 3), ("d", 4)],
        )
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [("d", 4), ("e", 5), ("f", 6)],
            [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5), ("f", 6)],
        )

    def test_model_sync_multi_prop_05(self):
        # Updating existing objects with multi props
        # Tracked list append

        def _get_append_val_func(
            append_val: typing.Any,
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.val.append(append_val)

            return change

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Collection[typing.Any],
            append_val: typing.Any,
            expected_val: typing.Collection[typing.Any],
        ) -> None:
            self._base_change_testcase(
                model_type,
                initial_val,
                _get_append_val_func(append_val),
                expected_val,
            )

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [], 1, [1])
        _testcase(default.A, [1, 2, 3], 2, [1, 2, 3, 2])
        _testcase(default.A, [1, 2, 3], 4, [1, 2, 3, 4])

        _testcase(
            default.B,
            [],
            [],
            [[]],
        )
        _testcase(
            default.B,
            [],
            [1],
            [[1]],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [],
            [[1], [2, 2], [3, 3, 3], []],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [2, 2],
            [[1], [2, 2], [3, 3, 3], [2, 2]],
        )
        _testcase(
            default.B,
            [[1], [2, 2], [3, 3, 3]],
            [4, 4, 4, 4],
            [[1], [2, 2], [3, 3, 3], [4, 4, 4, 4]],
        )

        _testcase(
            default.C,
            [],
            ("a", 1),
            [("a", 1)],
        )
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            ("b", 2),
            [("a", 1), ("b", 2), ("c", 3), ("b", 2)],
        )
        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            ("d", 4),
            [("a", 1), ("b", 2), ("c", 3), ("d", 4)],
        )

    def test_model_sync_multi_prop_06(self):
        # Updating existing objects with multi props
        # Tracked list pop

        def _get_pop_val_func() -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.val.pop()

            return change

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Collection[typing.Any],
            expected_val: typing.Collection[typing.Any],
        ) -> None:
            self._base_change_testcase(
                model_type,
                initial_val,
                _get_pop_val_func(),
                expected_val,
            )

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [1, 2, 3], [1, 2])

        _testcase(default.B, [[1], [2, 2], [3, 3, 3]], [[1], [2, 2]])

        _testcase(
            default.C,
            [("a", 1), ("b", 2), ("c", 3)],
            [("a", 1), ("b", 2)],
        )

    def test_model_sync_multi_prop_07(self):
        # Updating existing objects with single props
        # Clear prop

        def _get_clear_val_func() -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.val.clear()

            return change

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Collection[typing.Any],
        ) -> None:
            self._base_change_testcase(
                model_type,
                initial_val,
                _get_clear_val_func(),
                [],
            )

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [])
        _testcase(default.A, [1, 2, 3])

        _testcase(default.B, [])
        _testcase(default.B, [[1], [2, 2], [3, 3, 3]])

        _testcase(default.C, [])
        _testcase(default.C, [("a", 1), ("b", 2), ("c", 3)])

    @tb.xfail
    def test_model_sync_multi_prop_08(self):
        # Existing object without prop should not have it fetched

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Any,
            changed_val_0: typing.Any,
            changed_val_1: typing.Any,
            changed_val_2: typing.Any,
        ):
            original = model_type(val=initial_val)
            self.client.save(original)

            mirror_1 = self.client.query_required_single(
                model_type.select(val=False).limit(1)
            )
            original.val = changed_val_0
            self.client.save(original)
            self.client.sync(mirror_1)
            self.assertEqual(mirror_1.val._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_1.val._items, [])

            # Sync alongside another object with the prop set
            mirror_2 = self.client.query_required_single(
                model_type.select(val=True).limit(1)
            )
            original.val = changed_val_1
            self.client.save(original)
            self.client.sync(mirror_1, mirror_2)
            self.assertEqual(mirror_1.val._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_1.val._items, [])

            # Sync alongside another object with the prop changed
            mirror_2 = self.client.query_required_single(
                model_type.select(targets=True).limit(1)
            )
            mirror_2.val = changed_val_2
            self.client.save(original)
            self.client.sync(mirror_1, mirror_2)
            self.assertEqual(mirror_1.targets._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_1.targets._items, [])  # Fail

            # cleanup
            self.client.query(model_type.delete())

        from models.TestModelSyncMultiProp import default

        _testcase(default.A, [1], [2], [3], [4])
        _testcase(default.B, [[1]], [[2, 2]], [[3, 3, 3]], [[4, 4, 4, 4]])
        _testcase(default.C, [("a", 1)], [("b", 2)], [("c", 3)], [("d", 4)])


class TestModelSyncSingleLink(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type Target;
        type Source {
            target: Target;
        };
        type SourceWithProp {
            target: Target {
                lprop: int64;
            };
        };
    """

    def _check_links_equal(
        self, actual: typing.Any, expected: typing.Any
    ) -> None:
        self.assertEqual(actual, expected)

        # Also check linkprops
        actual_has_lprop = hasattr(actual, '__linkprops__')
        expected_has_lprop = hasattr(expected, '__linkprops__')
        self.assertEqual(actual_has_lprop, expected_has_lprop)
        if actual_has_lprop and expected_has_lprop:
            self.assertEqual(
                actual.__linkprops__.lprop,
                expected.__linkprops__.lprop,
            )

    def test_model_sync_single_link_01(self):
        # Insert new object with single link

        from models.TestModelSyncSingleLink import default

        target = default.Target()
        self.client.save(target)

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_target: typing.Any,
        ) -> None:
            with_target = model_type(target=initial_target)
            without_target = model_type()

            self.client.sync(with_target, without_target)

            self._check_links_equal(with_target.target, initial_target)
            self._check_links_equal(without_target.target, None)

            # cleanup
            self.client.query(model_type.delete())

        _testcase(default.Source, None)
        _testcase(default.Source, target)

        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target, lprop=1),
        )

    @tb.xfail
    def test_model_sync_single_link_01a(self):
        # Asserting error in _save
        from models.TestModelSyncSingleLink import default

        with_none = default.SourceWithProp(target=None)
        self.client.sync(with_none)

    def test_model_sync_single_link_02(self):
        # Updating existing objects with single link

        from models.TestModelSyncSingleLink import default

        target_a = default.Target()
        target_b = default.Target()
        self.client.save(target_a, target_b)

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_target: typing.Any,
            changed_target: typing.Any,
        ) -> None:
            original = model_type(target=initial_target)
            self.client.save(original)

            mirror_1 = self.client.query_required_single(
                model_type.select(target=True).limit(1)
            )
            mirror_2 = self.client.query_required_single(
                model_type.select(target=True).limit(1)
            )
            mirror_3 = self.client.query_required_single(
                model_type.select(target=False).limit(1)
            )

            self._check_links_equal(original.target, initial_target)
            self._check_links_equal(mirror_1.target, initial_target)
            self._check_links_equal(mirror_2.target, initial_target)
            self.assertFalse(hasattr(mirror_3, "val"))

            # change a value
            original.target = changed_target

            # sync some of the objects
            self.client.sync(original, mirror_1, mirror_3)

            # only synced objects with value set get update
            self._check_links_equal(original.target, changed_target)
            self._check_links_equal(mirror_1.target, changed_target)
            self._check_links_equal(mirror_2.target, initial_target)
            self.assertFalse(hasattr(mirror_3, 'val'))

            # cleanup
            self.client.query(model_type.delete())

        # Change to a new value
        _testcase(default.Source, target_a, target_b)
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a),
            default.SourceWithProp.target.link(target_b),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            default.SourceWithProp.target.link(target_b),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a),
            default.SourceWithProp.target.link(target_b, lprop=1),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            default.SourceWithProp.target.link(target_b, lprop=1),
        )

        # only changing lprop
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a),
            default.SourceWithProp.target.link(target_a, lprop=2),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            default.SourceWithProp.target.link(target_a),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            default.SourceWithProp.target.link(target_a, lprop=2),
        )

        # Change to the same value
        _testcase(default.Source, target_a, target_a)
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a),
            default.SourceWithProp.target.link(target_a),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            default.SourceWithProp.target.link(target_a, lprop=1),
        )

    @tb.xfail
    def test_model_sync_single_link_02a(self):
        # Updating existing objects with single link
        # Change from None to value

        from models.TestModelSyncSingleLink import default

        changed_target = default.Target()
        self.client.save(changed_target)

        original = default.Source()
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.Source.select(target=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.Source.select(target=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.Source.select(target=False).limit(1)
        )

        self.assertIsNone(original.target, None)
        self.assertIsNone(mirror_1.target, None)
        self.assertIsNone(mirror_2.target, None)
        self.assertFalse(hasattr(mirror_3, "val"))

        # change a value
        original.target = changed_target

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)  # Error here

        # only synced objects with value set get update
        self._check_links_equal(original.target, changed_target)
        self._check_links_equal(mirror_1.target, changed_target)
        self.assertIsNone(mirror_2.target)
        self.assertFalse(hasattr(mirror_3, 'val'))

    @tb.xfail
    def test_model_sync_single_link_02b(self):
        # Updating existing objects with single link
        # Change from value to None

        from models.TestModelSyncSingleLink import default

        initial_target = default.Target()
        self.client.save(initial_target)

        original = default.Source(target=initial_target)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.Source.select(target=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.Source.select(target=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.Source.select(target=False).limit(1)
        )

        self._check_links_equal(original.target, initial_target)
        self._check_links_equal(mirror_1.target, initial_target)
        self._check_links_equal(mirror_2.target, initial_target)
        self.assertFalse(hasattr(mirror_3, "val"))

        # change a value
        original.target = None

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)  # Error here

        # only synced objects with value set get update
        self.assertIsNone(original.target)
        self.assertIsNone(mirror_1.target)
        self._check_links_equal(mirror_2.target, initial_target)
        self.assertFalse(hasattr(mirror_3, 'val'))

    def _testcase_03(
        self,
        model_type: typing.Type[GelModel],
        initial_target: typing.Any,
        changed_target_0: typing.Any,
        changed_target_1: typing.Any,
        changed_target_2: typing.Any,
    ) -> None:
        original = model_type(target=initial_target)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            model_type.select(target=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            model_type.select(target=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            model_type.select(target=False).limit(1)
        )

        self._check_links_equal(original.target, initial_target)
        self._check_links_equal(mirror_1.target, initial_target)
        self._check_links_equal(mirror_2.target, initial_target)
        self.assertFalse(hasattr(mirror_3, "val"))

        # change a value
        original.target = changed_target_0
        mirror_1.target = changed_target_1
        mirror_2.target = changed_target_2

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects are updated
        self._check_links_equal(original.target, changed_target_0)
        self._check_links_equal(mirror_1.target, changed_target_0)
        self._check_links_equal(mirror_2.target, changed_target_2)
        self.assertFalse(hasattr(mirror_3, 'val'))

        # cleanup
        self.client.query(model_type.delete())

    def test_model_sync_single_link_03(self):
        # Reconciling different changes to single link

        from models.TestModelSyncSingleLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        self._testcase_03(
            default.Source,
            target_a,
            target_b,
            target_c,
            target_d,
        )

    @tb.xfail
    def test_model_sync_single_link_03a(self):
        # ISE on sync()
        # gel.errors.InternalServerError: more than one row returned by a
        # subquery used as an expression
        from models.TestModelSyncSingleLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        self._testcase_03(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a),
            default.SourceWithProp.target.link(target_b),
            default.SourceWithProp.target.link(target_c),
            default.SourceWithProp.target.link(target_d),
        )

    @tb.xfail
    def test_model_sync_single_link_03b(self):
        # ISE on sync()
        # gel.errors.InternalServerError: more than one row returned by a
        # subquery used as an expression
        from models.TestModelSyncSingleLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        self._testcase_03(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            default.SourceWithProp.target.link(target_a, lprop=2),
            default.SourceWithProp.target.link(target_a, lprop=3),
            default.SourceWithProp.target.link(target_a, lprop=4),
        )

    @tb.xfail
    def test_model_sync_single_link_04(self):
        # Existing object without link should not have it fetched

        from models.TestModelSyncSingleLink import default

        initial_target = default.Target()
        changed_target_0 = default.Target()
        changed_target_1 = default.Target()
        changed_target_2 = default.Target()
        self.client.save(
            initial_target,
            changed_target_0,
            changed_target_1,
            changed_target_2,
        )

        original = default.Source(target=initial_target)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.Source.select(target=False).limit(1)
        )
        original.target = changed_target_0
        self.client.save(original)
        self.client.sync(mirror_1)
        self.assertFalse(hasattr(mirror_1, "target"))

        # Sync alongside another object with the prop set
        mirror_2 = self.client.query_required_single(
            default.Source.select(target=True).limit(1)
        )
        original.target = changed_target_1
        self.client.save(original)
        self.client.sync(mirror_1, mirror_2)  # Error here
        self.assertFalse(hasattr(mirror_1, "target"))

        # Sync alongside another object with the prop changed
        mirror_2 = self.client.query_required_single(
            default.Source.select(targets=True).limit(1)
        )
        mirror_2.target = changed_target_2
        self.client.sync(mirror_1, mirror_2)
        self.assertEqual(mirror_1.targets._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_1.targets._items, [])
