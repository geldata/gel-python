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
    """

    def test_model_sync_basic_01(self):
        # Sync applies ids to new objects

        from models.TestModelSyncBasic import default

        synced = default.O()
        unsynced = default.O()

        self.client.sync(synced)

        self.assertTrue(hasattr(synced, "id"))
        self.assertFalse(hasattr(unsynced, "id"))


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
        # Set prop to new value

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

        _testcase(default.A, 1, 2)
        _testcase(default.B, [1], [2])
        _testcase(default.C, ("a", 1), ("b", 2))
        _testcase(default.E, [("a", 1)], [("b", 2)])
        _testcase(default.F, ("a", [1]), ("b", [2]))
        _testcase(default.G, ("a", ("a", 1)), ("b", ("b", 2)))
        _testcase(default.H, [("a", [1])], [("b", [2])])
        _testcase(default.I, ("a", [("a", 1)]), ("b", [("b", 2)]))

    def test_model_sync_single_prop_03(self):
        # Updating existing objects with single props
        # Set prop to None

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
            original.val = None

            # sync some of the objects
            self.client.sync(original, mirror_1, mirror_3)

            # only synced objects with value set get update
            self.assertIsNone(original.val)
            self.assertIsNone(mirror_1.val)
            self.assertEqual(mirror_2.val, initial_val)
            # self.assertFalse(hasattr(mirror_3, 'val'))  # Fail

        _testcase(default.A, 1)
        _testcase(default.B, [1])
        _testcase(default.C, ("a", 1))
        _testcase(default.E, [("a", 1)])
        _testcase(default.F, ("a", [1]))
        _testcase(default.G, ("a", ("a", 1)))
        _testcase(default.H, [("a", [1])])
        _testcase(default.I, ("a", [("a", 1)]))

    def test_model_sync_single_prop_04(self):
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
    def test_model_sync_single_prop_05(self):
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
    def test_model_sync_single_prop_06(self):
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

        _testcase(default.A, [1, 2, 3])
        _testcase(default.B, [[1], [2, 2], [3, 3, 3]])
        _testcase(default.C, [("a", 1), ("b", 2), ("c", 3)])

    @tb.xfail
    def test_model_sync_multi_prop_02(self):
        # Updating existing objects with multi props
        # Set prop to new value

        from models.TestModelSyncMultiProp import default

        initial_val = [1, 2, 3]
        changed_val = [4, 5, 6]

        original = default.A(val=initial_val)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )

        self.assertEqual(original.val, initial_val)
        self.assertEqual(mirror_1.val, initial_val)
        self.assertEqual(mirror_2.val, initial_val)
        self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_3.val._items, [])

        # change a value
        original.val = changed_val

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects with value set get update
        self.assertEqual(original.val, changed_val)
        self.assertEqual(mirror_1.val, changed_val)
        self.assertEqual(mirror_2.val, initial_val)
        # self.assertEqual(mirror_3.val, [])

    def test_model_sync_multi_prop_03(self):
        # Updating existing objects with multi props
        # Tracked list insert

        from models.TestModelSyncMultiProp import default

        initial_val = [1, 2, 3]
        insert_pos = 2
        insert_val = 9

        original = default.A(val=initial_val)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )

        self.assertEqual(original.val, initial_val)
        self.assertEqual(mirror_1.val, initial_val)
        self.assertEqual(mirror_2.val, initial_val)
        self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_3.val._items, [])

        # change a value
        original.val.insert(insert_pos, insert_val)

        expected_val = initial_val.copy()
        expected_val.append(insert_val)

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects with value set get update
        self.assertEqual(list(sorted(original.val)), expected_val)
        self.assertEqual(list(sorted(mirror_1.val)), expected_val)
        self.assertEqual(mirror_2.val, initial_val)
        # self.assertEqual(mirror_3.val, [])  # Fail

    def test_model_sync_multi_prop_04(self):
        # Updating existing objects with multi props
        # Tracked list extend

        from models.TestModelSyncMultiProp import default

        initial_val = [1, 2, 3]
        extend_vals = [4, 5, 6]

        original = default.A(val=initial_val)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )

        self.assertEqual(original.val, initial_val)
        self.assertEqual(mirror_1.val, initial_val)
        self.assertEqual(mirror_2.val, initial_val)
        self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_3.val._items, [])

        # change a value
        original.val.extend(extend_vals)

        expected_val = initial_val.copy()
        expected_val.extend(extend_vals)

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects with value set get update
        self.assertEqual(list(sorted(original.val)), expected_val)
        self.assertEqual(list(sorted(mirror_1.val)), expected_val)
        self.assertEqual(mirror_2.val, initial_val)
        # self.assertEqual(mirror_3.val, [])  # Fail

    def test_model_sync_multi_prop_05(self):
        # Updating existing objects with multi props
        # Tracked list append

        from models.TestModelSyncMultiProp import default

        initial_val = [1, 2, 3]
        append_val = 4

        original = default.A(val=initial_val)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )

        self.assertEqual(original.val, initial_val)
        self.assertEqual(mirror_1.val, initial_val)
        self.assertEqual(mirror_2.val, initial_val)
        self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_3.val._items, [])

        # change a value
        original.val.append(append_val)

        expected_val = initial_val.copy()
        expected_val.append(append_val)

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects with value set get update
        self.assertEqual(list(sorted(original.val)), expected_val)
        self.assertEqual(list(sorted(mirror_1.val)), expected_val)
        self.assertEqual(mirror_2.val, initial_val)
        # self.assertEqual(mirror_3.val, [])  # Fail

    def test_model_sync_multi_prop_06(self):
        # Updating existing objects with multi props
        # Tracked list pop

        from models.TestModelSyncMultiProp import default

        initial_val = [1, 2, 3]

        original = default.A(val=initial_val)
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        mirror_3 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )

        self.assertEqual(original.val, initial_val)
        self.assertEqual(mirror_1.val, initial_val)
        self.assertEqual(mirror_2.val, initial_val)
        self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_3.val._items, [])

        # change a value
        original.val.pop()

        expected_val = initial_val.copy()
        expected_val.pop()

        # sync some of the objects
        self.client.sync(original, mirror_1, mirror_3)

        # only synced objects with value set get update
        self.assertEqual(list(sorted(original.val)), expected_val)
        self.assertEqual(list(sorted(mirror_1.val)), expected_val)
        self.assertEqual(mirror_2.val, initial_val)
        # self.assertEqual(mirror_3.val, [])  # Fail

    def test_model_sync_multi_prop_07(self):
        # Updating existing objects with single props
        # Clear prop

        from models.TestModelSyncMultiProp import default

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_val: typing.Any,
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
            self.assertEqual(mirror_3.val._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_3.val._items, [])

            # change a value
            original.val.clear()

            # sync some of the objects
            self.client.sync(original, mirror_1, mirror_3)

            # only synced objects with value set get update
            self.assertEqual(original.val, [])
            self.assertEqual(mirror_1.val, [])
            self.assertEqual(mirror_2.val, initial_val)
            self.assertEqual(mirror_3.val, [])  # Fail

        _testcase(default.A, [1, 2, 3])
        _testcase(default.B, [[1], [2, 2], [3, 3, 3]])
        _testcase(default.C, [("a", 1), ("b", 2), ("c", 3)])

    @tb.xfail
    def test_model_sync_multi_prop_08(self):
        # Existing object without prop should not have it fetched

        from models.TestModelSyncMultiProp import default

        original = default.A(val=[1])
        self.client.save(original)

        mirror_1 = self.client.query_required_single(
            default.A.select(val=False).limit(1)
        )
        original.val = [2]
        self.client.save(original)
        self.client.sync(mirror_1)
        self.assertEqual(mirror_1.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_1.val._items, [])

        # Sync alongside another object with the prop set
        mirror_2 = self.client.query_required_single(
            default.A.select(val=True).limit(1)
        )
        original.val = [3]
        self.client.save(original)
        self.client.sync(mirror_1, mirror_2)
        self.assertEqual(mirror_1.val._mode, _tracked_list.Mode.Write)
        self.assertEqual(mirror_1.val._items, [])
