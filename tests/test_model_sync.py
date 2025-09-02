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

from gel._internal import _tracked_list
from gel._internal._qbmodel._abstract import _link_set
from gel._internal._qbmodel._pydantic._models import GelModel
from gel._internal._testbase import _models as tb

from . import nested_collections

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
        self.assertEqual(reactor.total_weight, 4.0026)
        self.assertEqual(reactor.atom_weights, (4.0026,))

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
        oxygen_atoms = [o_1]

        # Sync
        self.client.sync(*hydrogen_atoms, *oxygen_atoms)

        # Check that values are fetched
        self.assertEqual(
            [atom.weight for atom in hydrogen_atoms],
            [1.008, 1.008],
        )
        self.assertEqual(
            [atom.total_bond_count for atom in hydrogen_atoms],
            [1, 1],
        )
        self.assertEqual(
            [atom.total_bond_weight for atom in hydrogen_atoms],
            [15.999, 15.999],
        )

        self.assertEqual(
            [atom.weight for atom in oxygen_atoms],
            [15.999],
        )
        self.assertEqual(
            [atom.total_bond_count for atom in oxygen_atoms],
            [2],
        )
        self.assertEqual(
            [atom.total_bond_weight for atom in oxygen_atoms],
            [1.008 * 2],
        )

        self.assertEqual(
            reactor.total_weight,
            1.008 * 2 + 15.999,
        )
        self.assertEqual(
            tuple(sorted(reactor.atom_weights)),
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


class TestModelSyncComputedSingleProp(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type FromConstant {
            val := 1;
        };

        type FromSingleProp {
            n: int64;
            val := .n + 1;
        };

        type FromMultiProp {
            multi n: int64;
            val := sum(.n) + 1;
        };

        type Target {
            val: int64;
        };
        type FromSingleLink {
            target: Target;
            val := .target.val + 1;
        };
        type FromMultiLink {
            multi target: Target;
            val := sum(.target.val) + 1;
        };

        type ExclusiveSource {
            target: FromExclusiveBacklink {
                constraint exclusive;
            };
            val: int64;
        };
        type FromExclusiveBacklink {
            val := .<target[is ExclusiveSource].val + 1;
        }

        type SingleSource {
            target: FromSingleBacklink;
            val: int64;
        };
        type FromSingleBacklink {
            val := sum(.<target[is SingleSource].val) + 1;
        }

        type FromStableExpr {
            val := count(detached FromStableExpr);
        };

        global SomeGlobal: int64;
        type FromGlobal {
            val := global SomeGlobal + 1;
        };
    """

    def test_model_sync_computed_single_prop_constant_01(self):
        # Create new

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromConstant()
        self.client.sync(original)

        self.assertEqual(original.val, 1)

    def test_model_sync_computed_single_prop_constant_02(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromConstant())
        mirror = self.client.query_required_single(
            default.FromConstant.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_single_prop_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleProp()
        self.client.sync(original)

        self.assertEqual(original.val, None)

    def test_model_sync_computed_single_prop_from_single_prop_02(self):
        # Create new, expr prop has value

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleProp(n=1)
        self.client.sync(original)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_single_prop_03(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromSingleProp())
        mirror = self.client.query_required_single(
            default.FromSingleProp.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_single_prop_04(self):
        # Update with val set, initially None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleProp()
        self.client.sync(original)

        original.n = 9
        self.client.sync(original)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_single_prop_05(self):
        # Update with val set, initially not None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleProp(n=1)
        self.client.sync(original)

        original.n = 9
        self.client.sync(original)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_multi_prop_01(self):
        # Create new, expr prop is empty

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromMultiProp()
        self.client.sync(original)

        self.assertEqual(original.val, 1)

    def test_model_sync_computed_single_prop_from_multi_prop_02(self):
        # Create new, expr prop has values

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromMultiProp(n=[1, 2, 3])
        self.client.sync(original)

        self.assertEqual(original.val, 7)

    def test_model_sync_computed_single_prop_from_multi_prop_03(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromMultiProp())
        mirror = self.client.query_required_single(
            default.FromMultiProp.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_multi_prop_04(self):
        # Update with val set, initially empty

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromMultiProp()
        self.client.sync(original)

        original.n = [7, 8, 9]
        self.client.sync(original)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_single_prop_from_multi_prop_05(self):
        # Update with val set, initially has values

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromMultiProp(n=[1, 2, 3])
        self.client.sync(original)

        original.n = [7, 8, 9]
        self.client.sync(original)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_single_prop_from_single_link_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleLink()
        self.client.sync(original)

        self.assertEqual(original.val, None)

    def test_model_sync_computed_single_prop_from_single_link_02(self):
        # Create new, target already exists

        from models.TestModelSyncComputedSingleProp import default

        target = default.Target(val=1)
        self.client.save(target)

        original = default.FromSingleLink(target=target)
        self.client.sync(original)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_single_link_03(self):
        # Create new, target created alongside object

        from models.TestModelSyncComputedSingleProp import default

        target = default.Target(val=1)
        original = default.FromSingleLink(target=target)
        self.client.sync(original, target)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_single_link_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromSingleLink())
        mirror = self.client.query_required_single(
            default.FromSingleLink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_single_link_05(self):
        # Update with val set, initially target is None

        from models.TestModelSyncComputedSingleProp import default

        target = default.Target(val=9)
        original = default.FromSingleLink()
        self.client.sync(original, target)

        original.target = target
        self.client.sync(original)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_single_link_06(self):
        # Update with val set, initially target is set
        # target val changes

        from models.TestModelSyncComputedSingleProp import default

        target = default.Target(val=1)
        self.client.save(target)

        original = default.FromSingleLink(target=target)
        self.client.sync(original)

        target.val = 9
        self.client.sync(original, target)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_single_link_07(self):
        # Update with val set, initially target is set
        # target changes

        from models.TestModelSyncComputedSingleProp import default

        target_a = default.Target(val=1)
        self.client.save(target_a)

        original = default.FromSingleLink(target=target_a)
        self.client.sync(original)

        target_b = default.Target(val=9)
        original.target = target_b
        self.client.sync(original, target_b)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_multi_prop_from_multi_link_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromMultiLink()
        self.client.sync(original)

        self.assertEqual(original.val, 1)

    def test_model_sync_computed_multi_prop_from_multi_link_02(self):
        # Create new, target already exists

        from models.TestModelSyncComputedSingleProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original)

        self.assertEqual(original.val, 7)

    def test_model_sync_computed_multi_prop_from_multi_link_03(self):
        # Create new, target created alongside object

        from models.TestModelSyncComputedSingleProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original, target_a, target_b, target_c)

        self.assertEqual(original.val, 7)

    def test_model_sync_computed_multi_prop_from_multi_link_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromMultiLink())
        mirror = self.client.query_required_single(
            default.FromMultiLink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_multi_link_05(self):
        # Update with val set, initially target is empty

        from models.TestModelSyncComputedSingleProp import default

        target_a = default.Target(val=7)
        target_b = default.Target(val=8)
        target_c = default.Target(val=9)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink()
        self.client.sync(original)

        original.target = [target_a, target_b, target_c]
        self.client.sync(original)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_multi_prop_from_multi_link_06(self):
        # Update with val set, initially target has values
        # target val changes

        from models.TestModelSyncComputedSingleProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original)

        target_a.val = 7
        target_b.val = 8
        target_c.val = 9
        self.client.sync(original)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_multi_prop_from_multi_link_07(self):
        # Update with val set, initially target has values
        # target changes

        from models.TestModelSyncComputedSingleProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original)

        target_d = default.Target(val=7)
        target_e = default.Target(val=8)
        target_f = default.Target(val=9)
        original.target = [target_d, target_e, target_f]
        self.client.sync(original, target_d, target_e, target_f)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_single_prop_from_exclusive_backlink_01(self):
        # Create new, no source

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromExclusiveBacklink()
        self.client.sync(original)

        self.assertEqual(original.val, None)

    def test_model_sync_computed_single_prop_from_exclusive_backlink_02(self):
        # Create new, source already exists

        from models.TestModelSyncComputedSingleProp import default

        source = default.ExclusiveSource(val=1)
        self.client.save(source)

        original = default.FromExclusiveBacklink()
        source.target = original
        self.client.sync(original, source)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_exclusive_backlink_03(self):
        # Create new, source created alongside object

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromExclusiveBacklink()
        source = default.ExclusiveSource(val=1, target=original)
        self.client.sync(original, source)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_exclusive_backlink_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromExclusiveBacklink())
        mirror = self.client.query_required_single(
            default.FromExclusiveBacklink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_exclusive_backlink_05(self):
        # Update with val set, initially no source

        from models.TestModelSyncComputedSingleProp import default

        source = default.ExclusiveSource(val=9)
        self.client.save(source)

        original = default.FromExclusiveBacklink()
        self.client.sync(original)

        source.target = original
        self.client.sync(original, source)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_exclusive_backlink_06(self):
        # Update with val set, initially no source
        # source val changes

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromExclusiveBacklink()
        source = default.ExclusiveSource(val=1, target=original)
        self.client.sync(original, source)

        source.val = 9
        self.client.sync(original, source)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_exclusive_backlink_07(self):
        # Update with val set, initially has source
        # source changes

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromExclusiveBacklink()
        source_a = default.ExclusiveSource(val=1, target=original)
        self.client.sync(original, source_a)

        source_a.target = None
        self.client.sync(source_a)
        source_b = default.ExclusiveSource(val=9, target=original)
        self.client.sync(original, source_b)

        self.assertEqual(original.val, 10)

    def test_model_sync_computed_single_prop_from_single_backlink_01(self):
        # Create new, no sources

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleBacklink()
        self.client.sync(original)

        self.assertEqual(original.val, 1)

    def test_model_sync_computed_single_prop_from_single_backlink_02(self):
        # Create new, sources already exists

        from models.TestModelSyncComputedSingleProp import default

        source_a = default.SingleSource(val=1)
        source_b = default.SingleSource(val=2)
        source_c = default.SingleSource(val=3)
        self.client.save(source_a, source_b, source_c)

        original = default.FromSingleBacklink()
        source_a.target = original
        source_b.target = original
        source_c.target = original
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, 7)

    def test_model_sync_computed_single_prop_from_single_backlink_03(self):
        # Create new, sources created alongside object

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleBacklink()
        source_a = default.SingleSource(val=1, target=original)
        source_b = default.SingleSource(val=2, target=original)
        source_c = default.SingleSource(val=3, target=original)
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, 7)

    def test_model_sync_computed_single_prop_from_single_backlink_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromSingleBacklink())
        mirror = self.client.query_required_single(
            default.FromSingleBacklink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_single_backlink_05(self):
        # Update with val set, initially no sources

        from models.TestModelSyncComputedSingleProp import default

        source_a = default.SingleSource(val=7)
        source_b = default.SingleSource(val=8)
        source_c = default.SingleSource(val=9)
        self.client.save(source_a, source_b, source_c)

        original = default.FromSingleBacklink()
        self.client.sync(original)

        source_a.target = original
        source_b.target = original
        source_c.target = original
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_single_prop_from_single_backlink_06(self):
        # Update with val set, initially no sources
        # source vals changes

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleBacklink()
        source_a = default.SingleSource(val=1, target=original)
        source_b = default.SingleSource(val=2, target=original)
        source_c = default.SingleSource(val=3, target=original)
        self.client.sync(original, source_a, source_b, source_c)

        source_a.val = 7
        source_b.val = 8
        source_c.val = 9
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_single_prop_from_single_backlink_07(self):
        # Update with val set, initially has sources
        # sources change

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromSingleBacklink()
        source_a = default.SingleSource(val=1)
        source_b = default.SingleSource(val=2)
        source_c = default.SingleSource(val=3)
        self.client.sync(original, source_a, source_b, source_c)

        source_a.target = None
        source_b.target = None
        source_c.target = None
        self.client.sync(source_a, source_b, source_c)
        source_d = default.SingleSource(val=7, target=original)
        source_e = default.SingleSource(val=8, target=original)
        source_f = default.SingleSource(val=9, target=original)
        self.client.sync(original, source_d, source_e, source_f)

        self.assertEqual(original.val, 25)

    def test_model_sync_computed_single_prop_from_stable_expr_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromStableExpr()
        self.client.sync(original)

        self.assertEqual(original.val, 1)

    def test_model_sync_computed_single_prop_from_stable_expr_02(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromStableExpr())
        mirror = self.client.query_required_single(
            default.FromStableExpr.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_stable_expr_03(self):
        # Update with val set, initially None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromStableExpr()
        self.client.sync(original)

        # This increments val by 1
        other = default.FromStableExpr()
        self.client.sync(original, other)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_global_01(self):
        # Create new, global is None

        from models.TestModelSyncComputedSingleProp import default

        original = default.FromGlobal()
        self.client.sync(original)

        self.assertEqual(original.val, None)

    def test_model_sync_computed_single_prop_from_global_02(self):
        # Create new, global has value

        from models.TestModelSyncComputedSingleProp import default

        sess_client = self.client.with_globals({"default::SomeGlobal": 1})
        original = default.FromGlobal()
        sess_client.sync(original)

        self.assertEqual(original.val, 2)

    def test_model_sync_computed_single_prop_from_global_03(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedSingleProp import default

        self.client.sync(default.FromGlobal())
        mirror = self.client.query_required_single(
            default.FromGlobal.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_single_prop_from_global_04(self):
        # Update with val set, initially None

        from models.TestModelSyncComputedSingleProp import default

        sess_client = self.client.with_globals({"default::SomeGlobal": 1})
        original = default.FromGlobal()
        sess_client.sync(original)

        sess_client = self.client.with_globals({"default::SomeGlobal": 9})
        sess_client.sync(original)

        self.assertEqual(original.val, 10)


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


class TestModelSyncComputedMultiProp(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type FromConstant {
            val := {1, 2, 3};
        };

        type FromSingleProp {
            n: int64;
            val := array_unpack(array_fill(9, .n + 1));
        };

        type FromMultiProp {
            multi n: int64;
            val := array_unpack(array_fill(9, sum(.n) + 1));
        };

        type Target {
            val: int64;
        };
        type FromSingleLink {
            target: Target;
            val := array_unpack(array_fill(9, .target.val + 1));
        };
        type FromMultiLink {
            multi target: Target;
            val := array_unpack(array_fill(9, sum(.target.val) + 1));
        };

        type ExclusiveSource {
            target: FromExclusiveBacklink {
                constraint exclusive;
            };
            val: int64;
        };
        type FromExclusiveBacklink {
            val := array_unpack(array_fill(
                9,
                .<target[is ExclusiveSource].val + 1,
            ));
        }

        type SingleSource {
            target: FromSingleBacklink;
            val: int64;
        };
        type FromSingleBacklink {
            val := array_unpack(array_fill(
                9,
                sum(.<target[is SingleSource].val) + 1,
            ));
        }

        type FromStableExpr {
            val := array_unpack(array_fill(9, count(detached FromStableExpr)));
        };

        global SomeGlobal: int64;
        type FromGlobal {
            val := array_unpack(array_fill(9, global SomeGlobal + 1));
        };
    """

    def test_model_sync_computed_multi_prop_constant_01(self):
        # Create new

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromConstant()
        self.client.sync(original)

        self.assertEqual(original.val, (1, 2, 3))

    def test_model_sync_computed_multi_prop_constant_02(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromConstant())
        mirror = self.client.query_required_single(
            default.FromConstant.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_single_prop_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleProp()
        self.client.sync(original)

        self.assertEqual(original.val, ())

    def test_model_sync_computed_multi_prop_from_single_prop_02(self):
        # Create new, expr prop has value

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleProp(n=1)
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_single_prop_03(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromSingleProp())
        mirror = self.client.query_required_single(
            default.FromSingleProp.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_single_prop_04(self):
        # Update with val set, initially None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleProp()
        self.client.sync(original)

        original.n = 9
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_single_prop_05(self):
        # Update with val set, initially not None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleProp(n=1)
        self.client.sync(original)

        original.n = 9
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_multi_prop_01(self):
        # Create new, expr prop is empty

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromMultiProp()
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 1)

    def test_model_sync_computed_multi_prop_from_multi_prop_02(self):
        # Create new, expr prop has values

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromMultiProp(n=[1, 2, 3])
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 7)

    def test_model_sync_computed_multi_prop_from_multi_prop_03(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromMultiProp())
        mirror = self.client.query_required_single(
            default.FromMultiProp.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_multi_prop_04(self):
        # Update with val set, initially empty

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromMultiProp()
        self.client.sync(original)

        original.n = [7, 8, 9]
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_multi_prop_05(self):
        # Update with val set, initially has values

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromMultiProp(n=[1, 2, 3])
        self.client.sync(original)

        original.n = [7, 8, 9]
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_single_link_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleLink()
        self.client.sync(original)

        self.assertEqual(original.val, ())

    def test_model_sync_computed_multi_prop_from_single_link_02(self):
        # Create new, target already exists

        from models.TestModelSyncComputedMultiProp import default

        target = default.Target(val=1)
        self.client.save(target)

        original = default.FromSingleLink(target=target)
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_single_link_03(self):
        # Create new, target created alongside object

        from models.TestModelSyncComputedMultiProp import default

        target = default.Target(val=1)
        original = default.FromSingleLink(target=target)
        self.client.sync(original, target)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_single_link_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromSingleLink())
        mirror = self.client.query_required_single(
            default.FromSingleLink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_single_link_05(self):
        # Update with val set, initially target is None

        from models.TestModelSyncComputedMultiProp import default

        target = default.Target(val=9)
        original = default.FromSingleLink()
        self.client.sync(original, target)

        original.target = target
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_single_link_06(self):
        # Update with val set, initially target is set
        # target val changes

        from models.TestModelSyncComputedMultiProp import default

        target = default.Target(val=1)
        self.client.save(target)

        original = default.FromSingleLink(target=target)
        self.client.sync(original)

        target.val = 9
        self.client.sync(original, target)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_single_link_07(self):
        # Update with val set, initially target is set
        # target changes

        from models.TestModelSyncComputedMultiProp import default

        target_a = default.Target(val=1)
        self.client.save(target_a)

        original = default.FromSingleLink(target=target_a)
        self.client.sync(original)

        target_b = default.Target(val=9)
        original.target = target_b
        self.client.sync(original, target_b)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_multi_link_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromMultiLink()
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 1)

    def test_model_sync_computed_multi_prop_from_multi_link_02(self):
        # Create new, target already exists

        from models.TestModelSyncComputedMultiProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 7)

    def test_model_sync_computed_multi_prop_from_multi_link_03(self):
        # Create new, target created alongside object

        from models.TestModelSyncComputedMultiProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original, target_a, target_b, target_c)

        self.assertEqual(original.val, (9,) * 7)

    def test_model_sync_computed_multi_prop_from_multi_link_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromMultiLink())
        mirror = self.client.query_required_single(
            default.FromMultiLink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_multi_link_05(self):
        # Update with val set, initially target is empty

        from models.TestModelSyncComputedMultiProp import default

        target_a = default.Target(val=7)
        target_b = default.Target(val=8)
        target_c = default.Target(val=9)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink()
        self.client.sync(original)

        original.target = [target_a, target_b, target_c]
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_multi_link_06(self):
        # Update with val set, initially target has values
        # target val changes

        from models.TestModelSyncComputedMultiProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original)

        target_a.val = 7
        target_b.val = 8
        target_c.val = 9
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_multi_link_07(self):
        # Update with val set, initially target has values
        # target changes

        from models.TestModelSyncComputedMultiProp import default

        target_a = default.Target(val=1)
        target_b = default.Target(val=2)
        target_c = default.Target(val=3)
        self.client.save(target_a, target_b, target_c)

        original = default.FromMultiLink(target=[target_a, target_b, target_c])
        self.client.sync(original)

        target_d = default.Target(val=7)
        target_e = default.Target(val=8)
        target_f = default.Target(val=9)
        original.target = [target_d, target_e, target_f]
        self.client.sync(original, target_d, target_e, target_f)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_01(self):
        # Create new, no source

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromExclusiveBacklink()
        self.client.sync(original)

        self.assertEqual(original.val, ())

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_02(self):
        # Create new, source already exists

        from models.TestModelSyncComputedMultiProp import default

        source = default.ExclusiveSource(val=1)
        self.client.save(source)

        original = default.FromExclusiveBacklink()
        source.target = original
        self.client.sync(original, source)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_03(self):
        # Create new, source created alongside object

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromExclusiveBacklink()
        source = default.ExclusiveSource(val=1, target=original)
        self.client.sync(original, source)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromExclusiveBacklink())
        mirror = self.client.query_required_single(
            default.FromExclusiveBacklink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_05(self):
        # Update with val set, initially no source

        from models.TestModelSyncComputedMultiProp import default

        source = default.ExclusiveSource(val=9)
        self.client.save(source)

        original = default.FromExclusiveBacklink()
        self.client.sync(original)

        source.target = original
        self.client.sync(original, source)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_06(self):
        # Update with val set, initially no source
        # source val changes

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromExclusiveBacklink()
        source = default.ExclusiveSource(val=1, target=original)
        self.client.sync(original, source)

        source.val = 9
        self.client.sync(original, source)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_exclusive_backlink_07(self):
        # Update with val set, initially has source
        # source changes

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromExclusiveBacklink()
        source_a = default.ExclusiveSource(val=1, target=original)
        self.client.sync(original, source_a)

        source_a.target = None
        self.client.sync(source_a)
        source_b = default.ExclusiveSource(val=9, target=original)
        self.client.sync(original, source_b)

        self.assertEqual(original.val, (9,) * 10)

    def test_model_sync_computed_multi_prop_from_single_backlink_01(self):
        # Create new, no sources

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleBacklink()
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 1)

    def test_model_sync_computed_multi_prop_from_single_backlink_02(self):
        # Create new, sources already exists

        from models.TestModelSyncComputedMultiProp import default

        source_a = default.SingleSource(val=1)
        source_b = default.SingleSource(val=2)
        source_c = default.SingleSource(val=3)
        self.client.save(source_a, source_b, source_c)

        original = default.FromSingleBacklink()
        source_a.target = original
        source_b.target = original
        source_c.target = original
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, (9,) * 7)

    def test_model_sync_computed_multi_prop_from_single_backlink_03(self):
        # Create new, sources created alongside object

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleBacklink()
        source_a = default.SingleSource(val=1, target=original)
        source_b = default.SingleSource(val=2, target=original)
        source_c = default.SingleSource(val=3, target=original)
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, (9,) * 7)

    def test_model_sync_computed_multi_prop_from_single_backlink_04(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromSingleBacklink())
        mirror = self.client.query_required_single(
            default.FromSingleBacklink.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_single_backlink_05(self):
        # Update with val set, initially no sources

        from models.TestModelSyncComputedMultiProp import default

        source_a = default.SingleSource(val=7)
        source_b = default.SingleSource(val=8)
        source_c = default.SingleSource(val=9)
        self.client.save(source_a, source_b, source_c)

        original = default.FromSingleBacklink()
        self.client.sync(original)

        source_a.target = original
        source_b.target = original
        source_c.target = original
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_single_backlink_06(self):
        # Update with val set, initially no sources
        # source vals changes

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleBacklink()
        source_a = default.SingleSource(val=1, target=original)
        source_b = default.SingleSource(val=2, target=original)
        source_c = default.SingleSource(val=3, target=original)
        self.client.sync(original, source_a, source_b, source_c)

        source_a.val = 7
        source_b.val = 8
        source_c.val = 9
        self.client.sync(original, source_a, source_b, source_c)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_single_backlink_07(self):
        # Update with val set, initially has sources
        # sources change

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromSingleBacklink()
        source_a = default.SingleSource(val=1)
        source_b = default.SingleSource(val=2)
        source_c = default.SingleSource(val=3)
        self.client.sync(original, source_a, source_b, source_c)

        source_a.target = None
        source_b.target = None
        source_c.target = None
        self.client.sync(source_a, source_b, source_c)
        source_d = default.SingleSource(val=7, target=original)
        source_e = default.SingleSource(val=8, target=original)
        source_f = default.SingleSource(val=9, target=original)
        self.client.sync(original, source_d, source_e, source_f)

        self.assertEqual(original.val, (9,) * 25)

    def test_model_sync_computed_multi_prop_from_stable_expr_01(self):
        # Create new, expr prop is None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromStableExpr()
        self.client.sync(original)

        self.assertEqual(original.val, (9,) * 1)

    def test_model_sync_computed_multi_prop_from_stable_expr_02(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromStableExpr())
        mirror = self.client.query_required_single(
            default.FromStableExpr.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_stable_expr_03(self):
        # Update with val set, initially None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromStableExpr()
        self.client.sync(original)

        # This increments val by 1
        other = default.FromStableExpr()
        self.client.sync(original, other)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_global_01(self):
        # Create new, global is None

        from models.TestModelSyncComputedMultiProp import default

        original = default.FromGlobal()
        self.client.sync(original)

        self.assertEqual(original.val, ())

    def test_model_sync_computed_multi_prop_from_global_02(self):
        # Create new, global has value

        from models.TestModelSyncComputedMultiProp import default

        sess_client = self.client.with_globals({"default::SomeGlobal": 1})
        original = default.FromGlobal()
        sess_client.sync(original)

        self.assertEqual(original.val, (9,) * 2)

    def test_model_sync_computed_multi_prop_from_global_03(self):
        # Update without val set does not fetch it

        from models.TestModelSyncComputedMultiProp import default

        self.client.sync(default.FromGlobal())
        mirror = self.client.query_required_single(
            default.FromGlobal.select(val=False).limit(1)
        )
        self.client.sync(mirror)

        self.assertFalse(hasattr(mirror, 'val'))

    def test_model_sync_computed_multi_prop_from_global_04(self):
        # Update with val set, initially None

        from models.TestModelSyncComputedMultiProp import default

        sess_client = self.client.with_globals({"default::SomeGlobal": 1})
        original = default.FromGlobal()
        sess_client.sync(original)

        sess_client = self.client.with_globals({"default::SomeGlobal": 9})
        sess_client.sync(original)

        self.assertEqual(original.val, (9,) * 10)


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

        _testcase(default.SourceWithProp, None)
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target, lprop=1),
        )

    def test_model_sync_single_link_02(self):
        # Updating existing objects with single link

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

        from models.TestModelSyncSingleLink import default

        target_a = default.Target()
        target_b = default.Target()
        self.client.save(target_a, target_b)

        # Change to/from None
        _testcase(default.Source, None, target_b)
        _testcase(default.Source, target_a, None)

        _testcase(
            default.SourceWithProp,
            None,
            default.SourceWithProp.target.link(target_b),
        )
        _testcase(
            default.SourceWithProp,
            None,
            default.SourceWithProp.target.link(target_b, lprop=1),
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a),
            None,
        )
        _testcase(
            default.SourceWithProp,
            default.SourceWithProp.target.link(target_a, lprop=1),
            None,
        )

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
        _testcase(default.Source, None, None)
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


class TestModelSyncMultiLink(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type Target;
        type Source {
            multi targets: Target;
        };
        type SourceWithProp {
            multi targets: Target {
                lprop: int64;
            };
        };
    """

    def _check_multilinks_equal(
        self,
        actual: typing.Collection[typing.Any],
        expected: typing.Collection[typing.Any],
    ) -> None:
        self.assertEqual(actual, expected)

        # Also check linkprops
        if isinstance(actual, _link_set.LinkWithPropsSet):
            expected_lprops = {e.id: e.__linkprops__.lprop for e in expected}
            for a in actual:
                self.assertEqual(a.__linkprops__.lprop, expected_lprops[a.id])

    def _base_testcase(
        self,
        model_type: typing.Type[GelModel],
        initial_targets: typing.Collection[typing.Any],
        change_original: typing.Callable[[GelModel], None],
        expected_targets: typing.Collection[typing.Any],
    ) -> None:
        expected_targets = set(expected_targets)

        original = model_type(targets=initial_targets)
        self.client.save(original)

        self._check_multilinks_equal(original.targets, initial_targets)

        # change a value
        change_original(original)

        # sync some of the objects
        self.client.sync(original)

        # only synced objects with value set get update
        self._check_multilinks_equal(original.targets, expected_targets)

        # cleanup
        self.client.query(model_type.delete())

    def test_model_sync_multi_link_01(self):
        # Insert new object with multi link

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        self.client.save(target_a, target_b, target_c)

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_targets: typing.Collection[typing.Any],
        ) -> None:
            with_targets = model_type(targets=initial_targets)
            without_targets = model_type()

            self.client.sync(with_targets, without_targets)

            self._check_multilinks_equal(with_targets.targets, initial_targets)
            self._check_multilinks_equal(without_targets.targets, [])

            # cleanup
            self.client.query(model_type.delete())

        # No linkprops
        _testcase(default.Source, [])
        _testcase(default.Source, [target_a, target_b, target_c])

        # With linkprops
        _testcase(default.SourceWithProp, [])
        _testcase(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
        )
        _testcase(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
        )

    def _get_assign_targets_func(
        self, changed_targets: typing.Collection[typing.Any]
    ) -> typing.Callable[[GelModel], None]:
        def change(original: GelModel):
            original.targets = changed_targets

        return change

    def _testcase_assign(
        self,
        model_type: typing.Type[GelModel],
        initial_targets: typing.Collection[typing.Any],
        changed_targets: typing.Collection[typing.Any],
    ) -> None:
        self._base_testcase(
            model_type,
            initial_targets,
            self._get_assign_targets_func(changed_targets),
            changed_targets,
        )

    def test_model_sync_multi_link_02(self):
        # Updating existing objects with multi link
        # Set links to new value

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # No linkprops
        self._testcase_assign(default.Source, [], [])
        self._testcase_assign(
            default.Source,
            [],
            [target_a, target_b, target_c],
        )

        self._testcase_assign(
            default.Source,
            [target_a, target_b, target_c],
            [],
        )
        self._testcase_assign(
            default.Source,
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
        )

        self._testcase_assign(
            default.Source,
            [target_a, target_b],
            [target_c, target_d],
        )
        self._testcase_assign(
            default.Source,
            [target_a, target_b],
            [target_a, target_b, target_c, target_d],
        )
        self._testcase_assign(
            default.Source,
            [target_a, target_b, target_c, target_d],
            [target_a, target_b],
        )

        self._testcase_assign(
            default.Source,
            [target_a, target_b, target_c],
            [target_c, target_d],
        )

        # With linkprops
        self._testcase_assign(default.SourceWithProp, [], [])
        self._testcase_assign(
            default.SourceWithProp,
            [],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
        )

        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
        )

        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [],
        )
        # Fail, moved to test_model_sync_multi_link_02a
        # self._testcase_assign(
        #     default.SourceWithProp,
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b, lprop=2),
        #         default.SourceWithProp.targets.link(target_c, lprop=3),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a),
        #         default.SourceWithProp.targets.link(target_b),
        #         default.SourceWithProp.targets.link(target_c),
        #     ],
        # )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=4),
                default.SourceWithProp.targets.link(target_b, lprop=5),
                default.SourceWithProp.targets.link(target_c, lprop=6),
            ],
        )

        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_02a(self):
        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # Fail, linkprop not reset
        self._testcase_assign(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
        )

    def test_model_sync_multi_link_03(self):
        # Updating existing objects with multi props
        # LinkSet clear

        def _get_clear_targets_func() -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.targets.clear()

            return change

        def _testcase_clear(
            model_type: typing.Type[GelModel],
            initial_targets: typing.Collection[typing.Any],
        ) -> None:
            self._base_testcase(
                model_type,
                initial_targets,
                _get_clear_targets_func(),
                [],
            )

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        self.client.save(target_a, target_b, target_c)

        # No linkprops
        _testcase_clear(default.Source, [])
        _testcase_clear(
            default.Source,
            [target_a, target_b, target_c],
        )

        # With linkprops
        _testcase_clear(default.SourceWithProp, [])
        _testcase_clear(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
        )
        _testcase_clear(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
        )

    def _get_update_targets_func(
        self,
        update_targets: typing.Collection[typing.Any],
    ) -> typing.Callable[[GelModel], None]:
        def change(original: GelModel):
            original.targets.update(update_targets)

        return change

    def _testcase_update(
        self,
        model_type: typing.Type[GelModel],
        initial_targets: typing.Collection[typing.Any],
        update_targets: typing.Collection[typing.Any],
        expected_targets: typing.Collection[typing.Any],
    ) -> None:
        self._base_testcase(
            model_type,
            initial_targets,
            self._get_update_targets_func(update_targets),
            expected_targets,
        )

    def test_model_sync_multi_link_04(self):
        # Updating existing objects with multi props
        # LinkSet update

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # No linkprops
        self._testcase_update(default.Source, [], [], [])
        self._testcase_update(
            default.Source,
            [],
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
        )

        self._testcase_update(
            default.Source,
            [target_a, target_b, target_c],
            [],
            [target_a, target_b, target_c],
        )
        self._testcase_update(
            default.Source,
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
        )

        self._testcase_update(
            default.Source,
            [target_a, target_b],
            [target_c, target_d],
            [target_a, target_b, target_c, target_d],
        )
        self._testcase_update(
            default.Source,
            [target_a, target_b],
            [target_a, target_b, target_c, target_d],
            [target_a, target_b, target_c, target_d],
        )
        self._testcase_update(
            default.Source,
            [target_a, target_b, target_c, target_d],
            [target_a, target_b],
            [target_a, target_b, target_c, target_d],
        )

        self._testcase_update(
            default.Source,
            [target_a, target_b, target_c],
            [target_c, target_d],
            [target_a, target_b, target_c, target_d],
        )

        # With linkprops
        self._testcase_update(default.SourceWithProp, [], [], [])
        self._testcase_update(
            default.SourceWithProp,
            [],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                # doesn't work without lprop=None ?!
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        # Fail, moved to test_model_sync_multi_link_04a
        # self._testcase_update(
        #     default.SourceWithProp,
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b, lprop=2),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a),
        #         default.SourceWithProp.targets.link(target_b),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a),
        #         default.SourceWithProp.targets.link(target_b),
        #     ],
        # )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=4),
                default.SourceWithProp.targets.link(target_b, lprop=5),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=4),
                default.SourceWithProp.targets.link(target_b, lprop=5),
            ],
        )

        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        # Fail, moved to test_model_sync_multi_link_04b
        # self._testcase_update(
        #     default.SourceWithProp,
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b, lprop=2),
        #         default.SourceWithProp.targets.link(target_c, lprop=3),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_b),
        #         default.SourceWithProp.targets.link(target_c),
        #         default.SourceWithProp.targets.link(target_d),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b),
        #         default.SourceWithProp.targets.link(target_c),
        #         default.SourceWithProp.targets.link(target_d),
        #     ],
        # )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
        )
        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_04a(self):
        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                # Fail, linkprops not reset
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_04b(self):
        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        self._testcase_update(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                # Fail, linkprops not reset
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )

    def _get_add_targets_func(
        self,
        add_target: typing.Any,
    ) -> typing.Callable[[GelModel], None]:
        def change(original: GelModel):
            original.targets.add(add_target)

        return change

    def _testcase_add(
        self,
        model_type: typing.Type[GelModel],
        initial_targets: typing.Collection[typing.Any],
        add_target: typing.Any,
        expected_targets: typing.Collection[typing.Any],
    ) -> None:
        self._base_testcase(
            model_type,
            initial_targets,
            self._get_add_targets_func(add_target),
            expected_targets,
        )

    def test_model_sync_multi_link_05(self):
        # Updating existing objects with multi props
        # LinkSet add

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # No linkprops
        self._testcase_add(
            default.Source,
            [],
            target_a,
            [target_a],
        )
        self._testcase_add(
            default.Source,
            [target_a],
            target_a,
            [target_a],
        )
        self._testcase_add(
            default.Source,
            [target_a, target_b, target_c],
            target_d,
            [target_a, target_b, target_c, target_d],
        )

        # With linkprops
        self._testcase_add(
            default.SourceWithProp,
            [],
            default.SourceWithProp.targets.link(target_a),
            [default.SourceWithProp.targets.link(target_a)],
        )
        self._testcase_add(
            default.SourceWithProp,
            [],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
        )

        self._testcase_add(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            default.SourceWithProp.targets.link(target_a),
            [default.SourceWithProp.targets.link(target_a)],
        )
        self._testcase_add(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
        )

        # Fail, moved to test_model_sync_multi_link_05a
        # self._testcase_add(
        #     default.SourceWithProp,
        #     [default.SourceWithProp.targets.link(target_a, lprop=1)],
        #     default.SourceWithProp.targets.link(target_a),
        #     [default.SourceWithProp.targets.link(target_a)],
        # )
        self._testcase_add(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
        )
        self._testcase_add(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a, lprop=2),
            [default.SourceWithProp.targets.link(target_a, lprop=2)],
        )

        self._testcase_add(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_d),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_add(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_d, lprop=4),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d, lprop=4),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_05a(self):
        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # Fail, linkprop not reset
        self._testcase_add(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a),
            [default.SourceWithProp.targets.link(target_a)],
        )

    def test_model_sync_multi_link_06(self):
        # Updating existing objects with multi props
        # LinkSet discard

        def _get_discard_targets_func(
            discard_target: typing.Any,
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.targets.discard(discard_target)

            return change

        def _testcase_discard(
            model_type: typing.Type[GelModel],
            initial_targets: typing.Collection[typing.Any],
            discard_target: typing.Any,
            expected_targets: typing.Collection[typing.Any],
        ) -> None:
            self._base_testcase(
                model_type,
                initial_targets,
                _get_discard_targets_func(discard_target),
                expected_targets,
            )

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # No linkprops
        _testcase_discard(
            default.Source,
            [target_a],
            target_a,
            [],
        )
        _testcase_discard(
            default.Source,
            [target_a, target_b, target_c],
            target_c,
            [target_a, target_b],
        )

        # With linkprops
        _testcase_discard(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            default.SourceWithProp.targets.link(target_a),
            [],
        )
        _testcase_discard(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [],
        )

        _testcase_discard(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a),
            [],
        )
        _testcase_discard(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [],
        )
        _testcase_discard(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a, lprop=2),
            [],
        )

        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_c),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_c, lprop=3),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_c),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_c, lprop=3),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_c, lprop=4),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

        # Discarding non-member items does nothing
        _testcase_discard(
            default.Source,
            [target_a, target_b, target_c],
            target_d,
            [target_a, target_b, target_c],
        )
        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_d),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
        )
        _testcase_discard(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_d),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
        )

    def test_model_sync_multi_link_07(self):
        # Updating existing objects with multi props
        # LinkSet remove

        def _get_remove_targets_func(
            remove_target: typing.Any,
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.targets.remove(remove_target)

            return change

        def _testcase_remove(
            model_type: typing.Type[GelModel],
            initial_targets: typing.Collection[typing.Any],
            remove_target: typing.Any,
            expected_targets: typing.Collection[typing.Any],
        ) -> None:
            self._base_testcase(
                model_type,
                initial_targets,
                _get_remove_targets_func(remove_target),
                expected_targets,
            )

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        self.client.save(target_a, target_b, target_c)

        # No linkprops
        _testcase_remove(
            default.Source,
            [target_a],
            target_a,
            [],
        )
        _testcase_remove(
            default.Source,
            [target_a, target_b, target_c],
            target_c,
            [target_a, target_b],
        )

        # With linkprops
        _testcase_remove(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            default.SourceWithProp.targets.link(target_a),
            [],
        )
        _testcase_remove(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [],
        )

        _testcase_remove(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a),
            [],
        )
        _testcase_remove(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a, lprop=1),
            [],
        )
        _testcase_remove(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            default.SourceWithProp.targets.link(target_a, lprop=2),
            [],
        )

        _testcase_remove(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_c),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        _testcase_remove(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            default.SourceWithProp.targets.link(target_c, lprop=3),
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        _testcase_remove(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_c),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        _testcase_remove(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_c, lprop=3),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        _testcase_remove(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            default.SourceWithProp.targets.link(target_c, lprop=4),
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

    def _get_op_iadd_targets_func(
        self,
        op_iadd_targets: typing.Collection[typing.Any],
    ) -> typing.Callable[[GelModel], None]:
        def change(original: GelModel):
            original.targets += op_iadd_targets

        return change

    def _testcase_op_iadd(
        self,
        model_type: typing.Type[GelModel],
        initial_targets: typing.Collection[typing.Any],
        op_iadd_targets: typing.Collection[typing.Any],
        expected_targets: typing.Collection[typing.Any],
    ) -> None:
        self._base_testcase(
            model_type,
            initial_targets,
            self._get_op_iadd_targets_func(op_iadd_targets),
            expected_targets,
        )

    def test_model_sync_multi_link_08(self):
        # Updating existing objects with multi props
        # LinkSet operator iadd

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # No linkprops
        self._testcase_op_iadd(default.Source, [], [], [])
        self._testcase_op_iadd(
            default.Source,
            [],
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
        )

        self._testcase_op_iadd(
            default.Source,
            [target_a, target_b, target_c],
            [],
            [target_a, target_b, target_c],
        )
        self._testcase_op_iadd(
            default.Source,
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
            [target_a, target_b, target_c],
        )

        self._testcase_op_iadd(
            default.Source,
            [target_a, target_b],
            [target_c, target_d],
            [target_a, target_b, target_c, target_d],
        )
        self._testcase_op_iadd(
            default.Source,
            [target_a, target_b],
            [target_a, target_b, target_c, target_d],
            [target_a, target_b, target_c, target_d],
        )
        self._testcase_op_iadd(
            default.Source,
            [target_a, target_b, target_c, target_d],
            [target_a, target_b],
            [target_a, target_b, target_c, target_d],
        )

        self._testcase_op_iadd(
            default.Source,
            [target_a, target_b, target_c],
            [target_c, target_d],
            [target_a, target_b, target_c, target_d],
        )

        # With linkprops
        self._testcase_op_iadd(default.SourceWithProp, [], [], [])
        self._testcase_op_iadd(
            default.SourceWithProp,
            [],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                # doesn't work without lprop=None ?!
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        # Fail, moved to test_model_sync_multi_link_08a
        # self._testcase_op_iadd(
        #     default.SourceWithProp,
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b, lprop=2),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a),
        #         default.SourceWithProp.targets.link(target_b),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a),
        #         default.SourceWithProp.targets.link(target_b),
        #     ],
        # )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=4),
                default.SourceWithProp.targets.link(target_b, lprop=5),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=4),
                default.SourceWithProp.targets.link(target_b, lprop=5),
            ],
        )

        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )
        # Fail, moved to test_model_sync_multi_link_08b
        # self._testcase_op_iadd(
        #     default.SourceWithProp,
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b, lprop=2),
        #         default.SourceWithProp.targets.link(target_c, lprop=3),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_b),
        #         default.SourceWithProp.targets.link(target_c),
        #         default.SourceWithProp.targets.link(target_d),
        #     ],
        #     [
        #         default.SourceWithProp.targets.link(target_a, lprop=1),
        #         default.SourceWithProp.targets.link(target_b),
        #         default.SourceWithProp.targets.link(target_c),
        #         default.SourceWithProp.targets.link(target_d),
        #     ],
        # )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
        )
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=4),
                default.SourceWithProp.targets.link(target_c, lprop=5),
                default.SourceWithProp.targets.link(target_d, lprop=6),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_08a(self):
        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # Fail, linkprop not reset
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_08b(self):
        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # Fail, linkprop not reset
        self._testcase_op_iadd(
            default.SourceWithProp,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
        )

    def test_model_sync_multi_link_09(self):
        # Updating existing objects with multi props
        # LinkSet operator isub

        def _get_op_isub_targets_func(
            op_isub_targets: typing.Collection[typing.Any],
        ) -> typing.Callable[[GelModel], None]:
            def change(original: GelModel):
                original.targets -= op_isub_targets

            return change

        def _testcase_op_isub(
            model_type: typing.Type[GelModel],
            initial_targets: typing.Collection[typing.Any],
            op_isub_targets: typing.Collection[typing.Any],
            expected_targets: typing.Collection[typing.Any],
        ) -> None:
            self._base_testcase(
                model_type,
                initial_targets,
                _get_op_isub_targets_func(op_isub_targets),
                expected_targets,
            )

        from models.TestModelSyncMultiLink import default

        target_a = default.Target()
        target_b = default.Target()
        target_c = default.Target()
        target_d = default.Target()
        self.client.save(target_a, target_b, target_c, target_d)

        # No linkprops
        _testcase_op_isub(default.Source, [], [], [])
        _testcase_op_isub(
            default.Source,
            [],
            [target_a],
            [],
        )

        _testcase_op_isub(
            default.Source,
            [target_a],
            [],
            [target_a],
        )
        _testcase_op_isub(
            default.Source,
            [target_a],
            [target_a],
            [],
        )
        _testcase_op_isub(
            default.Source,
            [target_a],
            [target_b],
            [target_a],
        )

        _testcase_op_isub(
            default.Source,
            [target_a, target_b, target_c],
            [target_c, target_d],
            [target_a, target_b],
        )

        # With linkprops
        _testcase_op_isub(default.SourceWithProp, [], [], [])
        _testcase_op_isub(
            default.SourceWithProp,
            [],
            [default.SourceWithProp.targets.link(target_a)],
            [],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [],
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [],
        )

        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            [],
            [default.SourceWithProp.targets.link(target_a)],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            [default.SourceWithProp.targets.link(target_a)],
            [],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a)],
            [default.SourceWithProp.targets.link(target_b)],
            [default.SourceWithProp.targets.link(target_a)],
        )

        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [],
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [default.SourceWithProp.targets.link(target_a)],
            [],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [default.SourceWithProp.targets.link(target_a, lprop=2)],
            [],
        )
        _testcase_op_isub(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
            [default.SourceWithProp.targets.link(target_b)],
            [default.SourceWithProp.targets.link(target_a, lprop=1)],
        )

        _testcase_op_isub(
            default.Source,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )
        _testcase_op_isub(
            default.Source,
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
                default.SourceWithProp.targets.link(target_c),
            ],
            [
                default.SourceWithProp.targets.link(target_c, lprop=3),
                default.SourceWithProp.targets.link(target_d, lprop=4),
            ],
            [
                default.SourceWithProp.targets.link(target_a),
                default.SourceWithProp.targets.link(target_b),
            ],
        )

        _testcase_op_isub(
            default.Source,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_c),
                default.SourceWithProp.targets.link(target_d),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        _testcase_op_isub(
            default.Source,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_c, lprop=3),
                default.SourceWithProp.targets.link(target_d, lprop=4),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )
        _testcase_op_isub(
            default.Source,
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
                default.SourceWithProp.targets.link(target_c, lprop=3),
            ],
            [
                default.SourceWithProp.targets.link(target_c, lprop=9),
                default.SourceWithProp.targets.link(target_d, lprop=4),
            ],
            [
                default.SourceWithProp.targets.link(target_a, lprop=1),
                default.SourceWithProp.targets.link(target_b, lprop=2),
            ],
        )

    @tb.xfail
    def test_model_sync_multi_link_10(self):
        # Existing object without link should not have it fetched
        # No linkprops

        def _testcase(
            model_type: typing.Type[GelModel],
            initial_targets: typing.Any,
            changed_targets_0: typing.Any,
            changed_targets_1: typing.Any,
            changed_targets_2: typing.Any,
        ):
            original = model_type(targets=initial_targets)
            self.client.save(original)

            mirror_1 = self.client.query_required_single(
                model_type.select(targets=False).limit(1)
            )
            original.targets = changed_targets_0
            self.client.save(original)
            self.client.sync(mirror_1)
            self.assertEqual(mirror_1.targets._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_1.targets._items, [])

            # Sync alongside another object with the prop set
            mirror_2 = self.client.query_required_single(
                model_type.select(targets=True).limit(1)
            )
            original.targets = changed_targets_1
            self.client.save(original)
            self.client.sync(mirror_1, mirror_2)
            self.assertEqual(mirror_1.targets._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_1.targets._items, [])

            # Sync alongside another object with the prop changed
            mirror_2 = self.client.query_required_single(
                model_type.select(targets=True).limit(1)
            )
            mirror_2.targets = changed_targets_2
            self.client.sync(mirror_1, mirror_2)
            self.assertEqual(mirror_1.targets._mode, _tracked_list.Mode.Write)
            self.assertEqual(mirror_1.targets._items, [])  # Fail

            # cleanup
            self.client.query(model_type.delete())

        from models.TestModelSyncMultiLink import default

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

        _testcase(
            default.Source,
            [initial_target],
            [changed_target_0],
            [changed_target_1],
            [changed_target_2],
        )
        _testcase(
            default.SourceWithProp,
            [default.SourceWithProp.targets.link(initial_target)],
            [default.SourceWithProp.targets.link(changed_target_0)],
            [default.SourceWithProp.targets.link(changed_target_1)],
            [default.SourceWithProp.targets.link(changed_target_2)],
        )


class TestModelSyncRewrite(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type SingleProp {
            n: int64;
            dummy: int64;
            val: int64 {
                rewrite insert using (__subject__.n + 1);
                rewrite update using (__subject__.n + 2);
            };
        };

        type Target {
            n: int64;
        };
        type SingleLink {
            n: int64;
            dummy: int64;
            target: Target {
                rewrite insert using ((
                    insert Target { n := __subject__.n + 1 }
                ));
                rewrite update using ((
                    insert Target { n := __subject__.n + 2 }
                ));
            };
        };
    """

    def test_model_sync_rewrite_insert_01(self):
        # Insert, property with rewrite

        from models.TestModelSyncRewrite import default

        def _testcase(
            insert_n: int | None,
            insert_val: int | None,
            expected_val: int | None,
        ) -> None:
            original = default.SingleProp(n=insert_n, val=insert_val)
            self.client.sync(original)

            self.assertEqual(original.val, expected_val)

            # cleanup
            self.client.query(default.SingleProp.delete())

        _testcase(None, None, None)
        _testcase(1, None, 2)
        _testcase(1, 0, 2)

    @tb.xfail  # rewritten link is not refetched
    def test_model_sync_rewrite_insert_02(self):
        # Insert, link with rewrite

        from models.TestModelSyncRewrite import default

        def _testcase(
            insert_n: int | None,
            insert_target: default.Target | None,
            expected_val: int | None,
        ) -> None:
            original = default.SingleLink(n=insert_n, target=insert_target)
            self.client.sync(original)

            self.assertNotEqual(original.target, insert_target)
            assert original.target is not None
            self.assertEqual(original.target.n, expected_val)

            # cleanup
            self.client.query(default.SingleLink.delete())

        target_zero = default.Target(n=0)
        self.client.save(target_zero)

        _testcase(None, None, None)
        _testcase(1, None, 2)
        _testcase(1, target_zero, 2)

    def test_model_sync_rewrite_update_01(self):
        # Update, property with rewrite
        # Only update the rewrite field

        from models.TestModelSyncRewrite import default

        def _testcase(
            insert_n: int | None,
            update_val: int | None,
            expected_val: int | None,
        ) -> None:
            original = default.SingleProp(n=insert_n)
            self.client.sync(original)

            original.val = update_val
            original.dummy = 1  # Change some other prop in parallel
            self.client.sync(original)

            self.assertEqual(original.val, expected_val)

            # cleanup
            self.client.query(default.SingleProp.delete())

        _testcase(None, None, None)
        _testcase(None, 1, None)
        _testcase(1, None, 3)
        _testcase(1, 1, 3)
        _testcase(1, 9, 3)

    def test_model_sync_rewrite_update_02(self):
        # Update, property with rewrite
        # Only update other field

        from models.TestModelSyncRewrite import default

        def _testcase(
            insert_n: int | None,
            update_n: int | None,
            expected_val: int | None,
        ) -> None:
            original = default.SingleProp(n=insert_n)
            self.client.sync(original)

            original.n = update_n
            original.dummy = 1  # Change some other prop in parallel
            self.client.sync(original)

            self.assertEqual(original.val, expected_val)

            # cleanup
            self.client.query(default.SingleProp.delete())

        _testcase(None, None, None)
        _testcase(None, 1, 3)
        _testcase(1, None, None)
        _testcase(1, 1, 3)
        _testcase(1, 9, 11)

    @tb.xfail  # rewritten link is not refetched
    def test_model_sync_rewrite_update_03(self):
        # Update, link with rewrite
        # Only update the rewrite field

        from models.TestModelSyncRewrite import default

        # Initialize all links to not None, because sync currently breaks
        # otherwise.
        target_zero = default.Target(n=0)
        self.client.save(target_zero)

        def _testcase(
            insert_n: int | None,
            update_target: default.Target | None,
            expected_val: int | None,
        ) -> None:
            original = default.SingleLink(n=insert_n, target=target_zero)
            self.client.sync(original)

            insert_target = original.target

            original.target = update_target
            original.dummy = 1  # Change some other prop in parallel
            self.client.sync(original)

            self.assertNotEqual(original.target, insert_target)
            self.assertNotEqual(original.target, update_target)
            assert original.target is not None
            self.assertEqual(original.target.n, expected_val)

            # cleanup
            self.client.query(default.SingleLink.delete())

        target_one = default.Target(n=1)
        self.client.save(target_one)

        _testcase(1, None, 3)
        _testcase(1, target_one, 3)

    @tb.xfail  # rewritten link is not refetched
    def test_model_sync_rewrite_update_04(self):
        # Update, link with rewrite
        # Only update other field

        from models.TestModelSyncRewrite import default

        # Initialize all links to not None, because sync currently breaks
        # otherwise.
        target_zero = default.Target(n=0)
        self.client.save(target_zero)

        def _testcase(
            insert_n: int | None,
            update_n: int | None,
            expected_val: int | None,
        ) -> None:
            original = default.SingleLink(n=insert_n, target=target_zero)
            self.client.sync(original)

            insert_target = original.target

            original.n = update_n
            original.dummy = 1  # Change some other prop in parallel
            self.client.sync(original)

            self.assertNotEqual(original.target, insert_target)
            assert original.target is not None
            self.assertEqual(original.target.n, expected_val)

            # cleanup
            self.client.query(default.SingleLink.delete())

        _testcase(None, None, None)
        _testcase(1, None, None)
        _testcase(1, 1, 3)
        _testcase(1, 9, 11)


class TestModelSyncTrigger(tb.ModelTestCase):
    ISOLATED_TEST_BRANCHES = True

    SCHEMA = """
        type TestInsert {
            val: int64;
        };
        type TriggerInsert {
            n: int64;
            trigger update_target after insert
            for each do (
                update TestInsert
                set {
                    val := .val + __new__.n
                }
            );
        };
        type TriggerInsertComputed {
            n: int64;
            trigger update_target after insert
            for each do (
                update TestInsert
                set {
                    val := .val + __new__.n
                }
            );

            test_vals := TestInsert.val;
        };
        type TriggerInsertWithLink {
            n: int64;
            test: TestInsert;
            trigger update_target after insert
            for each do (
                update TestInsert
                set {
                    val := .val + __new__.n
                }
            );
        };

        type TestUpdate {
            val: int64;
        };
        type TriggerUpdate {
            n: int64;
            trigger update_target after update
            for each do (
                update TestUpdate
                set {
                    val := .val + __new__.n
                }
            );
        };
        type TriggerUpdateComputed {
            n: int64;
            trigger update_target after update
            for each do (
                update TestUpdate
                set {
                    val := .val + __new__.n
                }
            );

            test_vals := TestUpdate.val;
        };
    """

    def test_model_sync_trigger_insert_01(self):
        # Insert trigger, basic
        from models.TestModelSyncTrigger import default

        test_obj = default.TestInsert(val=0)
        trigger_a = default.TriggerInsert(n=1)
        self.client.sync(test_obj, trigger_a)
        self.assertEqual(test_obj.val, 1)

        trigger_b = default.TriggerInsert(n=2)
        self.client.sync(test_obj, trigger_b)
        self.assertEqual(test_obj.val, 3)

        trigger_c = default.TriggerInsert(n=3)
        self.client.sync(test_obj, trigger_c)
        self.assertEqual(test_obj.val, 6)

    def test_model_sync_trigger_insert_02(self):
        # Insert trigger, computed modified by trigger
        from models.TestModelSyncTrigger import default

        test_obj = default.TestInsert(val=0)
        trigger = default.TriggerInsertComputed(n=1)
        self.client.sync(test_obj, trigger)

        self.assertEqual(trigger.test_vals, (1,))

    def test_model_sync_trigger_insert_03(self):
        # Insert trigger
        # Link will cause test objs to be batched before trigger objs
        from models.TestModelSyncTrigger import default

        test_obj = default.TestInsert(val=0)
        trigger_a = default.TriggerInsertWithLink(n=1, test=test_obj)
        self.client.sync(test_obj, trigger_a)
        self.assertEqual(test_obj.val, 1)

        trigger_b = default.TriggerInsertWithLink(n=2, test=test_obj)
        self.client.sync(test_obj, trigger_b)
        self.assertEqual(test_obj.val, 3)

        trigger_c = default.TriggerInsertWithLink(n=3, test=test_obj)
        self.client.sync(test_obj, trigger_c)
        self.assertEqual(test_obj.val, 6)

    def test_model_sync_trigger_update_01(self):
        from models.TestModelSyncTrigger import default

        test_obj = default.TestUpdate(val=0)
        trigger = default.TriggerUpdate(n=0)
        self.client.sync(test_obj, trigger)

        trigger.n = 1
        self.client.sync(test_obj, trigger)
        self.assertEqual(test_obj.val, 1)

        trigger.n = 2
        self.client.sync(test_obj, trigger)
        self.assertEqual(test_obj.val, 3)

        trigger.n = 3
        self.client.sync(test_obj, trigger)
        self.assertEqual(test_obj.val, 6)

    def test_model_sync_trigger_update_02(self):
        # Update trigger, computed modified by trigger
        from models.TestModelSyncTrigger import default

        test_obj = default.TestUpdate(val=0)
        trigger = default.TriggerUpdateComputed(n=0)
        self.client.sync(test_obj, trigger)

        trigger.n = 1
        self.client.sync(test_obj, trigger)
        self.assertEqual(trigger.test_vals, (1,))
