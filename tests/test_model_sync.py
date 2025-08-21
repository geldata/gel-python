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

import os

from gel import _testbase as tb


class TestModelSync(tb.ModelTestCase):
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
