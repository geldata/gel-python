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

import typing
import dataclasses

import os

from gel import _testbase as tb
from gel.blocking_client import BatchIteration
from gel._internal._qbmodel._pydantic import GelModel
from gel._internal._save import (
    ChangeBatch,
    QueryBatch,
    QueryRefetch,
    SaveExecutor,
)


@dataclasses.dataclass
class ProfilingSaveExecutor(SaveExecutor):
    batch_queries: list[list[QueryBatch]] = dataclasses.field(
        default_factory=list
    )
    refetch_queries: list[list[QueryRefetch]] = dataclasses.field(
        default_factory=list
    )

    def _compile_batch(
        self, batch: ChangeBatch, /, *, for_insert: bool
    ) -> list[QueryBatch]:
        compiled_batch = super()._compile_batch(batch, for_insert=for_insert)
        self.batch_queries.append(compiled_batch)
        return compiled_batch

    def get_refetch_queries(
        self,
    ) -> list[QueryRefetch]:
        refetch_query = super().get_refetch_queries()
        self.refetch_queries.append(refetch_query)
        return refetch_query


class ProfilingTestClient(tb.TestClient):
    __slots__ = ("executors", "batch_queries", "refetch_queries")
    _save_executor_type = ProfilingSaveExecutor

    executors: list[ProfilingSaveExecutor]
    batch_queries: list[QueryBatch]
    refetch_queries: list[QueryRefetch]

    def __init__(self, **kwargs: typing.Any):
        super().__init__(**kwargs)
        self.executors = []
        self.batch_queries = []
        self.refetch_queries = []

    def _get_make_save_executor(
        self,
        *,
        refetch: bool,
        objs: tuple[GelModel, ...],
        warn_on_large_sync_set: bool = False,
    ) -> typing.Callable[[], SaveExecutor]:
        """Clear executors and return a save executor constructor which will
        add new save executors to the list."""

        self.executors = []
        self.batch_queries = []
        self.refetch_queries = []

        base_make_executor = super()._get_make_save_executor(
            refetch=refetch,
            objs=objs,
            warn_on_large_sync_set=warn_on_large_sync_set,
        )

        def wrapped_make_executor() -> SaveExecutor:
            executor = base_make_executor()
            self.executors.append(typing.cast(ProfilingSaveExecutor, executor))
            return executor

        return wrapped_make_executor

    def _send_batch_query(
        self,
        tx: BatchIteration,
        batch: QueryBatch,
    ) -> None:
        super()._send_batch_query(tx, batch)
        self.batch_queries.append(batch)

    def _send_refetch_query(
        self,
        tx: BatchIteration,
        ref: QueryRefetch,
    ) -> None:
        super()._send_refetch_query(tx, ref)
        self.refetch_queries.append(ref)

    def get_executor_batch_changes(
        self,
    ) -> list[list[list[tuple[list[tuple[GelModel, set[str]]], bool]]]]:
        # Get models per executor, per batch, per query, per change
        return [
            [
                [
                    (
                        [
                            (change.model, set(change.fields.keys()))
                            for change in query.changes
                        ],
                        query.insert,
                    )
                    for query in batch
                ]
                for batch in executor.batch_queries
            ]
            for executor in self.executors
        ]

    def get_executor_refetch_changes(
        self,
    ) -> list[list[list[dict[str, typing.Any]]]]:
        # Get models per executor, per batch, per query, per change
        return [
            [
                [query.args for query in refetch]
                for refetch in executor.refetch_queries
            ]
            for executor in self.executors
        ]


class ProfilingTestCase(tb.ModelTestCase):
    client: ProfilingTestClient

    @classmethod
    def _get_client_class(cls, connection_class):
        return ProfilingTestClient


class TestProfile(ProfilingTestCase):

    SCHEMA = os.path.join(
        os.path.dirname(__file__), "dbsetup", "chemistry.gel"
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), "dbsetup", "chemistry.esdl"
    )

    def setUp(self):
        super().setUp()

        from models.chemistry import default

        hydrogen = self.client.query_required_single(
            default.Element.filter(symbol="H").limit(1)
        )
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )
        carbon = self.client.query_required_single(
            default.Element.filter(symbol="C").limit(1)
        )
        oxygen = self.client.query_required_single(
            default.Element.filter(symbol="O").limit(1)
        )

        # Hydrogen gas (H2)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_1.bonds = [default.RefAtom.bonds.link(h_2, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(h_1, count=1)]
        hydrogen_gas = default.Compound(
            name="hydrogen gas",
            atoms=[h_1, h_2],
        )
        self.client.save(h_1, h_2, hydrogen_gas)

        # Helium gas (He)
        he_1 = default.RefAtom(element=helium)
        helium_gas = default.Compound(
            name="helium gas",
            atoms=[he_1],
        )
        self.client.save(he_1, helium_gas)

        # Oxygen gas (O2)
        o_1 = default.RefAtom(element=oxygen)
        o_2 = default.RefAtom(element=oxygen)
        o_1.bonds = [default.RefAtom.bonds.link(o_2, count=2)]
        o_2.bonds = [default.RefAtom.bonds.link(o_1, count=2)]
        oxygen_gas = default.Compound(
            name="oxygen gas",
            atoms=[o_1, o_2],
        )
        self.client.save(o_1, o_2, oxygen_gas)

        # Water (H2O)
        o_1 = default.RefAtom(element=oxygen)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        o_1.bonds = [
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(o_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(o_1, count=1)]
        oxygen_gas = default.Compound(
            name="water",
            atoms=[o_1, h_1, h_2],
            alternate_names=["Dihydrogen oxide"],
        )
        self.client.save(o_1, h_1, h_2, oxygen_gas)

        # Carbon dioxide (CO2)
        c_1 = default.RefAtom(element=carbon)
        o_1 = default.RefAtom(element=oxygen)
        o_2 = default.RefAtom(element=oxygen)
        c_1.bonds = [
            default.RefAtom.bonds.link(o_1, count=2),
            default.RefAtom.bonds.link(o_2, count=2),
        ]
        o_1.bonds = [default.RefAtom.bonds.link(c_1, count=2)]
        o_2.bonds = [default.RefAtom.bonds.link(c_1, count=2)]
        carbon_dioxide = default.Compound(
            name="carbon dioxide",
            atoms=[c_1, o_1, o_2],
        )
        self.client.save(c_1, o_1, o_2, carbon_dioxide)

        # Methane (CH4)
        c_1 = default.RefAtom(element=carbon)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_3 = default.RefAtom(element=hydrogen)
        h_4 = default.RefAtom(element=hydrogen)

        c_1.bonds = [
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
            default.RefAtom.bonds.link(h_3, count=1),
            default.RefAtom.bonds.link(h_4, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_3.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_4.bonds = [default.RefAtom.bonds.link(c_1, count=1)]

        methane = default.Compound(
            name="methane",
            atoms=[c_1, h_1, h_2, h_3, h_4],
        )
        self.client.save(c_1, h_1, h_2, h_3, h_4, methane)

        # Ethane (C2H6)
        c_1 = default.RefAtom(element=carbon)
        c_2 = default.RefAtom(element=carbon)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_3 = default.RefAtom(element=hydrogen)
        h_4 = default.RefAtom(element=hydrogen)
        h_5 = default.RefAtom(element=hydrogen)
        h_6 = default.RefAtom(element=hydrogen)

        c_1.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
            default.RefAtom.bonds.link(h_3, count=1),
        ]
        c_2.bonds = [
            default.RefAtom.bonds.link(c_1, count=1),
            default.RefAtom.bonds.link(h_4, count=1),
            default.RefAtom.bonds.link(h_5, count=1),
            default.RefAtom.bonds.link(h_6, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_3.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_4.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_5.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_6.bonds = [default.RefAtom.bonds.link(c_2, count=1)]

        ethane = default.Compound(
            name="ethane",
            atoms=[c_1, c_2, h_1, h_2, h_3, h_4, h_5, h_6],
        )
        self.client.save(c_1, c_2, h_1, h_2, h_3, h_4, h_5, h_6, ethane)

        # Propane (C3H8)
        c_1 = default.RefAtom(element=carbon)
        c_2 = default.RefAtom(element=carbon)
        c_3 = default.RefAtom(element=carbon)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_3 = default.RefAtom(element=hydrogen)
        h_4 = default.RefAtom(element=hydrogen)
        h_5 = default.RefAtom(element=hydrogen)
        h_6 = default.RefAtom(element=hydrogen)
        h_7 = default.RefAtom(element=hydrogen)
        h_8 = default.RefAtom(element=hydrogen)

        c_1.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
            default.RefAtom.bonds.link(h_3, count=1),
        ]
        c_2.bonds = [
            default.RefAtom.bonds.link(c_1, count=1),
            default.RefAtom.bonds.link(c_3, count=1),
            default.RefAtom.bonds.link(h_4, count=1),
            default.RefAtom.bonds.link(h_5, count=1),
        ]
        c_3.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(h_6, count=1),
            default.RefAtom.bonds.link(h_7, count=1),
            default.RefAtom.bonds.link(h_8, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_3.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_4.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_5.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_6.bonds = [default.RefAtom.bonds.link(c_3, count=1)]
        h_7.bonds = [default.RefAtom.bonds.link(c_3, count=1)]
        h_8.bonds = [default.RefAtom.bonds.link(c_3, count=1)]

        propane = default.Compound(
            name="propane",
            atoms=[c_1, c_2, c_3, h_1, h_2, h_3, h_4, h_5, h_6, h_7, h_8],
        )
        self.client.save(
            c_1, c_2, c_3, h_1, h_2, h_3, h_4, h_5, h_6, h_7, h_8, propane
        )

        # Butane (C4H10)
        c_1 = default.RefAtom(element=carbon)
        c_2 = default.RefAtom(element=carbon)
        c_3 = default.RefAtom(element=carbon)
        c_4 = default.RefAtom(element=carbon)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_3 = default.RefAtom(element=hydrogen)
        h_4 = default.RefAtom(element=hydrogen)
        h_5 = default.RefAtom(element=hydrogen)
        h_6 = default.RefAtom(element=hydrogen)
        h_7 = default.RefAtom(element=hydrogen)
        h_8 = default.RefAtom(element=hydrogen)
        h_9 = default.RefAtom(element=hydrogen)
        h_10 = default.RefAtom(element=hydrogen)

        c_1.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
            default.RefAtom.bonds.link(h_3, count=1),
        ]
        c_2.bonds = [
            default.RefAtom.bonds.link(c_1, count=1),
            default.RefAtom.bonds.link(c_3, count=1),
            default.RefAtom.bonds.link(h_4, count=1),
            default.RefAtom.bonds.link(h_5, count=1),
        ]
        c_3.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(c_4, count=1),
            default.RefAtom.bonds.link(h_6, count=1),
            default.RefAtom.bonds.link(h_7, count=1),
        ]
        c_4.bonds = [
            default.RefAtom.bonds.link(c_3, count=1),
            default.RefAtom.bonds.link(h_8, count=1),
            default.RefAtom.bonds.link(h_9, count=1),
            default.RefAtom.bonds.link(h_10, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_3.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_4.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_5.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_6.bonds = [default.RefAtom.bonds.link(c_3, count=1)]
        h_7.bonds = [default.RefAtom.bonds.link(c_3, count=1)]
        h_8.bonds = [default.RefAtom.bonds.link(c_4, count=1)]
        h_9.bonds = [default.RefAtom.bonds.link(c_4, count=1)]
        h_10.bonds = [default.RefAtom.bonds.link(c_4, count=1)]

        butane = default.Compound(
            name="butane",
            atoms=[
                c_1,
                c_2,
                c_3,
                c_4,
                h_1,
                h_2,
                h_3,
                h_4,
                h_5,
                h_6,
                h_7,
                h_8,
                h_9,
                h_10,
            ],
        )
        self.client.save(
            c_1,
            c_2,
            c_3,
            c_4,
            h_1,
            h_2,
            h_3,
            h_4,
            h_5,
            h_6,
            h_7,
            h_8,
            h_9,
            h_10,
            butane,
        )

        # Methanol (CH3OH)
        c_1 = default.RefAtom(element=carbon)
        o_1 = default.RefAtom(element=oxygen)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_3 = default.RefAtom(element=hydrogen)
        h_4 = default.RefAtom(element=hydrogen)

        c_1.bonds = [
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
            default.RefAtom.bonds.link(h_3, count=1),
            default.RefAtom.bonds.link(o_1, count=1),
        ]
        o_1.bonds = [
            default.RefAtom.bonds.link(c_1, count=1),
            default.RefAtom.bonds.link(h_4, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_3.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_4.bonds = [default.RefAtom.bonds.link(o_1, count=1)]

        methanol = default.Compound(
            name="methanol",
            atoms=[c_1, h_1, h_2, h_3, o_1, h_4],
        )
        self.client.save(c_1, h_1, h_2, h_3, o_1, h_4, methanol)

        # Ethanol (C2H5OH)
        c_1 = default.RefAtom(element=carbon)
        c_2 = default.RefAtom(element=carbon)
        h_1 = default.RefAtom(element=hydrogen)
        h_2 = default.RefAtom(element=hydrogen)
        h_3 = default.RefAtom(element=hydrogen)
        h_4 = default.RefAtom(element=hydrogen)
        h_5 = default.RefAtom(element=hydrogen)
        o_1 = default.RefAtom(element=oxygen)
        h_6 = default.RefAtom(element=hydrogen)

        c_1.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(h_1, count=1),
            default.RefAtom.bonds.link(h_2, count=1),
            default.RefAtom.bonds.link(h_3, count=1),
        ]
        c_2.bonds = [
            default.RefAtom.bonds.link(c_1, count=1),
            default.RefAtom.bonds.link(h_4, count=1),
            default.RefAtom.bonds.link(h_5, count=1),
            default.RefAtom.bonds.link(o_1, count=1),
        ]
        o_1.bonds = [
            default.RefAtom.bonds.link(c_2, count=1),
            default.RefAtom.bonds.link(h_6, count=1),
        ]
        h_1.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_2.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_3.bonds = [default.RefAtom.bonds.link(c_1, count=1)]
        h_4.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_5.bonds = [default.RefAtom.bonds.link(c_2, count=1)]
        h_6.bonds = [default.RefAtom.bonds.link(o_1, count=1)]

        ethanol = default.Compound(
            name="ethanol",
            atoms=[c_1, c_2, h_1, h_2, h_3, h_4, h_5, o_1, h_6],
        )
        self.client.save(c_1, c_2, h_1, h_2, h_3, h_4, h_5, o_1, h_6, ethanol)

    def _create_from_reference(
        self,
        reactor,
        compound,
    ):
        from models.chemistry import default

        ref_atoms = list(compound.atoms)
        new_atoms = [
            default.Atom(reactor=reactor, element=ref_atom.element)
            for ref_atom in ref_atoms
        ]

        for ref_atom, atom in zip(ref_atoms, new_atoms, strict=True):
            atom.bonds = [
                default.Atom.bonds.link(
                    new_atoms[
                        ref_atoms.index(
                            typing.cast(
                                default.RefAtom, ref_bond.without_linkprops()
                            )
                        )
                    ],
                    count=ref_bond.__linkprops__.count,
                )
                for ref_bond in ref_atom.bonds
            ]

        return new_atoms

    def test_profiling_simple_01(self):
        # Adding an object with no links
        from models.chemistry import default

        # Create new objects
        reactor = default.Reactor()

        # Sync
        self.client.sync(reactor)

        # Check count of queries and their contents
        self.assertEqual(len(self.client.batch_queries), 1)
        self.assertEqual(len(self.client.refetch_queries), 0)
        self.assertEqual(
            self.client.get_executor_batch_changes(),
            [
                [
                    [
                        (
                            [(reactor, set())],
                            True,
                        )
                    ],
                ]
            ],
        )
        self.assertEqual(
            self.client.get_executor_refetch_changes(),
            [[[]]],
        )

        # Check that values are fetched
        self.assertEqual(reactor.total_weight, 0)

    def test_profiling_simple_02(self):
        # Adding an object with a link to existing object
        from models.chemistry import default

        # Create new objects
        hydrogen = self.client.query_required_single(
            default.Element.filter(symbol="H").limit(1)
        )
        ref_atom = default.RefAtom(element=hydrogen)

        # Sync
        self.client.sync(ref_atom)

        # Check count of queries and their contents
        self.assertEqual(len(self.client.batch_queries), 1)
        self.assertEqual(len(self.client.refetch_queries), 1)
        self.maxDiff = None
        self.assertEqual(
            self.client.get_executor_batch_changes(),
            [
                [
                    [
                        (
                            [(ref_atom, {"element"})],
                            True,
                        )
                    ],
                ]
            ],
        )
        self.assertEqual(
            self.client.get_executor_refetch_changes(),
            [
                [
                    [
                        {
                            "existing": [hydrogen.id],
                            "new": [ref_atom.id],
                            "spec": [(hydrogen.id, [])],
                        }
                    ],
                ]
            ],
        )

        # Check that computed values are fetched
        self.assertEqual(ref_atom.weight, 1.008)

    def test_profiling_simple_03(self):
        # Create two objects tom with links to existing and new items
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

        # Check count of queries and their contents
        self.assertEqual(len(self.client.batch_queries), 2)
        self.assertEqual(len(self.client.refetch_queries), 1)
        self.assertEqual(
            self.client.get_executor_batch_changes(),
            [
                [
                    [
                        (
                            [(reactor, set())],
                            True,
                        )
                    ],
                    [
                        (
                            [(new_atom, {"reactor", "element"})],
                            True,
                        )
                    ],
                ]
            ],
        )
        self.assertEqual(
            self.client.get_executor_refetch_changes(),
            [
                [
                    [
                        {
                            "existing": [helium.id],
                            "new": [reactor.id, new_atom.id],
                            "spec": [(helium.id, [])],
                        }
                    ],
                ]
            ],
        )

        # Check that values are fetched
        self.assertEqual(new_atom.weight, 4.0026)
        self.assertEqual(new_atom.total_bond_count, 0)
        self.assertEqual(reactor.total_weight, 4.0026)

    def test_profiling_simple_04(self):
        # Create many objects with links to existing and new items
        from models.chemistry import default

        # Existing objects
        hydrogen = self.client.query_required_single(
            default.Element.filter(symbol="H").limit(1)
        )
        oxygen = self.client.query_required_single(
            default.Element.filter(symbol="O").limit(1)
        )
        water = self.client.query_required_single(
            default.Compound.select(
                atoms=lambda compound: compound.atoms.select(
                    element=lambda atom: atom.element.select(name=True),
                    bonds=True,
                )
            )
            .filter(name="water")
            .limit(1)
        )

        # Create new objects
        reactor = default.Reactor()
        new_atoms = self._create_from_reference(
            reactor,
            water,
        )

        # Sync
        self.client.sync(*new_atoms)

        # Check count of queries and their contents
        self.assertEqual(len(self.client.batch_queries), 3)
        self.assertEqual(len(self.client.refetch_queries), 1)
        self.maxDiff = None
        self.assertEqual(
            self.client.get_executor_batch_changes(),
            [
                [
                    [
                        (
                            [(reactor, set())],
                            True,
                        )
                    ],
                    [
                        (
                            [
                                (atom, {"reactor", "element"})
                                for atom in new_atoms
                            ],
                            True,
                        )
                    ],
                    [
                        (
                            [(atom, {"bonds"}) for atom in new_atoms],
                            False,
                        )
                    ],
                ]
            ],
        )
        self.assertEqual(
            self.client.get_executor_refetch_changes(),
            [
                [
                    [
                        {
                            "existing": [hydrogen.id, oxygen.id],
                            "new": (
                                [reactor.id] + [atom.id for atom in new_atoms]
                            ),
                            "spec": [(hydrogen.id, []), (oxygen.id, [])],
                        }
                    ],
                ]
            ],
        )

        # Check that values are fetched
        self.assertEqual(
            list(sorted(atom.weight for atom in new_atoms)),
            [1.008, 1.008, 15.999],
        )
        self.assertEqual(
            list(sorted(atom.total_bond_count for atom in new_atoms)),
            [1, 1, 2],
        )
        self.assertEqual(reactor.total_weight, 4.0026)

    def test_profiling_large_sync_01(self):
        # Adding many simple objects with no links
        from models.chemistry import default

        # Create new objects
        reactors = []
        for _ in range(200):
            reactor = default.Reactor()
            reactors.append(reactor)

        # Sync
        self.client.sync(*reactors, warn_on_large_sync=False)

        # Check count of queries and their contents
        self.assertEqual(len(self.client.batch_queries), 1)
        self.assertEqual(len(self.client.refetch_queries), 0)
        self.assertEqual(
            self.client.get_executor_batch_changes(),
            [
                [
                    [
                        (
                            [(reactor, set()) for reactor in reactors],
                            True,
                        )
                    ],
                ]
            ],
        )
        self.assertEqual(
            self.client.get_executor_refetch_changes(),
            [[[]]],
        )

        # Check that values are fetched
        for reactor in reactors:
            self.assertEqual(reactor.total_weight, 0)  # No atoms
            self.assertEqual(len(reactor.atoms), 0)  # No atoms

    def test_profiling_large_sync_02(self):
        # Adding many objects with links between them
        from models.chemistry import default

        # Create a reactor
        reactor = default.Reactor()

        # Create 100 helium atoms
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        atoms = []
        for _ in range(200):
            atom = default.Atom(reactor=reactor, element=helium)
            atoms.append(atom)

        # Sync all objects
        self.client.sync(reactor, *atoms, warn_on_large_sync=False)

        # Check count of queries
        self.assertEqual(len(self.client.batch_queries), 2)
        self.assertEqual(len(self.client.refetch_queries), 1)

        # Check batch query contents
        self.assertEqual(
            self.client.get_executor_batch_changes(),
            [
                [
                    [
                        (
                            [(reactor, set())],
                            True,
                        )
                    ],
                    [
                        (
                            [(atom, {"reactor", "element"}) for atom in atoms],
                            True,
                        )
                    ],
                ]
            ],
        )

        # Check refetch query contents
        self.assertEqual(
            self.client.get_executor_refetch_changes(),
            [
                [
                    [
                        {
                            "existing": [helium.id],
                            "new": [reactor.id] + [atom.id for atom in atoms],
                            "spec": [(helium.id, [])],
                        }
                    ],
                ]
            ],
        )

        # Check that values are fetched
        self.assertEqual(reactor.total_weight, 400.26)
        self.assertEqual(len(reactor.atoms), 100)

        for atom in atoms:
            self.assertEqual(atom.weight, 4.0026)
            self.assertEqual(atom.total_bond_count, 0)

    def test_profiling_large_sync_03(self):
        # Create many atoms, sync, do a big reaction and sync again
        from models.chemistry import default

        # Get existing objects
        carbon = self.client.query_required_single(
            default.Element.filter(symbol="C").limit(1)
        )
        hydrogen = self.client.query_required_single(
            default.Element.filter(symbol="H").limit(1)
        )
        oxygen = self.client.query_required_single(
            default.Element.filter(symbol="O").limit(1)
        )

        methane = self.client.query_required_single(
            default.Compound.select(
                atoms=lambda compound: compound.atoms.select(
                    element=lambda atom: atom.element.select(name=True),
                    bonds=True,
                )
            )
            .filter(name="methane")
            .limit(1)
        )

        # Create new objects
        reactor = default.Reactor()

        methane_atoms = [
            atom
            for _ in range(20)
            for atom in self._create_from_reference(reactor, methane)
        ]
        carbon_atoms = [
            atom for atom in methane_atoms if atom.element == carbon
        ]
        hydrogen_atoms = [
            atom for atom in methane_atoms if atom.element == hydrogen
        ]

        oxygen_atoms_for_carbon = [
            default.Atom(reactor=reactor, element=oxygen)
            for _ in range(len(carbon_atoms) * 2)
        ]
        oxygen_atoms_for_hydrogen = [
            default.Atom(reactor=reactor, element=oxygen)
            for _ in range(len(hydrogen_atoms) // 2)
        ]
        oxygen_atoms = oxygen_atoms_for_carbon + oxygen_atoms_for_hydrogen

        # Initial sync
        self.client.sync(
            reactor, *methane_atoms, *oxygen_atoms, warn_on_large_sync=False
        )

        # Combust the methanes

        # Create bonds between carbon and oxygen atoms
        for i in range(len(carbon_atoms)):
            c_1 = carbon_atoms[i]
            o_1 = oxygen_atoms_for_carbon[i * 2]
            o_2 = oxygen_atoms_for_carbon[i * 2 + 1]

            c_1.bonds = [
                default.Atom.bonds.link(o_1, count=2),
                default.Atom.bonds.link(o_2, count=2),
            ]
            o_1.bonds = [default.Atom.bonds.link(c_1, count=2)]
            o_2.bonds = [default.Atom.bonds.link(c_1, count=2)]

        # Create bonds between hydrogen and oxygen atoms
        for i in range(len(hydrogen_atoms) // 2):
            h_1 = hydrogen_atoms[i * 2]
            h_2 = hydrogen_atoms[i * 2 + 1]
            o_1 = oxygen_atoms_for_hydrogen[i]

            h_1.bonds = [default.Atom.bonds.link(o_1, count=1)]
            h_2.bonds = [default.Atom.bonds.link(o_1, count=1)]
            o_1.bonds = [
                default.Atom.bonds.link(h_1, count=1),
                default.Atom.bonds.link(h_2, count=1),
            ]

        # Updating sync
        self.client.sync(
            reactor, *methane_atoms, *oxygen_atoms, warn_on_large_sync=False
        )

        self.maxDiff = None

        # Check count of queries
        self.assertEqual(len(self.client.batch_queries), 1)
        self.assertEqual(len(self.client.refetch_queries), 3)

        # Check batch query contents
        batch_changes = self.client.get_executor_batch_changes()
        self.assertEqual(len(batch_changes), 1)  # One executor
        self.assertEqual(len(batch_changes[0]), 1)  # One batch
        self.assertEqual(len(batch_changes[0][0]), 1)  # One query in the batch
        batch_query = batch_changes[0][0][0]
        # Check that all expected atoms are included
        self.assertEqual(
            set(methane_atoms + oxygen_atoms),
            {atom for atom, _ in batch_query[0]},
        )
        # Check that all atoms have bond updates
        self.assertTrue(
            all({"bonds"} == fields for _, fields in batch_query[0])
        )
        # Check that it's an update query
        self.assertFalse(batch_query[1])

        # Check refetch query contents
        refetch_changes = self.client.get_executor_refetch_changes()
        self.assertEqual(len(refetch_changes), 1)  # One executor
        self.assertEqual(len(refetch_changes[0]), 1)  # One batch
        self.assertEqual(
            len(refetch_changes[0][0]), 3
        )  # Three refetch queries in the batch

        # Check that all refetch queries have the expected structure
        for refetch_query in refetch_changes[0][0]:
            self.assertEqual(refetch_query.keys(), {"existing", "new", "spec"})

        # Each refetch should reference all existing objects
        for refetch_query in refetch_changes[0][0]:
            self.assertEqual(
                set(refetch_query["existing"]),
                {
                    reactor.id,
                    hydrogen.id,
                    carbon.id,
                    oxygen.id,
                    *[atom.id for atom in methane_atoms + oxygen_atoms],
                },
            )

        # Each refetch should have an empty new list
        for refetch_query in refetch_changes[0][0]:
            self.assertEqual(len(refetch_query["new"]), 0)

        # Check refetch specs
        self.assertEqual(
            set(atom_id for atom_id, _ in refetch_changes[0][0][0]["spec"]),
            {reactor.id},
        )
        self.assertEqual(
            set(atom_id for atom_id, _ in refetch_changes[0][0][1]["spec"]),
            {atom.id for atom in methane_atoms + oxygen_atoms},
        )
        self.assertEqual(
            set(atom_id for atom_id, _ in refetch_changes[0][0][2]["spec"]),
            {hydrogen.id, carbon.id, oxygen.id},
        )

    def test_profiling_large_sync_04(self):
        # Create many atoms and then move some to another reactor
        from models.chemistry import default

        # Get existing elements
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )
        neon = self.client.query_required_single(
            default.Element.filter(symbol="Ne").limit(1)
        )

        # Create two reactors
        reactor_1 = default.Reactor()
        reactor_2 = default.Reactor()

        # Create 100 helium atoms in reactor 1
        helium_atoms = [
            default.Atom(reactor=reactor_1, element=helium) for _ in range(100)
        ]
        # Create 100 neon atoms in reactor 1
        neon_atoms: list[default.Atom] = [
            default.Atom(reactor=reactor_1, element=neon) for _ in range(100)
        ]

        # Initial sync
        self.client.sync(
            reactor_1,
            reactor_2,
            *helium_atoms,
            *neon_atoms,
            warn_on_large_sync=False,
        )

        # Move all neon atoms to reactor 2
        for atom in neon_atoms:
            atom.reactor = reactor_2

        # Updating sync - only the neon atoms should be updated
        self.client.sync(*neon_atoms, warn_on_large_sync=False)

        # Check count of queries
        self.assertEqual(len(self.client.batch_queries), 1)
        self.assertEqual(len(self.client.refetch_queries), 3)

        # Check batch query contents
        batch_changes = self.client.get_executor_batch_changes()
        self.assertEqual(len(batch_changes), 1)  # One executor
        self.assertEqual(len(batch_changes[0]), 1)  # One batch
        self.assertEqual(len(batch_changes[0][0]), 1)  # One query in the batch

        batch_query = batch_changes[0][0][0]
        # Check that only neon atoms are included in the update
        self.assertEqual(set(neon_atoms), {atom for atom, _ in batch_query[0]})
        # Check that all atoms have reactor updates
        self.assertTrue(
            all({"reactor"} == fields for _, fields in batch_query[0])
        )
        # Check that it's an update query
        self.assertFalse(batch_query[1])

        # Check refetch query contents
        refetch_changes = self.client.get_executor_refetch_changes()
        self.assertEqual(len(refetch_changes), 1)  # One executor
        self.assertEqual(len(refetch_changes[0]), 1)  # One batch
        self.assertEqual(
            len(refetch_changes[0][0]), 3
        )  # Three refetch queries in the batch

        # Check that all refetch queries have the expected structure
        for refetch_query in refetch_changes[0][0]:
            self.assertEqual(refetch_query.keys(), {"existing", "new", "spec"})

        # Each refetch should reference modified and related objects
        for refetch_query in refetch_changes[0][0]:
            self.assertEqual(
                set(refetch_query["existing"]),
                {
                    reactor_2.id,
                    neon.id,
                    *[atom.id for atom in neon_atoms],
                },
            )

        # Check that new is empty (no new objects in second sync)
        for refetch_query in refetch_changes[0][0]:
            self.assertEqual(len(refetch_query["new"]), 0)

        # Check refetch specs
        self.assertEqual(
            set(atom_id for atom_id, _ in refetch_changes[0][0][0]["spec"]),
            {atom.id for atom in neon_atoms},
        )
        self.assertEqual(
            set(atom_id for atom_id, _ in refetch_changes[0][0][1]["spec"]),
            {neon.id},
        )
        self.assertEqual(
            set(atom_id for atom_id, _ in refetch_changes[0][0][2]["spec"]),
            {reactor_2.id},
        )
