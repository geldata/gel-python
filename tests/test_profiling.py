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

import csv
import os
import time

from gel import _testbase as tb
from gel.blocking_client import BatchIteration
from gel._internal._qbmodel._pydantic import GelModel
from gel._internal._save import (
    ChangeBatch,
    QueryBatch,
    QueryRefetch,
    QueryRefetchArgs,
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
    __slots__ = (
        "executors",
        "batch_queries",
        "batch_query_times",
        "refetch_queries",
        "refetch_query_times",
    )
    _save_executor_type = ProfilingSaveExecutor

    executors: list[ProfilingSaveExecutor]
    batch_queries: list[QueryBatch]
    batch_query_times: list[float]
    refetch_queries: list[QueryRefetch]
    refetch_query_times: list[float]

    def __init__(self, **kwargs: typing.Any):
        super().__init__(**kwargs)
        self.executors = []
        self.batch_queries = []
        self.batch_query_times = []
        self.refetch_queries = []
        self.refetch_query_times = []

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
        self.batch_query_times = []
        self.refetch_queries = []
        self.refetch_query_times = []

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

    def _send_batch_queries(
        self,
        tx: BatchIteration,
        batches: list[QueryBatch],
    ) -> list[typing.Any]:
        result: list[typing.Any] = []
        for batch in batches:
            self._send_batch_query(tx, batch)

            start = time.perf_counter_ns()
            result.extend(tx.wait())
            finish = time.perf_counter_ns()

            self.batch_queries.append(batch)
            self.batch_query_times.append((finish - start) / 1000000000)
        return result

    def _send_refetch_queries(
        self,
        tx: BatchIteration,
        ref_queries: list[QueryRefetch],
    ) -> list[typing.Any]:
        result: list[typing.Any] = []
        for ref in ref_queries:
            self._send_refetch_query(tx, ref)

            start = time.perf_counter_ns()
            result.extend(tx.wait())
            finish = time.perf_counter_ns()

            self.refetch_queries.append(ref)
            self.refetch_query_times.append((finish - start) / 1000000000)
        return result

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
    ) -> list[list[list[QueryRefetchArgs]]]:
        # Get models per executor, per batch, per query, per change
        return [
            [
                [query.args for query in refetch]
                for refetch in executor.refetch_queries
            ]
            for executor in self.executors
        ]

    def get_profiling_query_labels(
        self, object_labels: dict[str, typing.Sequence[GelModel]]
    ) -> tuple[list[str], list[str]]:
        batch_labels = []
        refetch_labels = []

        for batch_query in self.batch_queries:
            batch_labels.append(
                "batch("
                + ", ".join(
                    label
                    for label, objects in object_labels.items()
                    if any(
                        set(objects)
                        & set(change.model for change in batch_query.changes)
                    )
                )
                + ")"
            )

        for refetch_query in self.refetch_queries:
            batch_labels.append(
                "refetch("
                + ", ".join(
                    label
                    for label, objects in object_labels.items()
                    if any(
                        set(obj.id for obj in objects)
                        & set(obj_id for obj_id, _ in refetch_query.args.spec)
                    )
                )
                + ")"
            )

        return batch_labels, refetch_labels

    def get_simple_profiling_query_labels(self) -> tuple[list[str], list[str]]:
        """Simple version for timing tests that just need sequential labels."""
        batch_labels = [
            f"batch_{i}" for i in range(len(self.batch_query_times))
        ]
        refetch_labels = [
            f"refetch_{i}" for i in range(len(self.refetch_query_times))
        ]
        return batch_labels, refetch_labels


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

    def test_profiling_timing_simple_01(self):
        # Adding objects with no links
        from models.chemistry import default

        # Get the number of batches and refetches
        # This also "warms up" the system
        reactors = [default.Reactor()]
        self.client.sync(*reactors)

        # Prepare CSV data
        batch_labels, refetch_labels = self.client.get_profiling_query_labels(
            {
                "reactors": reactors,
            }
        )

        csv_data = []
        csv_data.append(["object_count"] + batch_labels + refetch_labels)

        # Profiling with increasing numbers of objects
        for count in range(1, 1001):
            # Create new objects
            reactors = [default.Reactor() for _ in range(count)]

            # Sync
            self.client.sync(*reactors, warn_on_large_sync=False)

            # Add to CSV data
            csv_data.append(
                [
                    count,
                    *[round(t, 3) for t in self.client.batch_query_times],
                    *[round(t, 3) for t in self.client.refetch_query_times],
                ]
            )

        # Write to CSV file
        csv_filename = "profiling_timing_simple_01.csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)

    def test_profiling_timing_simple_02(self):
        # Adding a single reactor with increasing number of helium atoms
        from models.chemistry import default

        # Get existing helium element
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        # Get the number of batches and refetches from a warm-up run
        # This also "warms up" the system
        reactor = default.Reactor()
        atoms = [default.Atom(reactor=reactor, element=helium)]
        self.client.sync(reactor, *atoms)

        # Prepare CSV data
        batch_labels, refetch_labels = self.client.get_profiling_query_labels(
            {
                "helium": [helium],
                "reactor": [reactor],
                "atoms": atoms,
            }
        )

        csv_data = []
        csv_data.append(["object_count"] + batch_labels + refetch_labels)

        # Profiling with increasing numbers of objects
        for count in range(1, 1001):
            # Create new reactor and atoms
            reactor = default.Reactor()
            atoms = [
                default.Atom(reactor=reactor, element=helium)
                for _ in range(count)
            ]

            # Sync
            self.client.sync(reactor, *atoms, warn_on_large_sync=False)

            # Add to CSV data
            csv_data.append(
                [
                    count,
                    *[round(t, 3) for t in self.client.batch_query_times],
                    *[round(t, 3) for t in self.client.refetch_query_times],
                ]
            )

        # Write to CSV file
        csv_filename = "profiling_timing_simple_02.csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)

    def test_profiling_timing_simple_03(self):
        # Create two reactors, add atoms to first, sync, then move all to
        # second
        from models.chemistry import default

        # Get existing helium element
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        # Get the number of batches and refetches from a warm-up run
        # This also "warms up" the system
        reactor_1 = default.Reactor()
        reactor_2 = default.Reactor()
        atoms = [default.Atom(reactor=reactor_1, element=helium)]
        self.client.sync(reactor_1, reactor_2, *atoms)
        atoms[0].reactor = reactor_2
        self.client.sync(*atoms)

        # Prepare CSV data
        batch_labels, refetch_labels = self.client.get_profiling_query_labels(
            {
                "helium": [helium],
                "reactor_1": [reactor_1],
                "reactor_2": [reactor_2],
                "atoms": atoms,
            }
        )

        csv_data = []
        csv_data.append(["object_count"] + batch_labels + refetch_labels)

        # Profiling with increasing numbers of objects
        for count in range(1, 1001):
            # Create two reactors
            reactor_1 = default.Reactor()
            reactor_2 = default.Reactor()

            # Create atoms in reactor 1
            atoms = [
                default.Atom(reactor=reactor_1, element=helium)
                for _ in range(count)
            ]

            # Initial sync - create reactors and atoms
            self.client.sync(
                reactor_1, reactor_2, *atoms, warn_on_large_sync=False
            )

            # Move all atoms to reactor 2
            for atom in atoms:
                atom.reactor = reactor_2

            # Sync the move operation
            self.client.sync(*atoms, warn_on_large_sync=False)

            # Add to CSV data
            csv_data.append(
                [
                    count,
                    *[round(t, 3) for t in self.client.batch_query_times],
                    *[round(t, 3) for t in self.client.refetch_query_times],
                ]
            )

        # Write to CSV file
        csv_filename = "profiling_timing_simple_03.csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)

    def test_profiling_timing_simple_04(self):
        # Create a compound with an increasing number of helium RefAtoms
        from models.chemistry import default

        # Get existing helium element
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        # Get the number of batches and refetches from a warm-up run
        # This also "warms up" the system
        ref_atoms = [default.RefAtom(element=helium)]
        compound = default.Compound(name="test_compound", atoms=ref_atoms)
        self.client.sync(*ref_atoms, compound)

        # Prepare CSV data
        batch_labels, refetch_labels = self.client.get_profiling_query_labels(
            {
                "helium": [helium],
                "ref_atoms": ref_atoms,
                "compound": [compound],
            }
        )

        csv_data = []
        csv_data.append(["object_count"] + batch_labels + refetch_labels)

        # Profiling with increasing numbers of objects
        for count in range(1, 1001):
            # Create RefAtoms with helium element
            ref_atoms = [default.RefAtom(element=helium) for _ in range(count)]

            # Create compound with all the atoms
            compound = default.Compound(
                name=f"test_compound_{count}", atoms=ref_atoms
            )

            # Sync - create all objects
            self.client.sync(*ref_atoms, compound, warn_on_large_sync=False)

            # Add to CSV data
            csv_data.append(
                [
                    count,
                    *[round(t, 3) for t in self.client.batch_query_times],
                    *[round(t, 3) for t in self.client.refetch_query_times],
                ]
            )

        # Write to CSV file
        csv_filename = "profiling_timing_simple_04.csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)

    def test_profiling_timing_simple_05(self):
        # Create compound with atoms, sync, then clear atoms and sync again
        from models.chemistry import default

        # Get existing helium element
        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        # Get the number of batches and refetches from a warm-up run
        # This also "warms up" the system
        ref_atoms = [default.RefAtom(element=helium)]
        compound = default.Compound(name="test_compound", atoms=ref_atoms)
        self.client.sync(*ref_atoms, compound)
        compound.atoms.clear()
        self.client.sync(compound)

        # Prepare CSV data
        batch_labels, refetch_labels = self.client.get_profiling_query_labels(
            {
                "helium": [helium],
                "ref_atoms": ref_atoms,
                "compound": [compound],
            }
        )

        csv_data = []
        csv_data.append(["object_count"] + batch_labels + refetch_labels)

        # Profiling with increasing numbers of objects
        for count in range(1, 1001):
            # Create RefAtoms with helium element
            ref_atoms = [default.RefAtom(element=helium) for _ in range(count)]

            # Create compound with all the atoms
            compound = default.Compound(
                name=f"test_compound_{count}", atoms=ref_atoms
            )

            # Initial sync - create all objects
            self.client.sync(*ref_atoms, compound, warn_on_large_sync=False)

            # Clear all atoms from the compound
            compound.atoms.clear()

            # Sync the clear operation
            self.client.sync(compound, warn_on_large_sync=False)

            # Add to CSV data
            csv_data.append(
                [
                    count,
                    *[round(t, 3) for t in self.client.batch_query_times],
                    *[round(t, 3) for t in self.client.refetch_query_times],
                ]
            )

        # Write to CSV file
        csv_filename = "profiling_timing_simple_05.csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)
