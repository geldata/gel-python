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
import typing_extensions
import dataclasses

import csv
import os
import time

from gel import blocking_client
from gel._internal._qbmodel._pydantic import GelModel
from gel._internal._save import (
    ChangeBatch,
    QueryBatch,
    QueryRefetch,
    QueryRefetchArgs,
    SaveExecutor,
)
from gel._internal._testbase import _base as tb
from gel._internal._testbase import _models as tb_models


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
        tx: blocking_client.BatchIteration,
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
        tx: blocking_client.BatchIteration,
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


class BaseProfilingTestCase(tb_models.ModelTestCase):
    CLIENT_TYPE = ProfilingTestClient

    SCHEMA = ""

    @classmethod
    def _get_client_class(cls, connection_class):
        return ProfilingTestClient

    def get_profiling_client(self) -> ProfilingTestClient:
        return typing.cast(ProfilingTestClient, self.client)


@dataclasses.dataclass(kw_only=True, frozen=True)
class ProfilingRecord:
    object_count: int
    batch_data: list[float]
    refetch_data: list[float]


@dataclasses.dataclass(frozen=True)
class ProfilingData:
    batch_labels: typing.Sequence[str]
    refetch_labels: typing.Sequence[str]

    records: list[ProfilingRecord] = dataclasses.field(default_factory=list)

    def write_csv(self, filename: str):
        csv_data = []
        csv_data.append(
            ["object_count"]
            + list(self.batch_labels)
            + list(self.refetch_labels)
        )

        for record in self.records:
            csv_data.append(
                [
                    record.object_count,
                    *[round(t, 3) for t in record.batch_data],
                    *[round(t, 3) for t in record.refetch_data],
                ]
            )

        with open(filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csv_data)


CleanupObjects = typing_extensions.TypeAliasType(
    "CleanupObjects",
    dict[type[GelModel], typing.Sequence[GelModel]],
)


OperationResult = typing_extensions.TypeAliasType(
    "OperationResult",
    tuple[
        dict[str, typing.Sequence[GelModel]],
        CleanupObjects,
    ],
)


def cleanup_operation(
    client: ProfilingTestClient,
    objects: CleanupObjects,
) -> None:
    cleanup_ids = [o.id for _, t_objs in objects.items() for o in t_objs]
    client.query(
        "delete Object filter .id in array_unpack(<array<uuid>>$0)",
        cleanup_ids,
    )


def profile_operation(
    client: ProfilingTestClient,
    operation: typing.Callable[[int], OperationResult],
    count_max: int = 400,
) -> ProfilingData:
    # Get the number of batches and refetches
    # This also "warms up" the system
    object_labels, clenaup_objects = operation(1)
    cleanup_operation(client, clenaup_objects)

    # Prepare profiling data
    batch_labels, refetch_labels = client.get_profiling_query_labels(
        object_labels
    )
    data = ProfilingData(batch_labels, refetch_labels)

    # Profiling with increasing object count
    for count in range(5, count_max + 1, 5):
        _, clenaup_objects = operation(count)

        data.records.append(
            ProfilingRecord(
                object_count=count,
                batch_data=client.batch_query_times,
                refetch_data=client.refetch_query_times,
            )
        )

        cleanup_operation(client, clenaup_objects)

    return data


class TestProfilingSimple(BaseProfilingTestCase):
    SCHEMA = """
    using future simple_scoping;
    module default {
    type Obj_01;

    type Obj_02 {
        prop: int64;
    };

    type Obj_03 {
        prop: int64 {
            default := -1;
        };
    };

    type Target_04;
    type Source_04 {
        target: Target_04;
    };

    type Target_05;
    type Source_05 {
        target: Target_05 {
            lprop: int64;
        };
    };

    type Target_06;
    type Source_06 {
        target: Target_06 {
            lprop: int64 {
                default := 1;
            };
        };
    };
    }
    """

    def test_profiling_simple_01(self) -> None:
        # Isolated objects
        from models.TestProfilingSimple import default

        def _operation(count: int) -> OperationResult:
            print(f'_operation._operation({count})')

            objs = [default.Obj_01() for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_01: objs,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_simple_01.csv")

    def test_profiling_simple_02a(self) -> None:
        # Isolated objects with a property
        from models.TestProfilingSimple import default

        def _operation_value(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_02._operation_value({count})')

            objs = [default.Obj_02(prop=1) for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_02: objs,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_value)
        data.write_csv("profiling_simple_02_value.csv")

    def test_profiling_simple_02b(self) -> None:
        # Isolated objects with a property
        from models.TestProfilingSimple import default

        def _operation_default(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_02._operation_default({count})')

            objs = [default.Obj_02() for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_02: objs,
                },
            )

        data = profile_operation(
            self.get_profiling_client(), _operation_default
        )
        data.write_csv("profiling_simple_02_default.csv")

    def test_profiling_simple_02c(self) -> None:
        # Isolated objects with a property
        from models.TestProfilingSimple import default

        def _operation_none(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_02._operation_none({count})')

            objs = [default.Obj_02(prop=None) for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_02: objs,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_none)
        data.write_csv("profiling_simple_02_none.csv")

    def test_profiling_simple_03a(self) -> None:
        # Isolated objects with a property with default
        from models.TestProfilingSimple import default

        def _operation_value(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_03._operation_value({count})')

            objs = [default.Obj_03(prop=1) for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_03: objs,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_value)
        data.write_csv("profiling_simple_03_value.csv")

    def test_profiling_simple_03b(self) -> None:
        # Isolated objects with a property with default
        from models.TestProfilingSimple import default

        def _operation_default(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_03._operation_default({count})')

            objs = [default.Obj_03() for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_03: objs,
                },
            )

        data = profile_operation(
            self.get_profiling_client(), _operation_default
        )
        data.write_csv("profiling_simple_03_default.csv")

    def test_profiling_simple_03c(self) -> None:
        # Isolated objects with a property with default
        from models.TestProfilingSimple import default

        def _operation_none(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_03._operation_none({count})')

            objs = [default.Obj_03(prop=None) for _ in range(count)]

            self.client.sync(*objs, warn_on_large_sync=False)

            return (
                {
                    "object": objs,
                },
                {
                    default.Obj_03: objs,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_none)
        data.write_csv("profiling_simple_03_none.csv")

    def test_profiling_simple_04(self) -> None:
        # Objects with a common link target

        from models.TestProfilingSimple import default

        target = default.Target_04()
        self.client.save(target)

        def _operation(count: int) -> OperationResult:
            print(f'test_profiling_simple_04._operation({count})')

            sources = [default.Source_04(target=target) for _ in range(count)]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_04: sources,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_simple_04.csv")

    def test_profiling_simple_05a(self) -> None:
        # Objects with a common link target with a linkprop

        from models.TestProfilingSimple import default

        target = default.Target_05()
        self.client.save(target)

        def _operation_value(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_05._operation_value({count})')

            sources = [
                default.Source_05(
                    target=default.Source_05.target.link(target, lprop=1)
                )
                for _ in range(count)
            ]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_05: sources,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_value)
        data.write_csv("profiling_simple_05_value.csv")

    def test_profiling_simple_05b(self) -> None:
        # Objects with a common link target with a linkprop

        from models.TestProfilingSimple import default

        target = default.Target_05()
        self.client.save(target)

        noise = [
            default.Source_05(target=default.Source_05.target.link(target))
            for _ in range(50000)
        ]
        self.client.save(*noise)

        def _operation_default(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_05._operation_default({count})')

            sources = [
                default.Source_05(target=default.Source_05.target.link(target))
                for _ in range(count)
            ]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_05: sources,
                },
            )

        data = profile_operation(
            self.get_profiling_client(), _operation_default
        )
        data.write_csv("profiling_simple_05_default.csv")

    def test_profiling_simple_05c(self) -> None:
        # Objects with a common link target with a linkprop

        from models.TestProfilingSimple import default

        target = default.Target_05()
        self.client.save(target)

        def _operation_none(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_05._operation_none({count})')

            sources = [
                default.Source_05(
                    target=default.Source_05.target.link(target, lprop=None)
                )
                for _ in range(count)
            ]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_05: sources,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_none)
        data.write_csv("profiling_simple_05_none.csv")

    def test_profiling_simple_06a(self) -> None:
        # Objects with a common link target with a linkprop with default

        from models.TestProfilingSimple import default

        target = default.Target_06()
        self.client.save(target)

        def _operation_value(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_06._operation_value({count})')

            sources = [
                default.Source_06(
                    target=default.Source_06.target.link(target, lprop=1)
                )
                for _ in range(count)
            ]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_06: sources,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_value)
        data.write_csv("profiling_simple_06_value.csv")

    def test_profiling_simple_06b(self) -> None:
        # Objects with a common link target with a linkprop with default

        from models.TestProfilingSimple import default

        target = default.Target_06()
        self.client.save(target)

        def _operation_default(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_06._operation_default({count})')

            sources = [
                default.Source_06(target=default.Source_06.target.link(target))
                for _ in range(count)
            ]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_06: sources,
                },
            )

        data = profile_operation(
            self.get_profiling_client(), _operation_default
        )
        data.write_csv("profiling_simple_06_default.csv")

    def test_profiling_simple_06c(self) -> None:
        # Objects with a common link target with a linkprop with default

        from models.TestProfilingSimple import default

        target = default.Target_06()
        self.client.save(target)

        def _operation_none(
            count: int,
        ) -> OperationResult:
            print(f'test_profiling_simple_06._operation_none({count})')

            sources = [
                default.Source_06(
                    target=default.Source_06.target.link(target, lprop=None)
                )
                for _ in range(count)
            ]

            self.client.sync(target, *sources, warn_on_large_sync=False)

            return (
                {
                    "target": [target],
                    "sources": sources,
                },
                {
                    default.Source_06: sources,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation_none)
        data.write_csv("profiling_simple_06_none.csv")


class TestProfilingChemistry(BaseProfilingTestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), "dbsetup", "chemistry.gel"
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), "dbsetup", "chemistry.esdl"
    )

    def test_profiling_chemistry_01(self) -> None:
        # Adding objects with no links
        from models.chemistry import default

        def _operation(count: int) -> OperationResult:
            # Create reactors
            reactors = [default.Reactor() for _ in range(count)]

            # Sync
            self.client.sync(*reactors, warn_on_large_sync=False)

            return (
                {
                    "reactors": reactors,
                },
                {
                    default.Reactor: reactors,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_chemistry_01.csv")

    def test_profiling_chemistry_02(self) -> None:
        # Adding a single reactor with increasing number of helium atoms
        from models.chemistry import default

        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        def _operation(count: int) -> OperationResult:
            # Create new reactor and atoms
            reactor = default.Reactor()
            atoms = [
                default.Atom(reactor=reactor, element=helium)
                for _ in range(count)
            ]

            # Sync
            self.client.sync(reactor, *atoms, warn_on_large_sync=False)

            return (
                {
                    "helium": [helium],
                    "reactor": [reactor],
                    "atoms": atoms,
                },
                {
                    default.Reactor: [reactor],
                    default.Atom: atoms,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_chemistry_02.csv")

    def test_profiling_chemistry_03(self) -> None:
        # Create two reactors, add atoms to first, sync, then move all to
        # second
        from models.chemistry import default

        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        def _operation(count: int) -> OperationResult:
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

            return (
                {
                    "helium": [helium],
                    "reactor_1": [reactor_1],
                    "reactor_2": [reactor_2],
                    "atoms": atoms,
                },
                {
                    default.Reactor: [reactor_1, reactor_2],
                    default.Atom: atoms,
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_chemistry_03.csv")

    def test_profiling_chemistry_04(self) -> None:
        # Create a compound with an increasing number of helium RefAtoms
        from models.chemistry import default

        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        def _operation(count: int) -> OperationResult:
            # Create RefAtoms with helium element
            ref_atoms = [default.RefAtom(element=helium) for _ in range(count)]

            # Create compound with all the atoms
            compound = default.Compound(
                name=f"test_compound_{count}", atoms=ref_atoms
            )

            # Sync - create all objects
            self.client.sync(*ref_atoms, compound, warn_on_large_sync=False)

            return (
                {
                    "helium": [helium],
                    "ref_atoms": ref_atoms,
                    "compound": [compound],
                },
                {
                    default.RefAtom: ref_atoms,
                    default.Compound: [compound],
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_chemistry_04.csv")

    def test_profiling_chemistry_05(self) -> None:
        # Create compound with atoms, sync, then clear atoms and sync again
        from models.chemistry import default

        helium = self.client.query_required_single(
            default.Element.filter(symbol="He").limit(1)
        )

        def _operation(count: int) -> OperationResult:
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

            return (
                {
                    "helium": [helium],
                    "ref_atoms": ref_atoms,
                    "compound": [compound],
                },
                {
                    default.RefAtom: ref_atoms,
                    default.Compound: [compound],
                },
            )

        data = profile_operation(self.get_profiling_client(), _operation)
        data.write_csv("profiling_chemistry_05.csv")
