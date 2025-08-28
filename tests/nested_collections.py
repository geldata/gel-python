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

from __future__ import annotations
import abc
import typing

from gel._internal._tracked_list import AbstractTrackedList

# Useful functions for working with nested collections


class NonStrSequence(abc.ABC):
    @classmethod
    def __subclasshook__(cls, C):
        # not possible to do with AnyStr
        if issubclass(C, (str, bytes)):
            return NotImplemented
        else:
            return issubclass(C, typing.Sequence)

    @abc.abstractmethod
    def dummy(self):
        # ruff complains if we don't have an abstract method
        pass


def get_value(
    collection: typing.Sequence[typing.Any],
    indexes: list[int],
) -> typing.Any:
    if len(indexes) > 1:
        return get_value(collection[indexes[0]], indexes[1:])
    elif len(indexes) == 1:
        return collection[indexes[0]]
    else:
        raise RuntimeError


def set_prop_value(
    collection: AbstractTrackedList[typing.Any] | tuple[typing.Any],
    indexes: list[int],
    val: typing.Any,
) -> tuple[typing.Any] | None:
    # Modify, and also return, a prop list with the value at an index set to
    # a new value.

    assert indexes

    # If the collection is a tuple, we return the modified tuple and update
    # the owner.
    if isinstance(collection, tuple):
        if len(indexes) == 1:
            return (
                collection[: indexes[0]]
                + (val,)
                + collection[indexes[0] + 1 :]
            )

        else:
            inner_tuple_changed = set_prop_value(
                collection[indexes[0]],
                indexes[1:],
                val,
            )
            if inner_tuple_changed:
                return typing.cast(
                    tuple,
                    collection[: indexes[0]]
                    + (inner_tuple_changed,)
                    + collection[indexes[0] + 1 :],
                )
            else:
                return None

    elif isinstance(collection, (AbstractTrackedList, list)):
        if len(indexes) == 1:
            collection[indexes[0]] = val
        else:
            inner_tuple_changed = set_prop_value(
                collection[indexes[0]],
                indexes[1:],
                val,
            )
            if inner_tuple_changed:
                collection[indexes[0]] = inner_tuple_changed
        return None

    else:
        raise RuntimeError


def replace_value(
    collection: list[typing.Any] | tuple[typing.Any, ...],
    indexes: list[int],
    val: typing.Any,
) -> list[typing.Any] | tuple[typing.Any, ...]:
    assert indexes

    if isinstance(collection, list):
        return (
            collection[: indexes[0]]
            + (
                [
                    typing.cast(
                        list,
                        replace_value(
                            collection[indexes[0]],
                            indexes[1:],
                            val,
                        ),
                    )
                ]
                if len(indexes) > 1
                else [val]
            )
            + collection[indexes[0] + 1 :]
        )

    elif isinstance(collection, tuple):
        return (
            collection[: indexes[0]]
            + (
                (
                    typing.cast(
                        tuple,
                        replace_value(
                            collection[indexes[0]],
                            indexes[1:],
                            val,
                        ),
                    ),
                )
                if len(indexes) > 1
                else (val,)
            )
            + collection[indexes[0] + 1 :]
        )

    else:
        raise RuntimeError


def first_indexes(
    collection: typing.Sequence[typing.Any],
) -> list[int]:
    if not collection:
        return []
    if isinstance(collection[0], NonStrSequence):
        return [0] + first_indexes(collection[0])
    else:
        return [0]


def increment_indexes(
    collection: typing.Sequence[typing.Any],
    indexes: list[int],
) -> list[int]:
    # Iterate through all indexes, with children taking priority.
    #
    # eg. Iterating over ((0, 1), (2, 3)) will produce:
    # - [0, 0]
    # - [0, 1]
    # - [0]
    # - [1, 0]
    # - [1, 1]
    # - [1]
    assert indexes

    if len(indexes) > 1:
        return indexes[:1] + increment_indexes(
            collection[indexes[0]], indexes[1:]
        )

    elif len(indexes) == 1:
        next_index = indexes[0] + 1
        if next_index >= len(collection):
            return []

        return [next_index] + (
            first_indexes(collection[next_index])
            if isinstance(collection[next_index], NonStrSequence)
            else []
        )

    else:
        raise RuntimeError


def different_values_same_shape(val: typing.Any) -> typing.Any:
    if isinstance(val, list):
        return [different_values_same_shape(val) for val in val]

    elif isinstance(val, tuple):
        return tuple(different_values_same_shape(val) for val in val)

    elif isinstance(val, str):
        return val + "?"

    elif isinstance(val, int):
        return val + 100

    else:
        raise RuntimeError
