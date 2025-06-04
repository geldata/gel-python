#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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

from typing import (
    TypeVar,
    Any,
    Generic,
    Optional,
    Iterable,
    Iterator,
    Sequence,
)

T = TypeVar("T")


class Range(Generic[T]):
    __slots__ = ("_lower", "_upper", "_inc_lower", "_inc_upper", "_empty")

    def __init__(
        self,
        lower: Optional[T] = None,
        upper: Optional[T] = None,
        *,
        inc_lower: bool = True,
        inc_upper: bool = False,
        empty: bool = False,
    ) -> None:
        self._empty = empty

        if empty:
            if lower != upper or lower is not None and inc_upper and inc_lower:
                raise ValueError(
                    "conflicting arguments in range constructor: "
                    '"empty" is `true` while the specified bounds '
                    "suggest otherwise"
                )

            self._lower = self._upper = None
            self._inc_lower = self._inc_upper = False
        else:
            self._lower = lower
            self._upper = upper
            self._inc_lower = lower is not None and inc_lower
            self._inc_upper = upper is not None and inc_upper

    @property
    def lower(self) -> Optional[T]:
        return self._lower

    @property
    def inc_lower(self) -> bool:
        return self._inc_lower

    @property
    def upper(self) -> Optional[T]:
        return self._upper

    @property
    def inc_upper(self) -> bool:
        return self._inc_upper

    def is_empty(self) -> bool:
        return self._empty

    def __bool__(self):
        return not self.is_empty()

    def __eq__(self, other) -> bool:
        if isinstance(other, Range):
            o = other
        else:
            return NotImplemented

        return (
            self._lower,
            self._upper,
            self._inc_lower,
            self._inc_upper,
            self._empty,
        ) == (
            o._lower,
            o._upper,
            o._inc_lower,
            o._inc_upper,
            o._empty,
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._lower,
                self._upper,
                self._inc_lower,
                self._inc_upper,
                self._empty,
            )
        )

    def __str__(self) -> str:
        if self._empty:
            desc = "empty"
        else:
            lb = "(" if not self._inc_lower else "["
            if self._lower is not None:
                lb += repr(self._lower)

            if self._upper is not None:
                ub = repr(self._upper)
            else:
                ub = ""

            ub += ")" if self._inc_upper else "]"

            desc = f"{lb}, {ub}"

        return f"<Range {desc}>"

    __repr__ = __str__


# TODO: maybe we should implement range and multirange operations as well as
# normalization of the sub-ranges?
class MultiRange(Iterable[T]):
    _ranges: Sequence[T]

    def __init__(self, iterable: Optional[Iterable[T]] = None) -> None:
        if iterable is not None:
            self._ranges = tuple(iterable)
        else:
            self._ranges = tuple()

    def __len__(self) -> int:
        return len(self._ranges)

    def __iter__(self) -> Iterator[T]:
        return iter(self._ranges)

    def __reversed__(self) -> Iterator[T]:
        return reversed(self._ranges)

    def __str__(self) -> str:
        return f"<MultiRange {list(self._ranges)}>"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, MultiRange):
            return set(self._ranges) == set(other._ranges)
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self._ranges)
