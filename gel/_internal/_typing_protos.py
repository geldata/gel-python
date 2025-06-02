# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from typing import Protocol, TypeVar
from typing_extensions import TypeAliasType
from collections.abc import Iterable, Set as AbstractSet

import datetime


_KT = TypeVar("_KT")
_VT = TypeVar("_VT")
_KT_co = TypeVar("_KT_co", covariant=True)
_VT_co = TypeVar("_VT_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)


class SupportsDunderLT(Protocol[_T_contra]):
    def __lt__(self, other: _T_contra, /) -> bool: ...


class SupportsDunderGT(Protocol[_T_contra]):
    def __gt__(self, other: _T_contra, /) -> bool: ...


SupportsRichComparison = TypeAliasType(
    "SupportsRichComparison",
    SupportsDunderLT[_T_contra] | SupportsDunderGT[_T_contra],
    type_params=(_T_contra,),
)


class SupportsKeysAndGetItem(Protocol[_KT, _VT_co]):
    def keys(self) -> Iterable[_KT]: ...
    def __getitem__(self, key: _KT, /) -> _VT_co: ...


class SupportsItems(Protocol[_KT_co, _VT_co]):
    def items(self) -> AbstractSet[tuple[_KT_co, _VT_co]]: ...


MappingInput = TypeAliasType(
    "MappingInput",
    SupportsKeysAndGetItem[_KT, _VT] | Iterable[tuple[_KT, _VT]],
    type_params=(_KT, _VT),
)


class SupportsToOrdinal(Protocol):
    def toordinal(self) -> int: ...
