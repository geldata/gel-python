# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import TypeVar
from typing_extensions import dataclass_transform

import dataclasses


_dataclass = dataclasses.dataclass(eq=False, frozen=True, kw_only=True)

_T = TypeVar("_T")


@dataclass_transform(
    eq_default=False,
    frozen_default=True,
    kw_only_default=True,
)
def struct(t: type[_T]) -> type[_T]:
    return _dataclass(t)
