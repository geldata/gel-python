# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

# Type annotations must be visible in coerce_to_dataclass()
#
# ruff: noqa: TC001

from __future__ import annotations

import dataclasses

from . import _types
from . import _enums


@dataclasses.dataclass(frozen=True, kw_only=True)
class CallableParam:
    name: str
    type: _types.TypeRef
    kind: _enums.CallableParamKind
    typemod: _enums.TypeModifier
    default: str | None


@dataclasses.dataclass(frozen=True, kw_only=True)
class Callable:
    id: str
    name: str
    description: str | None
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]
