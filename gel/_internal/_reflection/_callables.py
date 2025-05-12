# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

import dataclasses
import uuid

from . import _enums
from . import _types


@dataclasses.dataclass(frozen=True, kw_only=True)
class CallableParam:
    name: str
    type: _types.TypeRef
    kind: _enums.CallableParamKind
    typemod: _enums.TypeModifier
    default: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class Callable:
    id: uuid.UUID
    name: str
    description: str
    return_type: _types.TypeRef
    return_typemod: _enums.TypeModifier
    params: list[CallableParam]
