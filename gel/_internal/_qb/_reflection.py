# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar

import dataclasses

if TYPE_CHECKING:
    import uuid
    from gel._internal import _edgeql
    from gel._internal import _reflection


@dataclasses.dataclass(frozen=True, kw_only=True)
class GelPointerReflection:
    type_: _reflection.SchemaPath
    kind: _edgeql.PointerKind
    cardinality: _edgeql.Cardinality
    computed: bool
    readonly: bool
    properties: dict[str, GelPointerReflection] | None


class GelTypeMetadata:
    class __gel_reflection__:  # noqa: N801
        id: ClassVar[uuid.UUID]
        name: ClassVar[_reflection.SchemaPath]
        pointers: ClassVar[dict[str, GelPointerReflection]]
