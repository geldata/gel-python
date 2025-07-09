# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Protocol

import dataclasses

if TYPE_CHECKING:
    import uuid
    from gel._internal import _edgeql
    from gel._internal._schemapath import SchemaPath


@dataclasses.dataclass(frozen=True, kw_only=True)
class GelPointerReflection:
    name: str
    type: SchemaPath
    typexpr: str
    kind: _edgeql.PointerKind
    cardinality: _edgeql.Cardinality
    computed: bool
    readonly: bool
    has_default: bool
    properties: dict[str, GelPointerReflection] | None


class GelReflectionProto(Protocol):
    id: ClassVar[uuid.UUID]
    name: ClassVar[SchemaPath]


class GelSchemaMetadata:
    class __gel_reflection__:  # noqa: N801
        id: ClassVar[uuid.UUID]
        name: ClassVar[SchemaPath]


class GelSourceMetadata(GelSchemaMetadata):
    class __gel_reflection__(GelSchemaMetadata.__gel_reflection__):  # noqa: N801
        pointers: ClassVar[dict[str, GelPointerReflection]]


class GelTypeMetadata(GelSchemaMetadata):
    pass


class GelObjectTypeMetadata(GelSourceMetadata, GelTypeMetadata):
    class __gel_reflection__(  # noqa: N801
        GelSourceMetadata.__gel_reflection__,
        GelTypeMetadata.__gel_reflection__,
    ):
        pass


class GelLinkMetadata(GelSourceMetadata):
    pass
