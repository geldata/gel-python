# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Protocol

import dataclasses

if TYPE_CHECKING:
    import abc
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


if TYPE_CHECKING:

    class GelObjectTypeMetadata(abc.ABC, GelSourceMetadata, GelTypeMetadata):
        class __gel_reflection__(  # noqa: N801
            GelSourceMetadata.__gel_reflection__,
            GelTypeMetadata.__gel_reflection__,
        ):
            abstract: ClassVar[bool]

        # A marker to indicate that the type is not abstract.
        # This is to make type checkers complain if you attempt
        # to instantiate an abstract type.
        # This might not be the most natural place to stick this into,
        # but it's very low profile and not in the user's face unlike
        # having types like "Abstract" and "Concrete" and using them
        # everywhere.
        @abc.abstractmethod
        def __gel_not_abstract__(self) -> None: ...

else:

    class GelObjectTypeMetadata(GelSourceMetadata, GelTypeMetadata):
        class __gel_reflection__(  # noqa: N801
            GelSourceMetadata.__gel_reflection__,
            GelTypeMetadata.__gel_reflection__,
        ):
            abstract: ClassVar[bool]


class GelLinkMetadata(GelSourceMetadata):
    pass
