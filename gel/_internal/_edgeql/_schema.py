# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations

from typing import TYPE_CHECKING, final

import re
import uuid

from gel._internal._polyfills._strenum import StrEnum
from gel._internal._schemapath import SchemaPath, TypeName


if TYPE_CHECKING:
    from collections.abc import Iterable


@final
class Cardinality(StrEnum):
    AtMostOne = "AtMostOne"
    One = "One"
    Many = "Many"
    AtLeastOne = "AtLeastOne"
    Empty = "Empty"

    def is_multi(self) -> bool:
        return self in {
            Cardinality.AtLeastOne,
            Cardinality.Many,
        }

    def is_optional(self) -> bool:
        return self in {
            Cardinality.AtMostOne,
            Cardinality.Many,
            Cardinality.Empty,
        }


@final
class PointerKind(StrEnum):
    Link = "Link"
    Property = "Property"

    def is_link(self) -> bool:
        return self is PointerKind.Link


def _mangle_name(name: str) -> str:
    return (
        name.replace("|", "||")
        .replace("&", "&&")
        .replace("::", "|")
        .replace("@", "&")
    )


_unmangle_re_1 = re.compile(r"\|+")


def unmangle_unqual_name(name: str) -> str:
    # Any number of pipes becomes a single ::.
    return _unmangle_re_1.sub("::", name)


# Should be same as gel/edb/schema/objects.py:TYPE_ID_NAMESPACE
_TYPE_ID_NAMESPACE = uuid.UUID("00e50276-2502-11e7-97f2-27fe51238dbd")


def _get_type_id(name: str, cls: str) -> uuid.UUID:
    return uuid.uuid5(_TYPE_ID_NAMESPACE, f"{name}-{cls}")


def get_array_type_id_and_name(
    element: str,
) -> tuple[uuid.UUID, TypeName]:
    type_id = _get_type_id(f"array<{_mangle_name(element)}>", "Array")
    type_name = f"array<{element}>"
    return type_id, SchemaPath(type_name)


def get_range_type_id_and_name(
    element: str,
) -> tuple[uuid.UUID, TypeName]:
    type_id = _get_type_id(f"range<{_mangle_name(element)}>", "Range")
    type_name = f"range<{element}>"
    return type_id, SchemaPath(type_name)


def get_multirange_type_id_and_name(
    element: str,
) -> tuple[uuid.UUID, TypeName]:
    type_id = _get_type_id(
        f"multirange<{_mangle_name(element)}>", "MultiRange"
    )
    type_name = f"multirange<{element}>"
    return type_id, SchemaPath(type_name)


def get_tuple_type_id_and_name(
    elements: Iterable[str],
) -> tuple[uuid.UUID, TypeName]:
    body = ", ".join(elements)
    type_id = _get_type_id(f"tuple<{_mangle_name(body)}>", "Tuple")
    type_name = f"tuple<{body}>"
    return type_id, SchemaPath(type_name)


def get_named_tuple_type_id_and_name(
    elements: dict[str, str],
) -> tuple[uuid.UUID, TypeName]:
    body = ", ".join(f"{n}:{t}" for n, t in elements.items())
    type_id = _get_type_id(f"tuple<{_mangle_name(body)}>", "Tuple")
    type_name = f"tuple<{body}>"
    return type_id, SchemaPath(type_name)


__all__ = (
    "Cardinality",
    "PointerKind",
    "get_array_type_id_and_name",
    "get_multirange_type_id_and_name",
    "get_named_tuple_type_id_and_name",
    "get_range_type_id_and_name",
    "get_tuple_type_id_and_name",
)
