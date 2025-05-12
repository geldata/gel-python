# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from ._enums import (
    Cardinality,
    OperatorKind,
    PointerKind,
    TypeKind,
    TypeModifier,
    SchemaPart,
)

from ._support import (
    parse_name,
    SchemaPath,
)

from ._casts import (
    CastMatrix,
    fetch_casts,
)

from ._operators import (
    Operator,
    OperatorMatrix,
    fetch_operators,
)

from ._types import (
    Type,
    AnyType,
    ArrayType,
    InheritingType,
    NamedTupleType,
    ObjectType,
    Pointer,
    PrimitiveType,
    ScalarType,
    TupleType,
    fetch_types,
    is_array_type,
    is_multi_range_type,
    is_named_tuple_type,
    is_non_enum_scalar_type,
    is_object_type,
    is_pseudo_type,
    is_primitive_type,
    is_range_type,
    is_scalar_type,
    is_tuple_type,
    is_link,
    is_property,
)

from ._modules import (
    fetch_modules,
)

__all__ = (
    "Type",
    "AnyType",
    "ArrayType",
    "Cardinality",
    "CastMatrix",
    "InheritingType",
    "NamedTupleType",
    "ObjectType",
    "Operator",
    "OperatorKind",
    "OperatorMatrix",
    "Pointer",
    "PointerKind",
    "PrimitiveType",
    "ScalarType",
    "SchemaPart",
    "SchemaPath",
    "TupleType",
    "TypeKind",
    "TypeModifier",
    "parse_name",
    "fetch_operators",
    "fetch_casts",
    "fetch_modules",
    "fetch_types",
    "is_array_type",
    "is_multi_range_type",
    "is_named_tuple_type",
    "is_non_enum_scalar_type",
    "is_object_type",
    "is_pseudo_type",
    "is_primitive_type",
    "is_range_type",
    "is_scalar_type",
    "is_tuple_type",
    "is_link",
    "is_property", "INFIX_OPERATOR_MAP", "PREFIX_OPERATOR_MAP",
)
