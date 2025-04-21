# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from typing import TypeVar, Union


class Type:
    pass


class ScalarType(Type):
    pass


class ObjectType(Type):
    pass


class EnumType(Type):
    pass


AnyType = Union[ScalarType, ObjectType, EnumType]
