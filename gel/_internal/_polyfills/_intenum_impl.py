# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import enum as _enum


class IntEnum(int, _enum.Enum):
    __str__ = int.__str__
    __format__ = int.__format__  # type: ignore [assignment]


__all__ = ("IntEnum",)
