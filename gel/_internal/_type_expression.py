# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

import typing


class Intersection:
    lhs: typing.ClassVar[type]
    rhs: typing.ClassVar[type]


class Union:
    lhs: typing.ClassVar[type]
    rhs: typing.ClassVar[type]
