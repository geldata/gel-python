# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    Literal,
    Generic,
    Optional,
    TypeVar,
    Union,
)

import dataclasses
import uuid

from gel import abstract
from . import _enums as enums
from . import _query


def fetch_modules(
    db: abstract.ReadOnlyExecutor,
    schema_part: enums.SchemaPart,
) -> list[str]:
    builtin = schema_part is enums.SchemaPart.STD
    modules: list[str] = db.query(_query.MODULES, builtin=builtin)
    return modules
