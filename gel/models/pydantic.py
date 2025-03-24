# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from typing import (
    ClassVar,
    NamedTuple,
)

import pydantic


class GelMetadata(NamedTuple):
    schema_name: str


class Exclusive:
    pass


class BaseGelModel(pydantic.BaseModel):
    __gel_metadata__: ClassVar[GelMetadata]
