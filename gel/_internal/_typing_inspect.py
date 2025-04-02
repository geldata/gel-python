# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


import typing
from typing import (
    _GenericAlias,  # type: ignore
    ClassVar,
)

from types import (
    GenericAlias,
)


def is_classvar(t: type) -> bool:
    return t is ClassVar or (
        _is_genericalias(t) and typing.get_origin(t) is ClassVar
    )


def _is_genericalias(t) -> bool:
    return isinstance(t, (GenericAlias, _GenericAlias))
