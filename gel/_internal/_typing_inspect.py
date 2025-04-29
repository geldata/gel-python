# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from typing import Any, ClassVar, get_origin
from typing import _GenericAlias  # type: ignore
from types import GenericAlias


def is_classvar(t: Any) -> bool:
    return t is ClassVar or (_is_genericalias(t) and get_origin(t) is ClassVar)


def _is_genericalias(t: Any) -> bool:
    return isinstance(t, (GenericAlias, _GenericAlias))
