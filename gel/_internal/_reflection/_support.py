# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from typing import NamedTuple


class QualName(NamedTuple):
    module: str
    name: str


def parse_name(name: str) -> QualName:
    # Assume the names are already validated to be properly formed
    # alphanumeric identifiers that may be prefixed by a module. If the module
    # is present assume it is safe to drop it (currently only defualt module
    # is allowed).

    # Split on module separator. Potentially if we ever handle more unusual
    # names, there may be more processing done.
    return QualName(*name.rsplit('::', 1))
