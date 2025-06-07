# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Optional, TYPE_CHECKING

import dataclasses

if TYPE_CHECKING:
    import uuid


@dataclasses.dataclass
class TokenData:
    auth_token: str
    identity_id: uuid.UUID
    provider_token: Optional[str]
    provider_refresh_token: Optional[str]
