# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

import importlib.util
import warnings

if importlib.util.find_spec("fastapi") is None:
    warnings.warn("FastAPI is not installed.", stacklevel=1)
else:
    from gel._internal._integration._fastapi._client import (
        gelify,
        Client,
        BlockingIOClient,
    )


__all__ = ["gelify", "Client", "BlockingIOClient"]
