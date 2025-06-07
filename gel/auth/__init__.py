# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

import importlib.util
import warnings


if importlib.util.find_spec("httpx") is None:
    warnings.warn("The 'httpx' package is not installed.", stacklevel=1)
else:
    from gel._internal._auth._token_data import TokenData
    from gel._internal._auth._pkce import (
        PKCE,
        generate_pkce,
        AsyncPKCE,
        generate_async_pkce,
    )

    from . import builtin_ui
    from . import email_password

__all__ = [
    "builtin_ui",
    "email_password",
    "TokenData",
    "PKCE",
    "generate_pkce",
    "AsyncPKCE",
    "generate_async_pkce",
]
