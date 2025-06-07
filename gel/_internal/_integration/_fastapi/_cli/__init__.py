# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

"""FastAPI CLI extensions"""

from ._patch import maybe_patch_fastapi_cli


__all__ = ("maybe_patch_fastapi_cli",)
