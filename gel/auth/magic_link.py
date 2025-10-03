# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from gel._internal._auth._magic_link import (
    AsyncLegacyMagicLink,
    AsyncMagicLink,
    AsyncMagicCode,
    AuthenticateCodeResultResponse,
    AuthenticateCodeFailedResponse,
    AuthenticateLinkResultResponse,
    AuthenticateLinkFailedResponse,
    LegacyMagicLink,
    MagicLink,
    MagicLinkSentResponse,
    MagicLinkFailedResponse,
    MagicCode,
    MagicCodeSentResponse,
    MagicCodeFailedResponse,
    make,
    make_async,
    VerificationMethod,
)

__all__ = [
    "AsyncLegacyMagicLink",
    "AsyncMagicLink",
    "AsyncMagicCode",
    "AuthenticateCodeResultResponse",
    "AuthenticateCodeFailedResponse",
    "AuthenticateLinkResultResponse",
    "AuthenticateLinkFailedResponse",
    "LegacyMagicLink",
    "MagicLink",
    "MagicLinkSentResponse",
    "MagicLinkFailedResponse",
    "MagicCode",
    "MagicCodeSentResponse",
    "MagicCodeFailedResponse",
    "make",
    "make_async",
    "VerificationMethod",
]
