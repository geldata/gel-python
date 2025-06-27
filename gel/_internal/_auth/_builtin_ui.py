#
# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any, Optional, TYPE_CHECKING, TypeVar

import dataclasses
import logging

import httpx

from gel import blocking_client

from . import _base as base
from . import _pkce as pkce_mod
from . import _token_data as td_mod

if TYPE_CHECKING:
    import gel

logger = logging.getLogger("gel.auth")


@dataclasses.dataclass
class BuiltinUIResponse:
    verifier: str
    redirect_url: str


C = TypeVar("C", bound=httpx.Client | httpx.AsyncClient)


class BaseBuiltinUI(base.BaseClient[C]):
    def start_sign_in(
        self, *, error: Optional[str] = None
    ) -> BuiltinUIResponse:
        logger.info("starting sign-in flow")
        pkce = self._generate_pkce()
        redirect_url = self._client.base_url.join("ui/signin").copy_set_param(
            "challenge", pkce.challenge
        )
        if error is not None:
            redirect_url = redirect_url.copy_set_param("error", error)

        return BuiltinUIResponse(
            verifier=pkce.verifier,
            redirect_url=str(redirect_url),
        )

    def start_sign_up(self) -> BuiltinUIResponse:
        logger.info("starting sign-up flow")
        pkce = self._generate_pkce()
        redirect_url = self._client.base_url.join(
            f"ui/signup?challenge={pkce.challenge}"
        )

        return BuiltinUIResponse(
            verifier=pkce.verifier,
            redirect_url=str(redirect_url),
        )

    async def _get_token(
        self, *, verifier: str, code: str
    ) -> td_mod.TokenData:
        pkce = self._pkce_from_verifier(verifier)
        logger.info("exchanging code for token: %s", code)
        return await pkce.internal_exchange_code_for_token(code)


class BuiltinUI(BaseBuiltinUI[httpx.Client]):
    def _init_http_client(self, **kwargs: Any) -> httpx.Client:
        return httpx.Client(**kwargs)

    def _generate_pkce(self) -> pkce_mod.PKCE:
        return pkce_mod.generate_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.PKCE:
        return pkce_mod.PKCE(self._client, verifier)

    def get_token(self, *, verifier: str, code: str) -> td_mod.TokenData:
        return blocking_client.iter_coroutine(
            self._get_token(verifier=verifier, code=code)
        )


def make(client: gel.Client, *, cls: type[BuiltinUI] = BuiltinUI) -> BuiltinUI:
    return cls(connection_info=client.check_connection())


class AsyncBuiltinUI(BaseBuiltinUI[httpx.AsyncClient]):
    def _init_http_client(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(**kwargs)

    def _generate_pkce(self) -> pkce_mod.AsyncPKCE:
        return pkce_mod.generate_async_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.AsyncPKCE:
        return pkce_mod.AsyncPKCE(self._client, verifier)

    async def get_token(self, *, verifier: str, code: str) -> td_mod.TokenData:
        return await self._get_token(verifier=verifier, code=code)


async def make_async(
    client: gel.AsyncIOClient, *, cls: type[AsyncBuiltinUI] = AsyncBuiltinUI
) -> AsyncBuiltinUI:
    return cls(connection_info=await client.check_connection())
