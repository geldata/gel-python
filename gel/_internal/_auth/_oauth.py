# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Any, Optional, TypeVar

import dataclasses

import httpx
import jwt
import logging

import gel
from gel import blocking_client

from . import _base as base
from . import _pkce as pkce_mod
from . import _token_data as td_mod


logger = logging.getLogger("gel.auth")


@dataclasses.dataclass
class AuthorizeData:
    verifier: str
    redirect_url: str


@dataclasses.dataclass
class TokenData(td_mod.TokenData):
    provider_id_token: Optional[str]

    def get_id_token_claims(self) -> Optional[Any]:
        if self.provider_id_token is None:
            return None
        return jwt.decode(
            self.provider_id_token, options={"verify_signature": False}
        )


C = TypeVar("C", bound=httpx.Client | httpx.AsyncClient)


class BaseOAuth(base.BaseClient[C]):
    def __init__(
        self,
        provider_name: str,
        *,
        connection_info: gel.ConnectionInfo,
        **kwargs: Any,
    ) -> None:
        super().__init__(connection_info=connection_info, **kwargs)
        self._provider_name = provider_name

    def authorize(
        self,
        *,
        redirect_to: str,
        redirect_to_on_signup: Optional[str],
        callback_url: Optional[str] = None,
    ) -> AuthorizeData:
        pkce = self._generate_pkce()
        redirect_url = (
            self._client.base_url.join("authorize")
            .copy_set_param("provider", self._provider_name)
            .copy_set_param("redirect_to", redirect_to)
            .copy_set_param("challenge", pkce.challenge)
        )
        if redirect_to_on_signup is not None:
            redirect_url = redirect_url.copy_set_param(
                "redirect_to_on_signup", redirect_to_on_signup
            )
        if callback_url is not None:
            redirect_url = redirect_url.copy_set_param(
                "callback_url", callback_url
            )

        return AuthorizeData(
            verifier=pkce.verifier,
            redirect_url=str(redirect_url),
        )

    async def _get_token(self, *, verifier: str, code: str) -> TokenData:
        pkce = self._pkce_from_verifier(verifier)
        logger.info("exchanging code for token: %s", code)
        return await pkce.internal_exchange_code_for_token(code, cls=TokenData)


class OAuth(BaseOAuth[httpx.Client]):
    def _init_http_client(self, **kwargs: Any) -> httpx.Client:
        return httpx.Client(**kwargs)

    def _generate_pkce(self) -> pkce_mod.PKCE:
        return pkce_mod.generate_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.PKCE:
        return pkce_mod.PKCE(self._client, verifier)

    def get_token(self, *, verifier: str, code: str) -> TokenData:
        return blocking_client.iter_coroutine(
            self._get_token(verifier=verifier, code=code)
        )


def make(
    client: gel.Client, *, provider_name: str, cls: type[OAuth] = OAuth
) -> OAuth:
    return cls(provider_name, connection_info=client.check_connection())


class AsyncOAuth(BaseOAuth[httpx.AsyncClient]):
    def _init_http_client(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(**kwargs)

    def _generate_pkce(self) -> pkce_mod.AsyncPKCE:
        return pkce_mod.generate_async_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.AsyncPKCE:
        return pkce_mod.AsyncPKCE(self._client, verifier)

    async def get_token(self, *, verifier: str, code: str) -> TokenData:
        return await self._get_token(verifier=verifier, code=code)


async def make_async(
    client: gel.AsyncIOClient,
    *,
    provider_name: str,
    cls: type[AsyncOAuth] = AsyncOAuth,
) -> AsyncOAuth:
    return cls(provider_name, connection_info=await client.check_connection())
