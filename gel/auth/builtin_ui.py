#
# This source file is part of the Gel open source project.
#
# Copyright 2025-present MagicStack Inc. and the Gel authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations
from typing import Type, TypeVar, Union

import dataclasses
import logging

import httpx

import gel
from gel import blocking_client

from . import token_data as td_mod
from . import pkce as pkce_mod
from .base import BaseClient

logger = logging.getLogger("gel.auth")


@dataclasses.dataclass
class BuiltinUIResponse:
    verifier: str
    redirect_url: str


C = TypeVar("C", bound=Union[httpx.Client, httpx.AsyncClient])


class BaseBuiltinUI(BaseClient[C]):
    def start_sign_in(self) -> BuiltinUIResponse:
        logger.info("starting sign-in flow")
        pkce = self._generate_pkce()
        redirect_url = self._client.base_url.join(
            f"/ui/signin?challenge={pkce.challenge}"
        )

        return BuiltinUIResponse(
            verifier=pkce.verifier,
            redirect_url=str(redirect_url),
        )

    def start_sign_up(self) -> BuiltinUIResponse:
        logger.info("starting sign-up flow")
        pkce = self._generate_pkce()
        redirect_url = self._client.base_url.join(
            f"/ui/signup?challenge={pkce.challenge}"
        )

        return BuiltinUIResponse(
            verifier=pkce.verifier,
            redirect_url=str(redirect_url),
        )

    async def _get_token(self, *, verifier: str, code: str) -> td_mod.TokenData:
        pkce = self._pkce_from_verifier(verifier)
        logger.info("exchanging code for token: %s", code)
        return await pkce.internal_exchange_code_for_token(code)


class BuiltinUI(BaseBuiltinUI[httpx.Client]):
    def _init_http_client(self, **kwargs) -> httpx.Client:
        return httpx.Client(**kwargs)

    def _generate_pkce(self) -> pkce_mod.BasePKCE:
        return pkce_mod.generate_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.BasePKCE:
        return pkce_mod.PKCE(self._client, verifier)

    def get_token(self, *, verifier: str, code: str) -> td_mod.TokenData:
        return blocking_client.iter_coroutine(
            self._get_token(verifier=verifier, code=code)
        )


def make(client: gel.Client, *, cls: Type[BuiltinUI] = BuiltinUI) -> BuiltinUI:
    return cls(connection_info=client.check_connection())


class AsyncBuiltinUI(BaseBuiltinUI[httpx.AsyncClient]):
    def _init_http_client(self, **kwargs) -> httpx.AsyncClient:
        return httpx.AsyncClient(**kwargs)

    def _generate_pkce(self) -> pkce_mod.BasePKCE:
        return pkce_mod.generate_async_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.BasePKCE:
        return pkce_mod.AsyncPKCE(self._client, verifier)

    async def get_token(self, *, verifier: str, code: str) -> td_mod.TokenData:
        return await self._get_token(verifier=verifier, code=code)


async def make_async(
    client: gel.AsyncIOClient, *, cls: Type[AsyncBuiltinUI] = AsyncBuiltinUI
) -> AsyncBuiltinUI:
    return cls(connection_info=await client.check_connection())
