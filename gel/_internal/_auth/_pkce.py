#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Generic, TypeVar

import base64
import dataclasses
import hashlib
import logging
import secrets

import httpx

from gel import blocking_client

from . import _token_data as token_data


logger = logging.getLogger("gel.auth")
C = TypeVar("C", bound=httpx.Client | httpx.AsyncClient)


class BasePKCE(Generic[C]):
    def __init__(self, http_client: C, verifier: str):
        self._http_client = http_client
        self._verifier = verifier
        self._challenge = (
            base64.urlsafe_b64encode(
                hashlib.sha256(verifier.encode()).digest()
            )
            .rstrip(b"=")
            .decode()
        )

    @property
    def verifier(self) -> str:
        return self._verifier

    @property
    def challenge(self) -> str:
        return self._challenge

    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        raise NotImplementedError

    async def internal_exchange_code_for_token(
        self, code: str
    ) -> token_data.TokenData:
        request = self._http_client.build_request(
            "GET",
            "/token",
            params={
                "code": code,
                "verifier": self._verifier,
            },
        )
        logger.info("exchanging code for token: %s", request.url)
        token_response = await self._send_http_request(request)

        logger.debug(
            "token response: [%d] %s",
            token_response.status_code,
            token_response.text,
        )
        token_response.raise_for_status()
        token_json = token_response.json()
        args = {
            field.name: token_json[field.name]
            for field in dataclasses.fields(token_data.TokenData)
        }
        return token_data.TokenData(**args)


class PKCE(BasePKCE[httpx.Client]):
    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        return self._http_client.send(request)

    def exchange_code_for_token(self, code: str) -> token_data.TokenData:
        return blocking_client.iter_coroutine(
            self.internal_exchange_code_for_token(code)
        )


def generate_pkce(
    http_client: httpx.Client,
) -> PKCE:
    verifier = secrets.token_urlsafe(32)
    return PKCE(http_client, verifier)


class AsyncPKCE(BasePKCE[httpx.AsyncClient]):
    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        return await self._http_client.send(request)

    async def exchange_code_for_token(self, code: str) -> token_data.TokenData:
        return await self.internal_exchange_code_for_token(code)


def generate_async_pkce(
    http_client: httpx.AsyncClient,
) -> AsyncPKCE:
    verifier = secrets.token_urlsafe(32)
    return AsyncPKCE(http_client, verifier)
