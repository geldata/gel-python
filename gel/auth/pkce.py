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

import base64
import hashlib
import logging
import secrets
from urllib.parse import urljoin

import httpx

from .token_data import TokenData


logger = logging.getLogger("gel_auth_core")


class PKCE:
    def __init__(self, verifier: str, *, base_url: str):
        self._base_url = base_url
        self._verifier = verifier
        self._challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
            .rstrip(b"=")
            .decode()
        )

    @property
    def verifier(self) -> str:
        return self._verifier

    @property
    def challenge(self) -> str:
        return self._challenge

    async def exchange_code_for_token(self, code: str) -> TokenData:
        async with httpx.AsyncClient() as http_client:
            url = urljoin(self._base_url, "token")
            logger.info(f"Exchanging code for token: {url}")
            token_response = await http_client.get(
                url,
                params={
                    "code": code,
                    "verifier": self._verifier,
                },
            )

            logger.info(f"Token response: {token_response.text}")
            token_response.raise_for_status()
            token_json = token_response.json()
            return TokenData(
                auth_token=token_json["auth_token"],
                identity_id=token_json["identity_id"],
                provider_token=token_json["provider_token"],
                provider_refresh_token=token_json["provider_refresh_token"],
            )


def generate_pkce(base_url: str) -> PKCE:
    verifier = secrets.token_urlsafe(32)
    return PKCE(verifier, base_url=base_url)
