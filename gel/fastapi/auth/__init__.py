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
from typing import Optional, TYPE_CHECKING

import datetime

import fastapi
import jwt
from fastapi import security

import gel

from .. import client as client_mod

if TYPE_CHECKING:
    from .email_password import EmailPassword


# def get_client_with_auth_token(client: C, *, auth_token: Optional[str]) -> C:
#     if auth_token:
#         return client.with_globals({"ext::auth::client_token": auth_token})
#     else:
#         return client


class GelAuth(client_mod.AsyncIOLifespan):
    auth_path_prefix: str = "/auth"
    auth_cookie_name: str = "gel_auth_token"
    verifier_cookie_name: str = "gel_verifier"
    secure_cookie: bool = True
    email_password: EmailPassword

    _pkce_verifier: Optional[security.APIKeyCookie] = None

    def __init__(self, client: gel.AsyncIOClient) -> None:
        super().__init__(client)

        from .email_password import EmailPassword

        self.email_password = EmailPassword(self)

    def get_unchecked_exp(self, token: str) -> Optional[datetime.datetime]:
        jwt_payload = jwt.decode(token, options={"verify_signature": False})
        if "exp" not in jwt_payload:
            return None
        return datetime.datetime.fromtimestamp(
            jwt_payload["exp"], tz=datetime.timezone.utc
        )

    def set_auth_cookie(self, token: str, response: fastapi.Response) -> None:
        exp = self.get_unchecked_exp(token)
        response.set_cookie(
            key=self.auth_cookie_name,
            value=token,
            httponly=True,
            secure=self.secure_cookie,
            samesite="lax",
            expires=exp,
        )

    def set_verifier_cookie(
        self, verifier: str, response: fastapi.Response
    ) -> None:
        response.set_cookie(
            key=self.verifier_cookie_name,
            value=verifier,
            httponly=True,
            secure=self.secure_cookie,
            samesite="lax",
            expires=int(datetime.timedelta(days=7).total_seconds()),
        )

    @property
    def pkce_verifier(self) -> security.APIKeyCookie:
        if self._pkce_verifier is None:
            self._pkce_verifier = security.APIKeyCookie(
                name=self.verifier_cookie_name,
                description="The cookie as the PKCE verifier",
                auto_error=False,
            )
        return self._pkce_verifier

    def install(self, app: fastapi.FastAPI) -> None:
        router = fastapi.APIRouter(
            prefix=self.auth_path_prefix,
            tags=["Gel Auth"],
            lifespan=self,
        )
        self.email_password.install(router)
        app.include_router(router)


authify = client_mod.make_gelify(gel.create_async_client, GelAuth)
