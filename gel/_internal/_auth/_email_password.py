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
from typing import Any, Optional, TYPE_CHECKING, TypeVar

import dataclasses
import logging

import httpx

import gel
from gel import blocking_client

from . import _token_data as td_mod
from . import _pkce as pkce_mod
from ._base import BaseServerFailedResponse, BaseClient

if TYPE_CHECKING:
    import uuid


logger = logging.getLogger("gel.auth")


@dataclasses.dataclass
class SignUpCompleteResponse:
    verifier: str
    token_data: td_mod.TokenData
    identity_id: uuid.UUID


@dataclasses.dataclass
class SignUpVerificationRequiredResponse:
    verifier: str
    identity_id: Optional[uuid.UUID]


@dataclasses.dataclass
class SignUpFailedResponse(BaseServerFailedResponse):
    verifier: str


SignUpResponse = (
    SignUpCompleteResponse
    | SignUpVerificationRequiredResponse
    | SignUpFailedResponse
)


@dataclasses.dataclass
class SignInCompleteResponse:
    verifier: str
    token_data: td_mod.TokenData
    identity_id: uuid.UUID


@dataclasses.dataclass
class SignInVerificationRequiredResponse:
    verifier: str
    identity_id: Optional[uuid.UUID]


@dataclasses.dataclass
class SignInFailedResponse(BaseServerFailedResponse):
    verifier: str


SignInResponse = (
    SignInCompleteResponse
    | SignInVerificationRequiredResponse
    | SignInFailedResponse
)


@dataclasses.dataclass
class EmailVerificationCompleteResponse:
    token_data: td_mod.TokenData


class EmailVerificationMissingProofResponse:
    pass


class EmailVerificationFailedResponse(BaseServerFailedResponse):
    pass


EmailVerificationResponse = (
    EmailVerificationCompleteResponse
    | EmailVerificationMissingProofResponse
    | EmailVerificationFailedResponse
)


@dataclasses.dataclass
class SendPasswordResetEmailCompleteResponse:
    verifier: str


@dataclasses.dataclass
class SendPasswordResetEmailFailedResponse(BaseServerFailedResponse):
    verifier: str


SendPasswordResetEmailResponse = (
    SendPasswordResetEmailCompleteResponse
    | SendPasswordResetEmailFailedResponse
)


@dataclasses.dataclass
class PasswordResetCompleteResponse:
    token_data: td_mod.TokenData


class PasswordResetMissingProofResponse:
    pass


class PasswordResetFailedResponse(BaseServerFailedResponse):
    pass


PasswordResetResponse = (
    PasswordResetCompleteResponse
    | PasswordResetMissingProofResponse
    | PasswordResetFailedResponse
)

C = TypeVar("C", bound=httpx.Client | httpx.AsyncClient)


class BaseEmailPassword(BaseClient[C]):
    def __init__(
        self, *, connection_info: gel.ConnectionInfo, **kwargs: Any
    ) -> None:
        self.provider = "builtin::local_emailpassword"
        super().__init__(connection_info=connection_info, **kwargs)

    async def _sign_up(
        self, email: str, password: str, *, verify_url: str
    ) -> SignUpResponse:
        logger.info("signing up user: %s", email)
        pkce = self._generate_pkce()
        register_response = await self._http_request(
            "POST",
            "/register",
            json={
                "email": email,
                "password": password,
                "verify_url": verify_url,
                "provider": self.provider,
                "challenge": pkce.challenge,
            },
        )
        try:
            register_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("register error: %s", e)
            return SignUpFailedResponse(
                verifier=pkce.verifier,
                status_code=e.response.status_code,
                message=e.response.text,
            )
        register_json = register_response.json()
        if "error" in register_json:
            error = register_json["error"]
            logger.error("register error: %s", error)
            return SignUpFailedResponse(
                verifier=pkce.verifier,
                status_code=register_response.status_code,
                message=error,
            )
        elif "code" in register_json:
            code = register_json["code"]
            logger.info("exchanging code for token: %s", code)
            token_data = await pkce.internal_exchange_code_for_token(code)

            logger.debug("PKCE verifier: %s", pkce.verifier)
            logger.debug("token data: %s", token_data)
            return SignUpCompleteResponse(
                verifier=pkce.verifier,
                token_data=token_data,
                identity_id=token_data.identity_id,
            )
        else:
            logger.info(
                "no code in register response, assuming verification required"
            )
            logger.debug("PKCE verifier: %s", pkce.verifier)
            return SignUpVerificationRequiredResponse(
                verifier=pkce.verifier,
                identity_id=register_json.get("identity_id"),
            )

    async def _sign_in(self, email: str, password: str) -> SignInResponse:
        logger.info("signing in user: %s", email)
        pkce = self._generate_pkce()
        sign_in_response = await self._http_request(
            "POST",
            "/authenticate",
            json={
                "email": email,
                "provider": self.provider,
                "password": password,
                "challenge": pkce.challenge,
            },
        )
        try:
            sign_in_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("sign in error: %s", e)
            return SignInFailedResponse(
                verifier=pkce.verifier,
                status_code=e.response.status_code,
                message=e.response.text,
            )
        sign_in_json = sign_in_response.json()
        if "error" in sign_in_json:
            error = sign_in_json["error"]
            logger.error("sign in error: %s", error)
            return SignInFailedResponse(
                verifier=pkce.verifier,
                status_code=sign_in_response.status_code,
                message=error,
            )
        elif "code" in sign_in_json:
            code = sign_in_json["code"]
            logger.info("exchanging code for token: %s", code)
            token_data = await pkce.internal_exchange_code_for_token(code)

            logger.debug("PKCE verifier: %s", pkce.verifier)
            logger.debug("token data: %s", token_data)
            return SignInCompleteResponse(
                verifier=pkce.verifier,
                token_data=token_data,
                identity_id=token_data.identity_id,
            )
        else:
            logger.info(
                "no code in sign in response, assuming verification required"
            )
            logger.debug("PKCE verifier: %s", pkce.verifier)
            return SignInVerificationRequiredResponse(
                verifier=pkce.verifier,
                identity_id=sign_in_json.get("identity_id"),
            )

    async def _verify_email(
        self, verification_token: str, verifier: Optional[str]
    ) -> EmailVerificationResponse:
        logger.info("verifying email")
        verify_response = await self._http_request(
            "POST",
            "/verify",
            json={
                "verification_token": verification_token,
                "provider": self.provider,
            },
        )
        try:
            verify_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("verify error: %s", e)
            return EmailVerificationFailedResponse(
                status_code=e.response.status_code,
                message=e.response.text,
            )
        verify_json = verify_response.json()
        if "error" in verify_json:
            error = verify_json["error"]
            logger.error("verify error: %s", error)
            return EmailVerificationFailedResponse(
                status_code=verify_response.status_code,
                message=error,
            )
        elif "code" in verify_json:
            code = verify_json["code"]
            if verifier is None:
                return EmailVerificationMissingProofResponse()

            pkce = self._pkce_from_verifier(verifier)
            logger.info("exchanging code for token: %s", code)
            token_data = await pkce.internal_exchange_code_for_token(code)

            logger.debug("PKCE verifier: %s", pkce.verifier)
            logger.debug("token data: %s", token_data)
            return EmailVerificationCompleteResponse(token_data=token_data)
        else:
            logger.error("no code in verify response: %r", verify_json)
            return EmailVerificationMissingProofResponse()

    async def _send_password_reset_email(
        self, email: str, *, reset_url: str
    ) -> SendPasswordResetEmailResponse:
        logger.info("sending password reset email: %s", email)
        pkce = self._generate_pkce()
        reset_response = await self._http_request(
            "POST",
            "/send-reset-email",
            json={
                "email": email,
                "provider": self.provider,
                "challenge": pkce.challenge,
                "reset_url": reset_url,
            },
        )
        try:
            reset_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("reset error: %s", e)
            return SendPasswordResetEmailFailedResponse(
                verifier=pkce.verifier,
                status_code=e.response.status_code,
                message=e.response.text,
            )
        reset_json = reset_response.json()
        if "error" in reset_json:
            error = reset_json["error"]
            logger.error("reset error: %s", error)
            return SendPasswordResetEmailFailedResponse(
                verifier=pkce.verifier,
                status_code=reset_response.status_code,
                message=error,
            )
        else:
            logger.debug("PKCE verifier: %s", pkce.verifier)
            return SendPasswordResetEmailCompleteResponse(
                verifier=pkce.verifier
            )

    async def _reset_password(
        self, reset_token: str, verifier: Optional[str], password: str
    ) -> PasswordResetResponse:
        logger.info("resetting password")
        reset_response = await self._http_request(
            "POST",
            "/reset-password",
            json={
                "provider": self.provider,
                "reset_token": reset_token,
                "password": password,
            },
        )
        try:
            reset_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("reset error: %s", e)
            return PasswordResetFailedResponse(
                status_code=e.response.status_code,
                message=e.response.text,
            )
        reset_json = reset_response.json()
        if "error" in reset_json:
            error = reset_json["error"]
            logger.error("reset error: %s", error)
            return PasswordResetFailedResponse(
                status_code=reset_response.status_code,
                message=error,
            )
        elif "code" in reset_json:
            code = reset_json["code"]
            if verifier is None:
                return PasswordResetMissingProofResponse()

            pkce = self._pkce_from_verifier(verifier)
            logger.info("exchanging code for token: %s", code)
            token_data = await pkce.internal_exchange_code_for_token(code)
            return PasswordResetCompleteResponse(token_data=token_data)
        else:
            logger.error("no code in reset response: %r", reset_json)
            return PasswordResetMissingProofResponse()


class EmailPassword(BaseEmailPassword[httpx.Client]):
    def _init_http_client(self, **kwargs: Any) -> httpx.Client:
        return httpx.Client(**kwargs)

    def _generate_pkce(self) -> pkce_mod.PKCE:
        return pkce_mod.generate_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.PKCE:
        return pkce_mod.PKCE(self._client, verifier)

    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        return self._client.send(request)

    def sign_up(
        self, email: str, password: str, *, verify_url: str
    ) -> SignUpResponse:
        return blocking_client.iter_coroutine(
            self._sign_up(email, password, verify_url=verify_url)
        )

    def sign_in(self, email: str, password: str) -> SignInResponse:
        return blocking_client.iter_coroutine(self._sign_in(email, password))

    def verify_email(
        self, verification_token: str, verifier: Optional[str]
    ) -> EmailVerificationResponse:
        return blocking_client.iter_coroutine(
            self._verify_email(verification_token, verifier)
        )

    def send_password_reset_email(
        self, email: str, *, reset_url: str
    ) -> SendPasswordResetEmailResponse:
        return blocking_client.iter_coroutine(
            self._send_password_reset_email(email, reset_url=reset_url)
        )

    def reset_password(
        self, reset_token: str, verifier: Optional[str], password: str
    ) -> PasswordResetResponse:
        return blocking_client.iter_coroutine(
            self._reset_password(reset_token, verifier, password)
        )


def make(
    client: gel.Client, *, cls: type[EmailPassword] = EmailPassword
) -> EmailPassword:
    return cls(connection_info=client.check_connection())


class AsyncEmailPassword(BaseEmailPassword[httpx.AsyncClient]):
    def _init_http_client(self, **kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(**kwargs)

    def _generate_pkce(self) -> pkce_mod.AsyncPKCE:
        return pkce_mod.generate_async_pkce(self._client)

    def _pkce_from_verifier(self, verifier: str) -> pkce_mod.AsyncPKCE:
        return pkce_mod.AsyncPKCE(self._client, verifier)

    async def _send_http_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        return await self._client.send(request)

    async def sign_up(
        self, email: str, password: str, *, verify_url: str
    ) -> SignUpResponse:
        return await self._sign_up(email, password, verify_url=verify_url)

    async def sign_in(self, email: str, password: str) -> SignInResponse:
        return await self._sign_in(email, password)

    async def verify_email(
        self, verification_token: str, verifier: Optional[str]
    ) -> EmailVerificationResponse:
        return await self._verify_email(verification_token, verifier)

    async def send_password_reset_email(
        self, email: str, *, reset_url: str
    ) -> SendPasswordResetEmailResponse:
        return await self._send_password_reset_email(
            email, reset_url=reset_url
        )

    async def reset_password(
        self, reset_token: str, verifier: Optional[str], password: str
    ) -> PasswordResetResponse:
        return await self._reset_password(reset_token, verifier, password)


async def make_async(
    client: gel.AsyncIOClient,
    *,
    cls: type[AsyncEmailPassword] = AsyncEmailPassword,
) -> AsyncEmailPassword:
    return cls(connection_info=await client.check_connection())
