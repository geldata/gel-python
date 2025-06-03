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
from typing import Annotated, Awaitable, Callable, Optional

import http
import logging

import pydantic
import fastapi
from fastapi import responses

from gel.auth import email_password as core

from . import GelAuth


logger = logging.getLogger("gel.auth")


class SignUpBody(pydantic.BaseModel):
    email: str
    password: str


class SignInBody(pydantic.BaseModel):
    email: str
    password: str


class VerifyBody(pydantic.BaseModel):
    verification_token: str


class SendPasswordResetBody(pydantic.BaseModel):
    email: str


class ResetPasswordBody(pydantic.BaseModel):
    reset_token: str
    password: str


OnSignUpComplete = Callable[
    [core.SignUpCompleteResponse], Awaitable[fastapi.Response]
]
OnSignUpVerificationRequired = Callable[
    [core.SignUpVerificationRequiredResponse], Awaitable[fastapi.Response]
]
OnSignUpFailed = Callable[
    [core.SignUpFailedResponse], Awaitable[fastapi.Response]
]
OnSignInComplete = Callable[
    [core.SignInCompleteResponse], Awaitable[fastapi.Response]
]
OnSignInVerificationRequired = Callable[
    [core.SignInVerificationRequiredResponse], Awaitable[fastapi.Response]
]
OnSignInFailed = Callable[
    [core.SignInFailedResponse], Awaitable[fastapi.Response]
]
OnEmailVerificationComplete = Callable[
    [core.EmailVerificationCompleteResponse], Awaitable[fastapi.Response]
]
OnEmailVerificationMissingProof = Callable[
    [core.EmailVerificationMissingProofResponse], Awaitable[fastapi.Response]
]
OnEmailVerificationFailed = Callable[
    [core.EmailVerificationFailedResponse], Awaitable[fastapi.Response]
]
OnSendPasswordResetEmailComplete = Callable[
    [core.SendPasswordResetEmailCompleteResponse], Awaitable[fastapi.Response]
]
OnSendPasswordResetEmailFailed = Callable[
    [core.SendPasswordResetEmailFailedResponse], Awaitable[fastapi.Response]
]
OnResetPasswordComplete = Callable[
    [core.PasswordResetCompleteResponse], Awaitable[fastapi.Response]
]
OnResetPasswordMissingProof = Callable[
    [core.PasswordResetMissingProofResponse], Awaitable[fastapi.Response]
]
OnResetPasswordFailed = Callable[
    [core.PasswordResetFailedResponse], Awaitable[fastapi.Response]
]


class EmailPassword:
    _auth: GelAuth

    # Sign-up
    sign_up_path: str = "/register"
    sign_up_name: str = "gel.auth.email_password.sign_up"
    _on_sign_up_complete: Optional[OnSignUpComplete] = None
    _on_sign_up_verification_required: Optional[
        OnSignUpVerificationRequired
    ] = None
    _on_sign_up_failed: Optional[OnSignUpFailed] = None

    # Sign-in
    sign_in_path: str = "/authenticate"
    sign_in_name: str = "gel.auth.email_password.sign_in"
    _on_sign_in_complete: Optional[OnSignInComplete] = None
    _on_sign_in_verification_required: Optional[
        OnSignInVerificationRequired
    ] = None
    _on_sign_in_failed: Optional[OnSignInFailed] = None

    # Email verification
    email_verification_path: str = "/verify"
    email_verification_name: str = "gel.auth.email_password.email_verification"
    _on_email_verification_complete: Optional[OnEmailVerificationComplete] = (
        None
    )
    _on_email_verification_missing_proof: Optional[
        OnEmailVerificationMissingProof
    ] = None
    _on_email_verification_failed: Optional[OnEmailVerificationFailed] = None

    # Send password reset
    send_password_reset_email_path: str = "/send-password-reset"
    send_password_reset_email_name: str = (
        "gel.auth.email_password.send_password_reset"
    )
    reset_password_page_name: Optional[str] = None
    _on_send_password_reset_email_complete: Optional[
        OnSendPasswordResetEmailComplete
    ] = None
    _on_send_password_reset_email_failed: Optional[
        OnSendPasswordResetEmailFailed
    ] = None

    # Reset password
    reset_password_path: str = "/reset-password"
    reset_password_name: str = "gel.auth.email_password.reset_password"
    _on_reset_password_complete: Optional[OnResetPasswordComplete] = None
    _on_reset_password_missing_proof: Optional[OnResetPasswordMissingProof] = (
        None
    )
    _on_reset_password_failed: Optional[OnResetPasswordFailed] = None

    def __init__(self, auth: GelAuth):
        self._auth = auth

    def _not_implemented(self, method: str) -> fastapi.Response:
        return responses.JSONResponse(
            status_code=http.HTTPStatus.NOT_IMPLEMENTED,
            content={"error": "not implemented", "method": method},
        )

    def on_sign_up_complete(self, func: OnSignUpComplete) -> OnSignUpComplete:
        self._on_sign_up_complete = func
        return func

    async def handle_sign_up_complete(
        self,
        result: core.SignUpCompleteResponse,
    ) -> fastapi.Response:
        if self._on_sign_up_complete is None:
            return self._not_implemented("on_sign_up_complete")

        response = await self._on_sign_up_complete(result)
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    def on_sign_up_verification_required(
        self, func: OnSignUpVerificationRequired
    ) -> OnSignUpVerificationRequired:
        self._on_sign_up_verification_required = func
        return func

    async def handle_sign_up_verification_required(
        self,
        result: core.SignUpVerificationRequiredResponse,
    ) -> fastapi.Response:
        if self._on_sign_up_verification_required is None:
            return self._not_implemented("on_sign_up_verification_required")

        response = await self._on_sign_up_verification_required(result)
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def on_sign_up_failed(self, func: OnSignUpFailed) -> OnSignUpFailed:
        self._on_sign_up_failed = func
        return func

    async def handle_sign_up_failed(
        self,
        result: core.SignUpFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] sign up failed: %s", result.status_code, result.message
        )
        logger.debug("%r", result)

        if self._on_sign_up_failed is None:
            return self._not_implemented("on_sign_up_failed")

        response = await self._on_sign_up_failed(result)
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def install_sign_up(self, router: fastapi.APIRouter) -> None:
        @router.post(self.sign_up_path, name=self.sign_up_name)
        async def sign_up(
            sign_up_body: Annotated[SignUpBody, fastapi.Form()],
            request: fastapi.Request,
        ) -> fastapi.Response:
            client = await core.make_async(self._auth.client)
            result = await client.sign_up(
                sign_up_body.email,
                sign_up_body.password,
                verify_url=str(request.url_for(self.email_verification_name)),
            )
            if isinstance(result, core.SignUpCompleteResponse):
                return await self.handle_sign_up_complete(result)
            elif isinstance(result, core.SignUpVerificationRequiredResponse):
                return await self.handle_sign_up_verification_required(result)
            elif isinstance(result, core.SignUpFailedResponse):
                return await self.handle_sign_up_failed(result)
            else:
                raise AssertionError("Invalid sign up response")

    def on_sign_in_complete(self, func: OnSignInComplete) -> OnSignInComplete:
        self._on_sign_in_complete = func
        return func

    async def handle_sign_in_complete(
        self,
        result: core.SignInCompleteResponse,
    ) -> fastapi.Response:
        if self._on_sign_in_complete is None:
            return self._not_implemented("on_sign_in_complete")

        response = await self._on_sign_in_complete(result)
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    def on_sign_in_verification_required(
        self, func: OnSignInVerificationRequired
    ) -> OnSignInVerificationRequired:
        self._on_sign_in_verification_required = func
        return func

    async def handle_sign_in_verification_required(
        self,
        result: core.SignInVerificationRequiredResponse,
    ) -> fastapi.Response:
        if self._on_sign_in_verification_required is None:
            return self._not_implemented("on_sign_in_verification_required")

        response = await self._on_sign_in_verification_required(result)
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def on_sign_in_failed(self, func: OnSignInFailed) -> OnSignInFailed:
        self._on_sign_in_failed = func
        return func

    async def handle_sign_in_failed(
        self,
        result: core.SignInFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] sign in failed: %s", result.status_code, result.message
        )
        logger.debug("%r", result)

        if self._on_sign_in_failed is None:
            return self._not_implemented("on_sign_in_failed")

        response = await self._on_sign_in_failed(result)
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def install_sign_in(self, router: fastapi.APIRouter) -> None:
        @router.post(self.sign_in_path, name=self.sign_in_name)
        async def sign_in(
            sign_in_body: Annotated[SignInBody, fastapi.Form()],
        ):
            client = await core.make_async(self._auth.client)
            result = await client.sign_in(
                sign_in_body.email, sign_in_body.password
            )
            if isinstance(result, core.SignInCompleteResponse):
                return await self.handle_sign_in_complete(result)
            elif isinstance(result, core.SignInVerificationRequiredResponse):
                return await self.handle_sign_in_verification_required(result)
            elif isinstance(result, core.SignInFailedResponse):
                return await self.handle_sign_in_failed(result)
            else:
                raise AssertionError("Invalid sign in response")

    def on_email_verification_complete(
        self, func: OnEmailVerificationComplete
    ) -> OnEmailVerificationComplete:
        self._on_email_verification_complete = func
        return func

    async def handle_email_verification_complete(
        self,
        result: core.EmailVerificationCompleteResponse,
    ) -> fastapi.Response:
        if self._on_email_verification_complete is None:
            return self._not_implemented("on_email_verification_complete")

        return await self._on_email_verification_complete(result)

    def on_email_verification_missing_proof(
        self, func: OnEmailVerificationMissingProof
    ) -> OnEmailVerificationMissingProof:
        self._on_email_verification_missing_proof = func
        return func

    async def handle_email_verification_missing_proof(
        self,
        result: core.EmailVerificationMissingProofResponse,
    ) -> fastapi.Response:
        if self._on_email_verification_missing_proof is None:
            return self._not_implemented("on_email_verification_missing_proof")

        return await self._on_email_verification_missing_proof(result)

    def on_email_verification_failed(
        self, func: OnEmailVerificationFailed
    ) -> OnEmailVerificationFailed:
        self._on_email_verification_failed = func
        return func

    async def handle_email_verification_failed(
        self,
        result: core.EmailVerificationFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] email verification failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self._on_email_verification_failed is None:
            return self._not_implemented("on_email_verification_failed")

        return await self._on_email_verification_failed(result)

    def install_email_verification(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.email_verification_path,
            name=self.email_verification_name,
        )
        async def verify(
            verify_body: Annotated[VerifyBody, fastapi.Query()],
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            client = await core.make_async(self._auth.client)
            result = await client.verify_email(
                verify_body.verification_token, verifier
            )
            if isinstance(result, core.EmailVerificationCompleteResponse):
                return await self.handle_email_verification_complete(result)
            elif isinstance(
                result, core.EmailVerificationMissingProofResponse
            ):
                return await self.handle_email_verification_missing_proof(
                    result
                )
            elif isinstance(result, core.EmailVerificationFailedResponse):
                return await self.handle_email_verification_failed(result)
            else:
                raise AssertionError("Invalid email verification response")

    def on_send_password_reset_email_complete(
        self, func: OnSendPasswordResetEmailComplete
    ) -> OnSendPasswordResetEmailComplete:
        self._on_send_password_reset_email_complete = func
        return func

    async def handle_send_password_reset_email_complete(
        self,
        result: core.SendPasswordResetEmailCompleteResponse,
    ) -> fastapi.Response:
        if self._on_send_password_reset_email_complete is None:
            return self._not_implemented(
                "on_send_password_reset_email_complete"
            )

        response = await self._on_send_password_reset_email_complete(result)
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def on_send_password_reset_email_failed(
        self, func: OnSendPasswordResetEmailFailed
    ) -> OnSendPasswordResetEmailFailed:
        self._on_send_password_reset_email_failed = func
        return func

    async def handle_send_password_reset_email_failed(
        self,
        result: core.SendPasswordResetEmailFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] send password reset email failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self._on_send_password_reset_email_failed is None:
            return self._not_implemented("on_send_password_reset_email_failed")

        response = await self._on_send_password_reset_email_failed(result)
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def install_send_password_reset(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.send_password_reset_email_path,
            name=self.send_password_reset_email_name,
        )
        async def send_password_reset(
            send_password_reset_body: Annotated[
                SendPasswordResetBody, fastapi.Form()
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            if self.reset_password_page_name is None:
                raise ValueError("Please specify reset_password_page_name")

            client = await core.make_async(self._auth.client)
            result = await client.send_password_reset_email(
                send_password_reset_body.email,
                reset_url=str(request.url_for(self.reset_password_page_name)),
            )
            if isinstance(result, core.SendPasswordResetEmailCompleteResponse):
                return await self.handle_send_password_reset_email_complete(
                    result
                )
            elif isinstance(result, core.SendPasswordResetEmailFailedResponse):
                return await self.handle_send_password_reset_email_failed(
                    result
                )
            else:
                raise AssertionError("Invalid send password reset response")

    def on_reset_password_complete(
        self, func: OnResetPasswordComplete
    ) -> OnResetPasswordComplete:
        self._on_reset_password_complete = func
        return func

    async def handle_reset_password_complete(
        self,
        result: core.PasswordResetCompleteResponse,
    ) -> fastapi.Response:
        if self._on_reset_password_complete is None:
            return self._not_implemented("on_reset_password_complete")

        return await self._on_reset_password_complete(result)

    def on_reset_password_missing_proof(
        self, func: OnResetPasswordMissingProof
    ) -> OnResetPasswordMissingProof:
        self._on_reset_password_missing_proof = func
        return func

    async def handle_reset_password_missing_proof(
        self,
        result: core.PasswordResetMissingProofResponse,
    ) -> fastapi.Response:
        if self._on_reset_password_missing_proof is None:
            return self._not_implemented("on_reset_password_missing_proof")

        return await self._on_reset_password_missing_proof(result)

    def on_reset_password_failed(
        self, func: OnResetPasswordFailed
    ) -> OnResetPasswordFailed:
        self._on_reset_password_failed = func
        return func

    async def handle_reset_password_failed(
        self,
        result: core.PasswordResetFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] password reset failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self._on_reset_password_failed is None:
            return self._not_implemented("on_reset_password_failed")

        return await self._on_reset_password_failed(result)

    def install_reset_password(self, router: fastapi.APIRouter) -> None:
        @router.post(self.reset_password_path, name=self.reset_password_name)
        async def reset_password(
            reset_password_body: Annotated[ResetPasswordBody, fastapi.Form()],
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ):
            client = await core.make_async(self._auth.client)
            result = await client.reset_password(
                reset_token=reset_password_body.reset_token,
                verifier=verifier,
                password=reset_password_body.password,
            )
            if isinstance(result, core.PasswordResetCompleteResponse):
                return await self.handle_reset_password_complete(result)
            elif isinstance(result, core.PasswordResetMissingProofResponse):
                return await self.handle_reset_password_missing_proof(result)
            elif isinstance(result, core.PasswordResetFailedResponse):
                return await self.handle_reset_password_failed(result)
            else:
                raise AssertionError("Invalid reset password response")

    def install(self, router: fastapi.APIRouter) -> None:
        self.install_sign_up(router)
        self.install_sign_in(router)
        self.install_email_verification(router)
        self.install_send_password_reset(router)
        self.install_reset_password(router)
