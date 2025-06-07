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
from typing import Annotated, Optional

import http
import logging

import pydantic
import fastapi
from fastapi import responses

from gel.auth import email_password as core

from . import GelAuth, Installable
from .. import utils


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


class EmailPassword(Installable):
    redirect_to: Optional[str] = "/"
    redirect_to_page_name: Optional[str] = None
    error_page_name: str = "error_page"
    sign_in_page_name: str = "sign_in_page"
    reset_password_page_name: str = "reset_password_page"

    _auth: GelAuth

    # Sign-up
    sign_up_path: str = "/register"
    sign_up_name: str = "gel.auth.email_password.sign_up"
    sign_up_summary: str = "Sign up with email and password"
    sign_up_default_response_class = responses.RedirectResponse
    sign_up_default_status_code = http.HTTPStatus.SEE_OTHER
    on_sign_up_complete: utils.Hook[core.SignUpCompleteResponse] = utils.Hook(
        "sign_up"
    )
    on_sign_up_verification_required: utils.Hook[
        core.SignUpVerificationRequiredResponse
    ] = utils.Hook("sign_up")
    on_sign_up_failed: utils.Hook[core.SignUpFailedResponse] = utils.Hook(
        "sign_up"
    )

    # Sign-in
    sign_in_path: str = "/authenticate"
    sign_in_name: str = "gel.auth.email_password.sign_in"
    sign_in_summary: str = "Sign in with email and password"
    sign_in_default_response_class = responses.RedirectResponse
    sign_in_default_status_code = http.HTTPStatus.SEE_OTHER
    on_sign_in_complete: utils.Hook[core.SignInCompleteResponse] = utils.Hook(
        "sign_in"
    )
    on_sign_in_verification_required: utils.Hook[
        core.SignInVerificationRequiredResponse
    ] = utils.Hook("sign_in")
    on_sign_in_failed: utils.Hook[core.SignInFailedResponse] = utils.Hook(
        "sign_in"
    )

    # Email verification
    email_verification_path: str = "/verify"
    email_verification_name: str = "gel.auth.email_password.email_verification"
    email_verification_summary: str = "Verify the email address"
    email_verification_default_response_class = responses.RedirectResponse
    email_verification_default_status_code = http.HTTPStatus.SEE_OTHER
    on_email_verification_complete: utils.Hook[
        core.EmailVerificationCompleteResponse
    ] = utils.Hook("email_verification")
    on_email_verification_missing_proof: utils.Hook[
        core.EmailVerificationMissingProofResponse
    ] = utils.Hook("email_verification")
    on_email_verification_failed: utils.Hook[
        core.EmailVerificationFailedResponse
    ] = utils.Hook("email_verification")

    # Send password reset
    send_password_reset_email_path: str = "/send-password-reset"
    send_password_reset_email_name: str = (
        "gel.auth.email_password.send_password_reset"
    )
    send_password_reset_email_summary: str = "Send a password reset email"
    send_password_reset_email_default_response_class = (
        responses.RedirectResponse
    )
    send_password_reset_email_default_status_code = http.HTTPStatus.SEE_OTHER
    on_send_password_reset_email_complete: utils.Hook[
        core.SendPasswordResetEmailCompleteResponse
    ] = utils.Hook("send_password_reset_email")
    on_send_password_reset_email_failed: utils.Hook[
        core.SendPasswordResetEmailFailedResponse
    ] = utils.Hook("send_password_reset_email")

    # Reset password
    reset_password_path: str = "/reset-password"
    reset_password_name: str = "gel.auth.email_password.reset_password"
    reset_password_summary: str = "Reset the password"
    reset_password_default_response_class = responses.RedirectResponse
    reset_password_default_status_code = http.HTTPStatus.SEE_OTHER
    on_reset_password_complete: utils.Hook[
        core.PasswordResetCompleteResponse
    ] = utils.Hook("reset_password")
    on_reset_password_missing_proof: utils.Hook[
        core.PasswordResetMissingProofResponse
    ] = utils.Hook("reset_password")
    on_reset_password_failed: utils.Hook[core.PasswordResetFailedResponse] = (
        utils.Hook("reset_password")
    )

    def __init__(self, auth: GelAuth):
        self._auth = auth

    def _not_implemented(self, method: str) -> fastapi.Response:
        return responses.JSONResponse(
            status_code=http.HTTPStatus.NOT_IMPLEMENTED,
            content={"error": "not implemented", "method": method},
        )

    def _redirect_success(
        self,
        request: fastapi.Request,
        key: str,
        *,
        method: str,
    ) -> fastapi.Response:
        response_class: type[responses.RedirectResponse] = getattr(
            self, f"{key}_default_response_class"
        )
        response_code = getattr(self, f"{key}_default_status_code")
        if self.redirect_to_page_name is not None:
            return response_class(
                url=request.url_for(self.redirect_to_page_name),
                status_code=response_code,
            )
        elif self.redirect_to is not None:
            return response_class(
                url=self.redirect_to,
                status_code=response_code,
            )
        else:
            return self._not_implemented(method)

    def _redirect_error(
        self,
        request: fastapi.Request,
        key: str,
        **query_params: str,
    ) -> fastapi.Response:
        response_class: type[responses.RedirectResponse] = getattr(
            self, f"{key}_default_response_class"
        )
        return response_class(
            url=request.url_for(self.error_page_name).include_query_params(
                **query_params
            ),
            status_code=getattr(self, f"{key}_default_status_code"),
        )

    def _redirect_sign_in(
        self,
        request: fastapi.Request,
        key: str,
        **query_params: str,
    ) -> fastapi.Response:
        response_class: type[responses.RedirectResponse] = getattr(
            self, f"{key}_default_response_class"
        )
        return response_class(
            url=request.url_for(self.sign_in_page_name).include_query_params(
                **query_params
            ),
            status_code=getattr(self, f"{key}_default_status_code"),
        )

    async def handle_sign_up_complete(
        self,
        request: fastapi.Request,
        result: core.SignUpCompleteResponse,
    ) -> fastapi.Response:
        response = await self._auth.handle_new_identity(
            request, result.identity_id, result.token_data
        )
        if response is None:
            if self.on_sign_up_complete.is_set():
                response = await self.on_sign_up_complete.call(request, result)
            else:
                response = self._redirect_success(
                    request, "sign_up", method="on_sign_up_complete"
                )
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    async def handle_sign_up_verification_required(
        self,
        request: fastapi.Request,
        result: core.SignUpVerificationRequiredResponse,
    ) -> fastapi.Response:
        if result.identity_id:
            response = await self._auth.handle_new_identity(
                request, result.identity_id, None
            )
        else:
            response = None
        if response is None:
            if self.on_sign_up_verification_required.is_set():
                response = await self.on_sign_up_verification_required.call(
                    request, result
                )
            else:
                response = self._redirect_sign_in(
                    request, "sign_up", incomplete="verification_required"
                )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_sign_up_failed(
        self,
        request: fastapi.Request,
        result: core.SignUpFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] sign up failed: %s", result.status_code, result.message
        )
        logger.debug("%r", result)

        if self.on_sign_up_failed.is_set():
            response = await self.on_sign_up_failed.call(request, result)
        else:
            response = self._redirect_error(
                request, "sign_up", error=result.message
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def install_sign_up(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.sign_up_path,
            name=self.sign_up_name,
            summary=self.sign_up_summary,
        )
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
            match result:
                case core.SignUpCompleteResponse():
                    return await self.handle_sign_up_complete(request, result)
                case core.SignUpVerificationRequiredResponse():
                    return await self.handle_sign_up_verification_required(
                        request, result
                    )
                case core.SignUpFailedResponse():
                    return await self.handle_sign_up_failed(request, result)
                case _:
                    raise AssertionError("Invalid sign up response")

    async def handle_sign_in_complete(
        self,
        request: fastapi.Request,
        result: core.SignInCompleteResponse,
    ) -> fastapi.Response:
        if self.on_sign_in_complete.is_set():
            response = await self.on_sign_in_complete.call(request, result)
        else:
            response = self._redirect_success(
                request, "sign_in", method="on_sign_in_complete"
            )
        self._auth.set_auth_cookie(result.token_data.auth_token, response)
        return response

    async def handle_sign_in_verification_required(
        self,
        request: fastapi.Request,
        result: core.SignInVerificationRequiredResponse,
    ) -> fastapi.Response:
        if self.on_sign_in_verification_required.is_set():
            response = await self.on_sign_in_verification_required.call(
                request, result
            )
        else:
            response = self._redirect_sign_in(
                request, "sign_in", incomplete="verification_required"
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_sign_in_failed(
        self,
        request: fastapi.Request,
        result: core.SignInFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] sign in failed: %s", result.status_code, result.message
        )
        logger.debug("%r", result)

        if self.on_sign_in_failed.is_set():
            response = await self.on_sign_in_failed.call(request, result)
        else:
            response = self._redirect_error(
                request, "sign_in", error=result.message
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def install_sign_in(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.sign_in_path,
            name=self.sign_in_name,
            summary=self.sign_in_summary,
        )
        async def sign_in(
            sign_in_body: Annotated[SignInBody, fastapi.Form()],
            request: fastapi.Request,
        ) -> fastapi.Response:
            client = await core.make_async(self._auth.client)
            result = await client.sign_in(
                sign_in_body.email, sign_in_body.password
            )
            match result:
                case core.SignInCompleteResponse():
                    return await self.handle_sign_in_complete(request, result)
                case core.SignInVerificationRequiredResponse():
                    return await self.handle_sign_in_verification_required(
                        request, result
                    )
                case core.SignInFailedResponse():
                    return await self.handle_sign_in_failed(request, result)
                case _:
                    raise AssertionError("Invalid sign in response")

    async def handle_email_verification_complete(
        self,
        request: fastapi.Request,
        result: core.EmailVerificationCompleteResponse,
    ) -> fastapi.Response:
        if self.on_email_verification_complete.is_set():
            return await self.on_email_verification_complete.call(
                request, result
            )
        else:
            return self._redirect_success(
                request,
                "email_verification",
                method="on_email_verification_complete",
            )

    async def handle_email_verification_missing_proof(
        self,
        request: fastapi.Request,
        result: core.EmailVerificationMissingProofResponse,
    ) -> fastapi.Response:
        if self.on_email_verification_missing_proof.is_set():
            return await self.on_email_verification_missing_proof.call(
                request, result
            )
        else:
            return self._redirect_sign_in(
                request, "email_verification", incomplete="verify"
            )

    async def handle_email_verification_failed(
        self,
        request: fastapi.Request,
        result: core.EmailVerificationFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] email verification failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self.on_email_verification_failed.is_set():
            return await self.on_email_verification_failed.call(
                request, result
            )
        else:
            return self._redirect_error(
                request, "email_verification", error=result.message
            )

    def install_email_verification(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.email_verification_path,
            name=self.email_verification_name,
            summary=self.email_verification_summary,
        )
        async def verify(
            request: fastapi.Request,
            verify_body: Annotated[VerifyBody, fastapi.Query()],
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            client = await core.make_async(self._auth.client)
            result = await client.verify_email(
                verify_body.verification_token, verifier
            )
            match result:
                case core.EmailVerificationCompleteResponse():
                    return await self.handle_email_verification_complete(
                        request, result
                    )
                case core.EmailVerificationMissingProofResponse():
                    return await self.handle_email_verification_missing_proof(
                        request, result
                    )
                case core.EmailVerificationFailedResponse():
                    return await self.handle_email_verification_failed(
                        request, result
                    )
                case _:
                    raise AssertionError("Invalid email verification response")

    async def handle_send_password_reset_email_complete(
        self,
        request: fastapi.Request,
        result: core.SendPasswordResetEmailCompleteResponse,
    ) -> fastapi.Response:
        if self.on_send_password_reset_email_complete.is_set():
            response = await self.on_send_password_reset_email_complete.call(
                request, result
            )
        else:
            response = self._redirect_sign_in(
                request,
                "send_password_reset_email",
                incomplete="password_reset_sent",
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    async def handle_send_password_reset_email_failed(
        self,
        request: fastapi.Request,
        result: core.SendPasswordResetEmailFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] send password reset email failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self.on_send_password_reset_email_failed.is_set():
            response = await self.on_send_password_reset_email_failed.call(
                request, result
            )
        else:
            response = self._redirect_error(
                request, "send_password_reset_email", error=result.message
            )
        self._auth.set_verifier_cookie(result.verifier, response)
        return response

    def install_send_password_reset(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.send_password_reset_email_path,
            name=self.send_password_reset_email_name,
            summary=self.send_password_reset_email_summary,
        )
        async def send_password_reset(
            send_password_reset_body: Annotated[
                SendPasswordResetBody, fastapi.Form()
            ],
            request: fastapi.Request,
        ) -> fastapi.Response:
            client = await core.make_async(self._auth.client)
            result = await client.send_password_reset_email(
                send_password_reset_body.email,
                reset_url=str(request.url_for(self.reset_password_page_name)),
            )
            match result:
                case core.SendPasswordResetEmailCompleteResponse():
                    return (
                        await self.handle_send_password_reset_email_complete(
                            request, result
                        )
                    )
                case core.SendPasswordResetEmailFailedResponse():
                    return await self.handle_send_password_reset_email_failed(
                        request, result
                    )
                case _:
                    raise AssertionError(
                        "Invalid send password reset response"
                    )

    async def handle_reset_password_complete(
        self,
        request: fastapi.Request,
        result: core.PasswordResetCompleteResponse,
    ) -> fastapi.Response:
        if self.on_reset_password_complete.is_set():
            return await self.on_reset_password_complete.call(request, result)
        else:
            return self._redirect_success(
                request, "reset_password", method="on_reset_password_complete"
            )

    async def handle_reset_password_missing_proof(
        self,
        request: fastapi.Request,
        result: core.PasswordResetMissingProofResponse,
    ) -> fastapi.Response:
        if self.on_reset_password_missing_proof.is_set():
            return await self.on_reset_password_missing_proof.call(
                request, result
            )
        else:
            return self._redirect_sign_in(
                request, "reset_password", incomplete="reset_password"
            )

    async def handle_reset_password_failed(
        self,
        request: fastapi.Request,
        result: core.PasswordResetFailedResponse,
    ) -> fastapi.Response:
        logger.info(
            "[%d] password reset failed: %s",
            result.status_code,
            result.message,
        )
        logger.debug("%r", result)

        if self.on_reset_password_failed.is_set():
            return await self.on_reset_password_failed.call(request, result)
        else:
            return self._redirect_error(
                request, "reset_password", error=result.message
            )

    def install_reset_password(self, router: fastapi.APIRouter) -> None:
        @router.post(
            self.reset_password_path,
            name=self.reset_password_name,
            summary=self.reset_password_summary,
        )
        async def reset_password(
            request: fastapi.Request,
            reset_password_body: Annotated[ResetPasswordBody, fastapi.Form()],
            verifier: Optional[str] = fastapi.Depends(
                self._auth.pkce_verifier
            ),
        ) -> fastapi.Response:
            client = await core.make_async(self._auth.client)
            result = await client.reset_password(
                reset_token=reset_password_body.reset_token,
                verifier=verifier,
                password=reset_password_body.password,
            )
            match result:
                case core.PasswordResetCompleteResponse():
                    return await self.handle_reset_password_complete(
                        request, result
                    )
                case core.PasswordResetMissingProofResponse():
                    return await self.handle_reset_password_missing_proof(
                        request, result
                    )
                case core.PasswordResetFailedResponse():
                    return await self.handle_reset_password_failed(
                        request, result
                    )
                case _:
                    raise AssertionError("Invalid reset password response")

    def install(self, router: fastapi.APIRouter) -> None:
        self.install_sign_up(router)
        self.install_sign_in(router)
        self.install_email_verification(router)
        self.install_send_password_reset(router)
        self.install_reset_password(router)
