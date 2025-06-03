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
from typing import Optional

import http

import fastapi
from fastapi import responses
from starlette.datastructures import URL

from gel.auth import builtin_ui as core

from . import GelAuth, Installable


class BuiltinUI(Installable):
    _auth: GelAuth

    sign_in_path: str = "/sign-in"
    sign_in_name: str = "gel.auth.builtin_ui.sign_in"
    sign_in_summary: str = "Redirect to the sign-in page of the built-in UI"

    sign_up_path: str = "/sign-up"
    sign_up_name: str = "gel.auth.builtin_ui.sign_up"
    sign_up_summary: str = "Redirect to the sign-up page of the built-in UI"

    incomplete_messages: dict[str, str] = {
        "verification_required": (
            "Please verify your email address before signing in."
        ),
        "verify": (
            "Successfully verified email! " "Please sign in to continue."
        ),
        "reset_password": (
            "Your password has been reset! " "Please sign in to continue."
        ),
        "password_reset_sent": (
            "Successfully sent password reset email! Please check "
            "your email for the link to reset your password."
        ),
    }

    def __init__(self, auth: GelAuth) -> None:
        self._auth = auth

    def install_sign_in_page(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.sign_in_path,
            name=self.sign_in_name,
            summary=self.sign_in_summary,
            response_class=responses.RedirectResponse,
            status_code=http.HTTPStatus.SEE_OTHER,
        )
        async def sign_in(
            response: fastapi.Response,
            incomplete: Optional[str] = None,
            error: Optional[str] = None,
        ) -> str:
            ui = await core.make_async(self._auth.client)
            result = ui.start_sign_in()
            self._auth.set_verifier_cookie(result.verifier, response)
            redirect_url = URL(result.redirect_url)
            if incomplete:
                error = self.incomplete_messages.get(incomplete, incomplete)
            if error:
                redirect_url = redirect_url.include_query_params(error=error)
            return str(redirect_url)

    def install_sign_up_page(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.sign_up_path,
            name=self.sign_up_name,
            summary=self.sign_up_summary,
            response_class=responses.RedirectResponse,
            status_code=http.HTTPStatus.SEE_OTHER,
        )
        async def sign_up(response: fastapi.Response) -> str:
            ui = await core.make_async(self._auth.client)
            result = ui.start_sign_up()
            self._auth.set_verifier_cookie(result.verifier, response)
            return str(result.redirect_url)

    def install(self, router: fastapi.APIRouter) -> None:
        self.install_sign_in_page(router)
        self.install_sign_up_page(router)
