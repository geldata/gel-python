# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

import http

import fastapi
from fastapi import responses

from gel.auth import builtin_ui as core

from . import GelAuth
from . import Installable
from .. import _utils as utils


class BuiltinUI(Installable):
    _auth: GelAuth

    sign_in_path = utils.Config("/sign-in")
    sign_in_name = utils.Config("gel.auth.builtin_ui.sign_in")
    sign_in_summary = utils.Config(
        "Redirect to the sign-in page of the built-in UI"
    )

    sign_up_path = utils.Config("/sign-up")
    sign_up_name = utils.Config("gel.auth.builtin_ui.sign_up")
    sign_up_summary = utils.Config(
        "Redirect to the sign-up page of the built-in UI"
    )

    def __init__(self, auth: GelAuth) -> None:
        self._auth = auth

    def install_sign_in_page(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.sign_in_path.value,
            name=self.sign_in_name.value,
            summary=self.sign_in_summary.value,
            response_class=responses.RedirectResponse,
            status_code=http.HTTPStatus.SEE_OTHER,
        )
        async def sign_in(response: fastapi.Response) -> str:
            ui = await core.make_async(self._auth.client)
            result = ui.start_sign_in()
            self._auth.set_verifier_cookie(result.verifier, response)
            return str(result.redirect_url)

    def install_sign_up_page(self, router: fastapi.APIRouter) -> None:
        @router.get(
            self.sign_up_path.value,
            name=self.sign_up_name.value,
            summary=self.sign_up_summary.value,
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
        super().install(router)
