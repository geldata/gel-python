# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    TextIO,
)

import getpass
import io
import pathlib
import sys
import typing

import gel
from gel.con_utils import find_gel_project_dir
from gel._internal._color import get_color

if TYPE_CHECKING:
    import argparse


C = get_color()


class AbstractCodeGenerator:
    def __init__(
        self,
        args: argparse.Namespace,
        *,
        project_dir: pathlib.Path | None = None,
        client: gel.Client | None = None,
        interactive: bool = True,
    ):
        self._default_module = "default"
        self._async = False
        self._no_cache = args.no_cache

        self._interactive = interactive
        self._stderr: TextIO
        if not interactive:
            self._stderr = io.StringIO()
        else:
            self._stderr = sys.stderr

        if project_dir is None:
            try:
                self._project_dir = pathlib.Path(find_gel_project_dir())
            except gel.ClientConnectionError:
                self.print_error(
                    "Cannot find gel.toml: "
                    "codegen must be run inside a Gel project directory"
                )
                self.abort(2)

            self.print_msg(
                f"Found Gel project: {C.BOLD}{self._project_dir}{C.ENDC}"
            )
        else:
            self._project_dir = project_dir

        if client is None:
            self._client = gel.create_client(**self._get_conn_args(args))
        else:
            self._client = client

    def get_error_output(self) -> str:
        if isinstance(self._stderr, io.StringIO):
            return self._stderr.getvalue()
        else:
            raise RuntimeError("Cannot get error output in non-silent mode")

    def abort(self, code: int) -> typing.NoReturn:
        if self._interactive:
            raise RuntimeError(f"aborting codegen, code={code}")
        else:
            sys.exit(code)

    def print_msg(self, msg: str) -> None:
        print(msg, file=self._stderr)

    def print_error(self, msg: str) -> None:
        print(
            f"{C.BOLD}{C.FAIL}error: {C.ENDC}{C.BOLD}{msg}{C.ENDC}",
            file=self._stderr,
        )

    def _get_conn_args(self, args: argparse.Namespace) -> dict[str, Any]:
        if args.password_from_stdin:
            if args.password:
                self.print_error(
                    "--password and --password-from-stdin "
                    "are mutually exclusive",
                )
                self.abort(22)
            if sys.stdin.isatty():
                password = getpass.getpass()
            else:
                password = sys.stdin.read().strip()
        else:
            password = args.password
        if args.dsn and args.instance:
            self.print_error("--dsn and --instance are mutually exclusive")
            self.abort(22)
        return dict(
            dsn=args.dsn or args.instance,
            credentials_file=args.credentials_file,
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=password,
            tls_ca_file=args.tls_ca_file,
            tls_security=args.tls_security,
        )
