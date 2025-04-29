#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    TYPE_CHECKING,
    Any,
    TextIO,
)

import getpass
import io
import os
import pathlib
import sys
import typing

import gel
from gel.con_utils import find_gel_project_dir
from gel.color import get_color

if TYPE_CHECKING:
    import argparse


C = get_color()

if pyver_env := os.environ.get("GEL_PYTHON_CODEGEN_PY_VER"):
    try:
        SYS_VERSION_INFO = tuple(map(int, pyver_env.split(".")))[:2]
    except ValueError:
        raise ValueError(
            f"invalid version in GEL_PYTHON_CODEGEN_PY_VER: {pyver_env}"
        ) from None
else:
    SYS_VERSION_INFO = sys.version_info[:2]

TYPE_MAPPING: dict[str, str | tuple[str, str]] = {
    "std::str": "str",
    "std::float32": "float",
    "std::float64": "float",
    "std::int16": "int",
    "std::int32": "int",
    "std::int64": "int",
    "std::bigint": "int",
    "std::bool": "bool",
    "std::uuid": ("uuid", "UUID"),
    "std::bytes": "bytes",
    "std::decimal": ("decimal", "Decimal"),
    "std::datetime": ("datetime", "datetime"),
    "std::duration": ("datetime", "timedelta"),
    "std::json": "str",
    "cal::local_date": ("datetime", "date"),
    "cal::local_time": ("datetime", "time"),
    "cal::local_datetime": ("datetime", "datetime"),
    "cal::relative_duration": ("gel", "RelativeDuration"),
    "cal::date_duration": ("gel", "DateDuration"),
    "std::cal::local_date": ("datetime", "date"),
    "std::cal::local_time": ("datetime", "time"),
    "std::cal::local_datetime": ("datetime", "datetime"),
    "std::cal::relative_duration": ("gel", "RelativeDuration"),
    "std::cal::date_duration": ("gel", "DateDuration"),
    "cfg::memory": ("gel", "ConfigMemory"),
    "std::cfg::memory": ("gel", "ConfigMemory"),
    "ext::pgvector::vector": ("array", "array"),
}

INPUT_TYPE_MAPPING = dict(TYPE_MAPPING)
INPUT_TYPE_MAPPING.update(
    {
        "ext::pgvector::vector": ("typing", "Sequence[float]"),
    }
)

PYDANTIC_MIXIN = """\
class NoPydanticValidation:
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, _handler):
        # Pydantic 2.x
        from pydantic_core.core_schema import any_schema
        return any_schema()

    @classmethod
    def __get_validators__(cls):
        # Pydantic 1.x
        from pydantic.dataclasses import dataclass as pydantic_dataclass
        _ = pydantic_dataclass(cls)
        cls.__pydantic_model__.__get_validators__ = lambda: []
        return []\
"""

MAX_LINE_LENGTH = 79


class Generator:
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
