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

from typing import (
    Any,
)

import argparse
import getpass
import os
import pathlib
import sys

import gel
from gel.con_utils import find_gel_project_dir
from gel.color import get_color


C = get_color()
if pyver_env := os.environ.get("GEL_PYTHON_CODEGEN_PY_VER"):
    try:
        SYS_VERSION_INFO = tuple(map(int, pyver_env.split(".")))[:2]
    except ValueError:
        raise ValueError(
            f"invalid version in GEL_PYTHON_CODEGEN_PY_VER: {pyver_env}"
        )
else:
    SYS_VERSION_INFO = sys.version_info[:2]

TYPE_MAPPING = {
    "std::str": "str",
    "std::float32": "float",
    "std::float64": "float",
    "std::int16": "int",
    "std::int32": "int",
    "std::int64": "int",
    "std::bigint": "int",
    "std::bool": "bool",
    "std::uuid": "uuid.UUID",
    "std::bytes": "bytes",
    "std::decimal": "decimal.Decimal",
    "std::datetime": "datetime.datetime",
    "std::duration": "datetime.timedelta",
    "std::json": "str",
    "cal::local_date": "datetime.date",
    "cal::local_time": "datetime.time",
    "cal::local_datetime": "datetime.datetime",
    "cal::relative_duration": "gel.RelativeDuration",
    "cal::date_duration": "gel.DateDuration",
    "cfg::memory": "gel.ConfigMemory",
    "ext::pgvector::vector": "array.array",
}

TYPE_IMPORTS = {
    "std::uuid": "uuid",
    "std::decimal": "decimal",
    "std::datetime": "datetime",
    "std::duration": "datetime",
    "cal::local_date": "datetime",
    "cal::local_time": "datetime",
    "cal::local_datetime": "datetime",
    "ext::pgvector::vector": "array",
}

INPUT_TYPE_MAPPING = TYPE_MAPPING.copy()
INPUT_TYPE_MAPPING.update(
    {
        "ext::pgvector::vector": "typing.Sequence[float]",
    }
)

INPUT_TYPE_IMPORTS = TYPE_IMPORTS.copy()
INPUT_TYPE_IMPORTS.update(
    {
        "ext::pgvector::vector": "typing",
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


def print_msg(msg: str) -> None:
    print(msg, file=sys.stderr)


def print_error(msg: str) -> None:
    print_msg(f"{C.BOLD}{C.FAIL}error: {C.ENDC}{C.BOLD}{msg}{C.ENDC}")


def _get_conn_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.password_from_stdin:
        if args.password:
            print_error(
                "--password and --password-from-stdin are mutually exclusive",
            )
            sys.exit(22)
        if sys.stdin.isatty():
            password = getpass.getpass()
        else:
            password = sys.stdin.read().strip()
    else:
        password = args.password
    if args.dsn and args.instance:
        print_error("--dsn and --instance are mutually exclusive")
        sys.exit(22)
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


class Generator:
    def __init__(self, args: argparse.Namespace):
        self._default_module = "default"
        self._async = False
        try:
            self._project_dir = pathlib.Path(find_gel_project_dir())
        except gel.ClientConnectionError:
            print(
                "Cannot find gel.toml: "
                "codegen must be run under an EdgeDB project dir"
            )
            sys.exit(2)
        print_msg(f"Found Gel project: {C.BOLD}{self._project_dir}{C.ENDC}")
        self._client = gel.create_client(**_get_conn_args(args))
