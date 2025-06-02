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
    NoReturn,
)

import argparse

from gel._internal._codegen import _models

from . import base
from . import queries


class ColoredArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        c = base.C
        self.exit(
            2,
            f"{c.BOLD}{c.FAIL}error:{c.ENDC} {c.BOLD}{message:s}{c.ENDC}\n",
        )


def _get_base_parser(description: str) -> ColoredArgumentParser:
    parser = ColoredArgumentParser(description=description)
    parser.add_argument("--dsn")
    parser.add_argument("--credentials-file", metavar="PATH")
    parser.add_argument("-I", "--instance", metavar="NAME")
    parser.add_argument("-H", "--host")
    parser.add_argument("-P", "--port")
    parser.add_argument("-d", "--database", metavar="NAME")
    parser.add_argument("-u", "--user")
    parser.add_argument("--password")
    parser.add_argument("--password-from-stdin", action="store_true")
    parser.add_argument("--tls-ca-file", metavar="PATH")
    parser.add_argument(
        "--tls-security",
        choices=["default", "strict", "no_host_verification", "insecure"],
    )

    return parser


def _augment_queries_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--file",
        action="append",
        nargs="?",
        help="Generate a single file instead of one per .edgeql file.",
    )
    parser.add_argument(
        "--dir",
        action="append",
        help="Only search .edgeql files under specified directories.",
    )
    parser.add_argument(
        "--target",
        choices=["blocking", "async"],
        nargs="*",
        default=["async"],
        help="Choose one or more targets to generate code (default is async).",
    )
    parser.add_argument(
        "--skip-pydantic-validation",
        action=argparse.BooleanOptionalAction,
        default=argparse.SUPPRESS,  # override the builtin help for default
        help="Add a mixin to generated dataclasses "
        "to skip Pydantic validation (default is to add the mixin).",
    )


def run_queries_generator(args: argparse.Namespace) -> None:
    if not hasattr(args, "skip_pydantic_validation"):
        args.skip_pydantic_validation = True
    queries.QueriesGenerator(args).run()


def run_models_generator(args: argparse.Namespace) -> None:
    _models.PydanticModelsGenerator(args).run()


def main() -> None:
    parser = _get_base_parser("Generate typed functions from .edgeql files")
    _augment_queries_parser(parser)
    args = parser.parse_args()
    run_queries_generator(args)


def generate() -> None:
    parser = _get_base_parser("Official Gel code generators for Python.")
    subparsers = parser.add_subparsers(title="GENERATORS", required=True)
    queries_parser = subparsers.add_parser(
        "queries",
        description="Generate typed functions from .edgeql files",
    )
    _augment_queries_parser(queries_parser)
    queries_parser.set_defaults(func=run_queries_generator)

    models_parser = subparsers.add_parser(
        "models",
        description="Generate models for current Gel schema",
    )
    models_parser.set_defaults(func=run_models_generator)

    args = parser.parse_args()
    args.func(args)
