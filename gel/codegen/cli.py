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


import argparse
import sys

from . import generator


class ColoredArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        c = generator.C
        self.exit(
            2,
            f"{c.BOLD}{c.FAIL}error:{c.ENDC} {c.BOLD}{message:s}{c.ENDC}\n",
        )


parser = ColoredArgumentParser(
    description="Generate Python code for .edgeql files."
)
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
parser.add_argument(
    "--allow-user-specified-id",
    action=argparse.BooleanOptionalAction,
    help="Allow user specified ids in .edgeql files (default is to disallow).",
)


def main():
    args = parser.parse_args()
    if not hasattr(args, "skip_pydantic_validation"):
        args.skip_pydantic_validation = True
    generator.Generator(args).run()
