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
import warnings

import gel

from gel.codegen import generator, models
from gel.compatibility.introspection import get_schema_json, GelORMWarning
from gel.compatibility.clihelper import _get_conn_args
from gel.orm.sqla import ModelGenerator as SQLAModGen
from gel.orm.sqlmodel import ModelGenerator as SQLModGen
from gel.orm.django.generator import ModelGenerator as DjangoModGen


class ColoredArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        c = generator.C
        self.exit(
            2,
            f"{c.BOLD}{c.FAIL}error:{c.ENDC} "
            f"{c.BOLD}{message:s}{c.ENDC}\n",
        )


parser = ColoredArgumentParser(
    description="Generate Python code for various Gel interfaces."
)
parser.add_argument(
    "mode",
    choices=['edgeql', 'pydantic', 'sqlalchemy', 'sqlmodel', 'django'],
    help="Pick which mode to generate code for: "
    "`edgeql` makes Python code for .edgeql files; "
    "`pydantic` makes Pydantic models based on the database schema; "
    "`sqlalchemy`, `sqlmodel`, `django` makes Python ORM code for a database",
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
    help="Choose one or more targets to generate code (default is async)."
)
parser.add_argument(
    "--out",
    help="The output directory for the generated files.",
)
parser.add_argument(
    "--mod",
    help="The fullname of the Python module corresponding to the output "
         "directory.",
)
if sys.version_info[:2] >= (3, 9):
    parser.add_argument(
        "--skip-pydantic-validation",
        action=argparse.BooleanOptionalAction,
        default=argparse.SUPPRESS,  # override the builtin help for default
        help="Add a mixin to generated dataclasses "
        "to skip Pydantic validation (default is to add the mixin).",
    )
else:
    parser.add_argument(
        "--skip-pydantic-validation",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-skip-pydantic-validation",
        dest="skip_pydantic_validation",
        action="store_false",
        default=False,
        help="Add a mixin to generated dataclasses "
             "to skip Pydantic validation (default is to add the mixin).",
    )


def main():
    args = parser.parse_args()

    match args.mode:
        case 'edgeql':
            if not hasattr(args, "skip_pydantic_validation"):
                args.skip_pydantic_validation = True
            generator.Generator(args).run()

        case 'pydantic':
            if args.mod is None:
                parser.error('pydantic requires to specify --mod')
            if args.out is None:
                parser.error('pydantic requires to specify --out')

            models.Generator(args).run()

        case 'sqlalchemy':
            if args.mod is None:
                parser.error('sqlalchemy requires to specify --mod')
            if args.out is None:
                parser.error('sqlalchemy requires to specify --out')

            with warnings.catch_warnings(record=True) as wlist:
                warnings.simplefilter("always", GelORMWarning)
                spec = get_schema_json(
                    gel.create_client(**generator._get_conn_args(args)))
                gen = SQLAModGen(
                    outdir=args.out,
                    basemodule=args.mod,
                )
                gen.render_models(spec)

                for w in wlist:
                    print(w.message)

        case 'sqlmodel':
            if args.mod is None:
                parser.error('sqlmodel requires to specify --mod')
            if args.out is None:
                parser.error('sqlmodel requires to specify --out')

            with warnings.catch_warnings(record=True) as wlist:
                warnings.simplefilter("always", GelORMWarning)
                spec = get_schema_json(
                    gel.create_client(**_get_conn_args(args)))
                gen = SQLModGen(
                    outdir=args.out,
                    basemodule=args.mod,
                )
                gen.render_models(spec)

                for w in wlist:
                    print(w.message)

        case 'django':
            with warnings.catch_warnings(record=True) as wlist:
                warnings.simplefilter("always", GelORMWarning)
                spec = get_schema_json(
                    gel.create_client(**_get_conn_args(args)))
                gen = DjangoModGen(
                    out=args.out,
                )
                gen.render_models(spec)

                for w in wlist:
                    print(w.message)
