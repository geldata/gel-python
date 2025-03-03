#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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
import warnings

import gel

from gel.codegen.generator import _get_conn_args
from .introspection import get_schema_json, GelORMWarning
from .sqla import ModelGenerator as SQLAModGen
from .sqlmodel import ModelGenerator as SQLModGen
from .django.generator import ModelGenerator as DjangoModGen


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(
            2,
            f"error: {message:s}\n",
        )


parser = ArgumentParser(
    description="Generate Python ORM code for accessing a Gel database."
)
parser.add_argument(
    "orm",
    choices=['sqlalchemy', 'sqlmodel', 'django'],
    help="Pick which ORM to generate models for.",
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
    "--out",
    help="The output directory for the generated files.",
    required=True,
)
parser.add_argument(
    "--mod",
    help="The fullname of the Python module corresponding to the output "
         "directory.",
)


def main():
    args = parser.parse_args()
    # setup client
    client = gel.create_client(**_get_conn_args(args))

    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always", GelORMWarning)

        spec = get_schema_json(client)
        generate_models(args, spec)

        for w in wlist:
            print(w.message)


def generate_models(args, spec):
    match args.orm:
        case 'sqlalchemy':
            if args.mod is None:
                parser.error('sqlalchemy requires to specify --mod')

            gen = SQLAModGen(
                outdir=args.out,
                basemodule=args.mod,
            )
            gen.render_models(spec)

        case 'sqlmodel':
            if args.mod is None:
                parser.error('sqlmodel requires to specify --mod')

            gen = SQLModGen(
                outdir=args.out,
                basemodule=args.mod,
            )
            gen.render_models(spec)

        case 'django':
            gen = DjangoModGen(
                out=args.out,
            )
            gen.render_models(spec)
