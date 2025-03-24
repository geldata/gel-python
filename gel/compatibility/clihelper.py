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

import argparse
import getpass
import sys

from gel.color import get_color


C = get_color()


def print_msg(msg):
    print(msg, file=sys.stderr)


def print_error(msg):
    print_msg(f"{C.BOLD}{C.FAIL}error: {C.ENDC}{C.BOLD}{msg}{C.ENDC}")


def _get_conn_args(args: argparse.Namespace):
    if args.password_from_stdin:
        if args.password:
            print_error(
                "--password and --password-from-stdin are "
                "mutually exclusive",
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
