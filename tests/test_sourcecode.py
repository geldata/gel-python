#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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


import pathlib
import subprocess
import unittest


def find_project_root() -> pathlib.Path:
    return pathlib.Path(__file__).parent.parent


class TestFlake8(unittest.TestCase):
    def test_cqa_ruff(self):
        project_root = find_project_root()

        try:
            import ruff  # NoQA
        except ImportError:
            raise unittest.SkipTest("ruff module is missing") from None

        for subdir in ["edgedb", "gel", "tests"]:
            try:
                subprocess.run(
                    ["ruff", "check", "."],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=project_root / subdir,
                )
            except subprocess.CalledProcessError as ex:
                output = ex.output.decode()
                raise AssertionError(
                    f"ruff validation failed:\n{output}"
                ) from None
