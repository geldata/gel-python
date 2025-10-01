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


import os
import pathlib
import subprocess
import sys
import unittest

from gel._internal import _version


def find_project_root() -> pathlib.Path:
    if gh_checkout := os.environ.get("GITHUB_WORKSPACE"):
        return pathlib.Path(gh_checkout)
    elif src_root := _version.get_project_source_root():
        return src_root
    else:
        return pathlib.Path(__file__).parent.parent


class TestFlake8(unittest.TestCase):
    def test_cqa_ruff_check(self):
        project_root = find_project_root()

        try:
            import ruff  # NoQA
        except ImportError:
            raise unittest.SkipTest("ruff module is missing") from None

        for subdir in ["edgedb", "gel", "tests"]:
            try:
                subprocess.run(
                    [sys.executable, "-m", "ruff", "check", "."],
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

    def test_cqa_ruff_format_check(self):
        project_root = find_project_root()

        try:
            import ruff  # NoQA
        except ImportError:
            raise unittest.SkipTest("ruff module is missing") from None

        for subdir in ["gel"]:
            try:
                subprocess.run(
                    [sys.executable, "-m", "ruff", "format", "--check", "."],
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

    def test_cqa_mypy(self):
        project_root = find_project_root()
        config_path = project_root / "pyproject.toml"
        if not os.path.exists(config_path):
            raise RuntimeError("could not locate pyproject.toml file")

        try:
            import mypy  # NoQA
        except ImportError:
            raise unittest.SkipTest("mypy module is missing")

        for subdir in ["edgedb", "gel", "tests"]:
            try:
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "mypy",
                        "--config-file",
                        config_path,
                        subdir,
                    ],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=project_root,
                )
            except subprocess.CalledProcessError as ex:
                output = ex.stdout.decode()
                if ex.stderr:
                    output += "\n\n" + ex.stderr.decode()
                raise AssertionError(
                    f"mypy validation failed:\n{output}"
                ) from None

    @unittest.skipIf(os.environ.get("CIBUILDWHEEL"), "broken in CIBW tests")
    def test_cqa_pyright(self):
        project_root = find_project_root()
        config_path = project_root / "pyproject.toml"
        if not os.path.exists(config_path):
            raise RuntimeError("could not locate pyproject.toml file")

        try:
            import pyright  # NoQA
        except ImportError:
            raise unittest.SkipTest("pyright module is missing")

        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pyright",
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=project_root,
                env=os.environ | {
                    # Suppress pyright-python's complaints about new
                    # pyright versions being available.
                    "PYRIGHT_PYTHON_IGNORE_WARNINGS": "1",
                }
            )
        except subprocess.CalledProcessError as ex:
            output = ex.stdout.decode()
            if ex.stderr:
                output += "\n\n" + ex.stderr.decode()
            raise AssertionError(
                f"pyright validation failed:\n{output}"
            ) from None
