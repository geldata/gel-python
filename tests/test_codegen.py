#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import asyncio
import pathlib
import shutil
import subprocess
import os
import tempfile

from gel import _testbase as tb


class TestCodegen(tb.AsyncQueryTestCase):
    SETUP = '''
        create extension pgvector;
        create scalar type v3 extending ext::pgvector::vector<3>;
    '''

    TEARDOWN = '''
        drop scalar type v3;
        drop extension pgvector;
    '''

    maxDiff = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        version = cls.loop.run_until_complete(
            cls.client.query_required_single('''
                select sys::get_version().major
            ''')
        )

        if version >= 6:
            suffix = ''
        elif version >= 5:
            suffix = '5'
        else:
            suffix = '3'

        cls.ASSERT_SUFFIX = f'.assert{suffix}'

    async def test_codegen(self):
        env = os.environ.copy()
        env.update(
            {
                f"EDGEDB_{k.upper()}": str(v)
                for k, v in self.get_connect_args().items()
            }
        )
        env["EDGEDB_DATABASE"] = self.get_database_name()
        container = pathlib.Path(__file__).absolute().parent / "codegen"
        with tempfile.TemporaryDirectory() as td:
            td_path = pathlib.Path(td)
            shutil.copytree(container / "linked", td_path / "linked")
            for project in container.iterdir():
                if project.name == "linked":
                    continue
                with self.subTest(msg=project.name):
                    cwd = td_path / project.name
                    shutil.copytree(project, cwd)
                    try:
                        await self._test_codegen(env, cwd)
                    except subprocess.CalledProcessError as e:
                        self.fail("Codegen failed: " + e.stdout.decode())

    async def _test_codegen(self, env, cwd: pathlib.Path):
        async def run(*args, extra_env=None):
            if extra_env is None:
                env_ = env
            else:
                env_ = env.copy()
                env_.update(extra_env)
            p = await asyncio.create_subprocess_exec(
                *args,
                cwd=cwd,
                env=env_,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                await asyncio.wait_for(p.wait(), 120)
            except asyncio.TimeoutError:
                p.terminate()
                await p.wait()
                raise
            else:
                if p.returncode:
                    raise subprocess.CalledProcessError(
                        p.returncode, args, output=await p.stdout.read(),
                    )

        cmd = env.get("EDGEDB_PYTHON_TEST_CODEGEN_CMD", "gel-py")
        await run(
            cmd, extra_env={"EDGEDB_PYTHON_CODEGEN_PY_VER": "3.8.5"}
        )
        await run(
            cmd,
            "--target",
            "blocking",
            "--no-skip-pydantic-validation",
            extra_env={"EDGEDB_PYTHON_CODEGEN_PY_VER": "3.9.2"},
        )
        await run(
            cmd,
            "--target",
            "async",
            "--file",
            "--no-skip-pydantic-validation",
            extra_env={"EDGEDB_PYTHON_CODEGEN_PY_VER": "3.10.3"},
        )

        for f in cwd.rglob("*.py"):
            a = f.with_suffix(f".py{self.ASSERT_SUFFIX}")
            if not a.exists():
                a = f.with_suffix(".py.assert")
            self.assertEqual(f.read_text(), a.read_text(), msg=f.name)
        for a in cwd.rglob("*.py.assert"):
            f = a.with_suffix("")
            self.assertTrue(f.exists(), f"{f} doesn't exist")
