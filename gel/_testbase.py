#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import argparse
import asyncio
import atexit
import contextlib
import functools
import gc
import hashlib
import importlib.util
import inspect
import json
import linecache
import logging
import os
import pathlib
import pickle
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import types
import typing
import unittest
import warnings


if typing.TYPE_CHECKING:
    import pydantic


import gel
from gel import asyncio_client
from gel import blocking_client
from gel.orm.introspection import get_schema_json, GelORMWarning
from gel.orm.sqla import ModelGenerator as SQLAModGen
from gel.orm.sqlmodel import ModelGenerator as SQLModGen
from gel.orm.django.generator import ModelGenerator as DjangoModGen


log = logging.getLogger(__name__)

_unset = object()


@contextlib.contextmanager
def silence_asyncio_long_exec_warning():
    def flt(log_record):
        msg = log_record.getMessage()
        return not msg.startswith("Executing ")

    logger = logging.getLogger("asyncio")
    logger.addFilter(flt)
    try:
        yield
    finally:
        logger.removeFilter(flt)


def _get_wsl_path(win_path):
    return (
        re.sub(r"^([A-Z]):", lambda m: f"/mnt/{m.group(1)}", win_path)
        .replace("\\", "/")
        .lower()
    )


_default_cluster = None


def _start_cluster(*, cleanup_atexit=True):
    global _default_cluster

    if isinstance(_default_cluster, Exception):
        # when starting a server fails
        # don't retry starting one for every TestCase
        # because repeating the failure can take a while
        raise _default_cluster

    if _default_cluster:
        return _default_cluster

    try:
        tmpdir = tempfile.TemporaryDirectory()
        status_file = os.path.join(tmpdir.name, "server-status")

        # if running on windows adjust the path for WSL
        status_file_unix = _get_wsl_path(status_file)

        env = os.environ.copy()
        # Make sure the PYTHONPATH of _this_ process does
        # not interfere with the server's.
        env.pop("PYTHONPATH", None)

        gel_server = env.get("GEL_SERVER_BINARY")
        if not gel_server:
            gel_server = env.get("EDGEDB_SERVER_BINARY")

        if not gel_server:
            if sys.platform == "win32":
                which_res = subprocess.run(
                    ["wsl", "-u", "edgedb", "which", "gel-server"],
                    capture_output=True,
                    text=True,
                )
                if which_res.returncode != 0:
                    which_res = subprocess.run(
                        ["wsl", "-u", "edgedb", "which", "edgedb-server"],
                        capture_output=True,
                        text=True,
                    )
                if which_res.returncode != 0:
                    raise RuntimeError(
                        "could not find gel-server binary; "
                        "specify the path of the binary (in WSL) directly via "
                        "the GEL_SERVER_BINARY environment variable",
                    )
                gel_server = which_res.stdout.strip()
            else:
                gel_server = shutil.which("gel-server")
                if not gel_server:
                    gel_server = shutil.which("edgedb-server")
                if not gel_server:
                    raise RuntimeError(
                        "could not find gel-server binary; modify PATH "
                        "or specify the binary directly via the "
                        "GEL_SERVER_BINARY environment variable",
                    )

        version_args = [gel_server, "--version"]
        if sys.platform == "win32":
            version_args = ["wsl", "-u", "edgedb"] + version_args
        version_res = subprocess.run(
            version_args,
            capture_output=True,
            text=True,
        )
        is_gel = version_res.stdout.startswith("gel-server,")

        version_line = version_res.stdout
        is_gel = version_line.startswith("gel-server,")

        # The default role became admin in nightly build 9024 for 6.0
        if is_gel:
            if "6.0-dev" in version_line:
                rev = int(version_line.split(".")[2].split("+")[0])
                has_admin = rev >= 9024
            else:
                has_admin = True
        else:
            has_admin = False

        role = "admin" if has_admin else "edgedb"
        args = [
            gel_server,
            "--temp-dir",
            "--testmode",
            f"--emit-server-status={status_file_unix}",
            "--port=auto",
            "--auto-shutdown",
            f"--bootstrap-command=ALTER ROLE {role} {{SET password:='test'}}",
        ]

        help_args = [gel_server, "--help"]
        if sys.platform == "win32":
            help_args = ["wsl", "-u", "edgedb"] + help_args

        supported_opts = subprocess.run(
            help_args,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            env=env,
            cwd=tmpdir.name,
        ).stdout

        if "--tls-cert-mode" in supported_opts:
            args.append("--tls-cert-mode=generate_self_signed")
        elif "--generate-self-signed-cert" in supported_opts:
            args.append("--generate-self-signed-cert")

        if sys.platform == "win32":
            args = ["wsl", "-u", "edgedb"] + args

        if env.get("EDGEDB_DEBUG_SERVER"):
            server_stdout = None
        else:
            server_stdout = subprocess.DEVNULL

        p = subprocess.Popen(
            args,
            env=env,
            cwd=tmpdir.name,
            stdout=server_stdout,
            stderr=subprocess.STDOUT,
        )

        for _ in range(600):
            if p.poll() is not None:
                raise RuntimeError(
                    "Database server crashed before signalling READY status."
                    " Run with `env EDGEDB_DEBUG_SERVER=1` to debug."
                )
            try:
                with open(status_file, "rb") as f:
                    for line in f:
                        if line.startswith(b"READY="):
                            break
                    else:
                        raise RuntimeError("not ready")
                break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError("server status file not found")

        data = json.loads(line.split(b"READY=")[1])
        con_args = dict(host="localhost", port=data["port"])
        if "tls_cert_file" in data:
            if sys.platform == "win32":
                con_args["tls_ca_file"] = os.path.join(
                    tmpdir.name, "edbtlscert.pem"
                )
                subprocess.check_call(
                    [
                        "wsl",
                        "-u",
                        "edgedb",
                        "cp",
                        data["tls_cert_file"],
                        _get_wsl_path(con_args["tls_ca_file"]),
                    ]
                )
            else:
                con_args["tls_ca_file"] = data["tls_cert_file"]

        client = gel.create_client(password="test", **con_args)
        client.ensure_connected()
        client.execute("""
            # Set session_idle_transaction_timeout to 5 minutes.
            CONFIGURE INSTANCE SET session_idle_transaction_timeout :=
                <duration>'5 minutes';
        """)
        _default_cluster = {
            "proc": p,
            "client": client,
            "con_args": con_args,
        }

        if "tls_cert_file" in data:
            # Keep the temp dir which we also copied the cert from WSL
            _default_cluster["_tmpdir"] = tmpdir

        atexit.register(client.close)
    except Exception as e:
        _default_cluster = e
        raise e

    return _default_cluster


class TestCaseMeta(type(unittest.TestCase)):
    _database_names = set()

    @staticmethod
    def _iter_methods(bases, ns):
        for base in bases:
            for methname in dir(base):
                if not methname.startswith("test_"):
                    continue

                meth = getattr(base, methname)
                if not inspect.iscoroutinefunction(meth):
                    continue

                yield methname, meth

        for methname, meth in ns.items():
            if not methname.startswith("test_"):
                continue

            if not inspect.iscoroutinefunction(meth):
                continue

            yield methname, meth

    @classmethod
    def wrap(mcls, meth):
        @functools.wraps(meth)
        def wrapper(self, *args, __meth__=meth, **kwargs):
            try_no = 1

            while True:
                try:
                    # There might be unobvious serializability
                    # anomalies across the test suite, so, rather
                    # than hunting them down every time, simply
                    # retry the test.
                    self.loop.run_until_complete(
                        __meth__(self, *args, **kwargs)
                    )
                except gel.TransactionSerializationError:
                    if try_no == 3:
                        raise
                    else:
                        self.loop.run_until_complete(
                            self.client.execute("ROLLBACK;")
                        )
                        try_no += 1
                else:
                    break

        return wrapper

    @classmethod
    def add_method(mcls, methname, ns, meth):
        ns[methname] = mcls.wrap(meth)

    def __new__(mcls, name, bases, ns):
        for methname, meth in mcls._iter_methods(bases, ns.copy()):
            if methname in ns:
                del ns[methname]
            mcls.add_method(methname, ns, meth)

        cls = super().__new__(mcls, name, bases, ns)
        if not ns.get("BASE_TEST_CLASS") and hasattr(cls, "get_database_name"):
            dbname = cls.get_database_name()

            if name in mcls._database_names:
                raise TypeError(
                    f"{name} wants duplicate database name: {dbname}"
                )

            mcls._database_names.add(name)

        return cls


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):
    @classmethod
    def setUpClass(cls):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        asyncio.set_event_loop(None)

    def add_fail_notes(self, **kwargs):
        if not hasattr(self, "fail_notes"):
            self.fail_notes = {}
        self.fail_notes.update(kwargs)

    @contextlib.contextmanager
    def annotate(self, **kwargs):
        # Annotate the test in case the nested block of code fails.
        try:
            yield
        except Exception:
            self.add_fail_notes(**kwargs)
            raise

    @contextlib.contextmanager
    def assertRaisesRegex(self, exception, regex, msg=None, **kwargs):
        with super().assertRaisesRegex(exception, regex, msg=msg):
            try:
                yield
            except BaseException as e:
                if isinstance(e, exception):
                    for attr_name, expected_val in kwargs.items():
                        val = getattr(e, attr_name)
                        if val != expected_val:
                            raise self.failureException(
                                f"{exception.__name__} context attribute "
                                f"{attr_name!r} is {val} (expected "
                                f"{expected_val!r})"
                            ) from e
                raise

    def addCleanup(self, func, *args, **kwargs):
        @functools.wraps(func)
        def cleanup():
            res = func(*args, **kwargs)
            if inspect.isawaitable(res):
                self.loop.run_until_complete(res)

        super().addCleanup(cleanup)


class ClusterTestCase(TestCase):
    BASE_TEST_CLASS = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cluster = _start_cluster(cleanup_atexit=True)


class TestAsyncIOClient(gel.AsyncIOClient):
    def _clear_codecs_cache(self):
        self._impl.codecs_registry.clear_cache()

    @property
    def connection(self):
        return self._impl._holders[0]._con

    @property
    def dbname(self):
        return self._impl._working_params.database

    @property
    def is_proto_lt_1_0(self):
        return self.connection._protocol.is_legacy


class TestClient(gel.Client):
    @property
    def connection(self):
        return self._impl._holders[0]._con

    @property
    def is_proto_lt_1_0(self):
        return self.connection._protocol.is_legacy

    @property
    def dbname(self):
        return self._impl._working_params.database


class ConnectedTestCaseMixin:
    is_client_async = True

    @classmethod
    def make_test_client(
        cls,
        *,
        cluster=None,
        database="edgedb",
        user="edgedb",
        password="test",
        host=...,
        port=...,
        connection_class=...,
    ):
        conargs = cls.get_connect_args(
            cluster=cluster, database=database, user=user, password=password
        )
        if host is not ...:
            conargs["host"] = host
        if port is not ...:
            conargs["port"] = port
        if connection_class is ...:
            connection_class = (
                asyncio_client.AsyncIOConnection
                if cls.is_client_async
                else blocking_client.BlockingIOConnection
            )
        client_class = (
            TestAsyncIOClient
            if issubclass(connection_class, asyncio_client.AsyncIOConnection)
            else TestClient
        )
        return client_class(
            connection_class=connection_class,
            max_concurrency=1,
            **conargs,
        )

    @classmethod
    def get_connect_args(
        cls, *, cluster=None, database="edgedb", user="edgedb", password="test"
    ):
        conargs = cls.cluster["con_args"].copy()
        conargs.update(dict(user=user, password=password, database=database))
        return conargs

    @classmethod
    def adapt_call(cls, coro):
        return cls.loop.run_until_complete(coro)


MAX_BRANCH_NAME_LEN = 51


class DatabaseTestCase(ClusterTestCase, ConnectedTestCaseMixin):
    SETUP = None
    TEARDOWN = None
    SCHEMA = None
    DEFAULT_MODULE = "test"

    SETUP_METHOD = None
    TEARDOWN_METHOD = None

    BASE_TEST_CLASS = True
    TEARDOWN_RETRY_DROP_DB = 1

    ISOLATED_TEST_BRANCHES = False

    def setUp(self):
        if self.ISOLATED_TEST_BRANCHES:
            cls = type(self)
            root = cls.get_database_name()

            testdb = (
                f"{os.getpid()}_{type(self).__name__}_{self._testMethodName}"
            )
            testdb = "_" + hashlib.sha1(testdb.encode()).hexdigest()
            self.__testdb__ = testdb

            self.adapt_call(
                cls.admin_client.query(f"""
                    create data branch {testdb} from {root};
                """)
            )
            self.client = cls.make_test_client(database=testdb)._with_debug(
                save_postcheck=True,
            )
            self.adapt_call(self.client.ensure_connected())

        if self.SETUP_METHOD:
            self.adapt_call(self.client.execute(self.SETUP_METHOD))

        super().setUp()

    def tearDown(self):
        try:
            if self.TEARDOWN_METHOD:
                self.adapt_call(self.client.execute(self.TEARDOWN_METHOD))
        finally:
            try:
                if (
                    self.client.connection
                    and self.client.connection.is_in_transaction()
                ):
                    raise AssertionError(
                        "test connection is still in transaction "
                        "*after* the test"
                    )

            finally:
                if self.ISOLATED_TEST_BRANCHES:
                    self.client.terminate()
                    self.client = None

                    self.adapt_call(
                        type(self).admin_client.query(f"""
                            drop branch {self.__testdb__};
                        """)
                    )

                super().tearDown()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dbname = cls.get_database_name()

        cls.admin_client = None

        class_set_up = os.environ.get("EDGEDB_TEST_CASES_SET_UP")

        # Only open an extra admin connection if necessary.
        if not class_set_up:
            script = f"CREATE DATABASE {dbname};"
            cls.admin_client = cls.make_test_client()
            cls.adapt_call(cls.admin_client.execute(script))

        cls.client = cls.make_test_client(database=dbname)
        cls.server_version = cls.adapt_call(
            cls.client.query_required_single("""
                select sys::get_version()
            """)
        )

        if not class_set_up:
            script = cls.get_setup_script()
            if script:
                # The setup is expected to contain a CREATE MIGRATION,
                # which needs to be wrapped in a transaction.
                if cls.is_client_async:

                    async def execute():
                        async for tr in cls.client.transaction():
                            async with tr:
                                await tr.execute(script)
                else:

                    def execute():
                        for tr in cls.client.transaction():
                            with tr:
                                tr.execute(script)

                cls.adapt_call(execute())

    @classmethod
    def get_database_name(cls):
        if cls.__name__.startswith("TestEdgeQL"):
            dbname = cls.__name__[len("TestEdgeQL") :]
        elif cls.__name__.startswith("Test"):
            dbname = cls.__name__[len("Test") :]
        else:
            dbname = cls.__name__

        return dbname.lower()

    @classmethod
    def get_setup_script(cls):
        script = ""
        schema = []

        # Look at all SCHEMA entries and potentially create multiple
        # modules, but always create the test module, if not `default`.
        if cls.DEFAULT_MODULE != "default":
            schema.append(f"\nmodule {cls.DEFAULT_MODULE} {{}}")
        for name, val in cls.__dict__.items():
            m = re.match(r"^SCHEMA(?:_(\w+))?", name)
            if m:
                module_name = (
                    (m.group(1) or cls.DEFAULT_MODULE)
                    .lower()
                    .replace("_", "::")
                )

                with open(val, "r") as sf:
                    module = sf.read()

                if f"module {module_name}" not in module:
                    schema.append(f"\nmodule {module_name} {{ {module} }}")
                else:
                    schema.append(module)

        # Don't wrap the script into a transaction here, so that
        # potentially it's easier to stitch multiple such scripts
        # together in a fashion similar to what `edb inittestdb` does.
        script += f"\nSTART MIGRATION TO {{ {''.join(schema)} }};"
        script += f"\nPOPULATE MIGRATION; \nCOMMIT MIGRATION;"

        if cls.SETUP:
            if not isinstance(cls.SETUP, (list, tuple)):
                scripts = [cls.SETUP]
            else:
                scripts = cls.SETUP

            for scr in scripts:
                if "\n" not in scr and os.path.exists(scr):
                    with open(scr, "rt") as f:
                        setup = f.read()
                else:
                    setup = scr

                script += "\n" + setup

        return script.strip(" \n")

    @classmethod
    def tearDownClass(cls):
        script = ""

        if cls.TEARDOWN:
            script = cls.TEARDOWN.strip()

        try:
            if script:
                cls.adapt_call(cls.client.execute(script))
        finally:
            try:
                cls.client.terminate()

                dbname = cls.get_database_name()
                script = f"DROP DATABASE {dbname};"

                retry = cls.TEARDOWN_RETRY_DROP_DB
                for i in range(retry):
                    try:
                        cls.adapt_call(cls.admin_client.execute(script))
                    except gel.errors.ExecutionError:
                        if i < retry - 1:
                            time.sleep(0.1)
                        else:
                            raise
                    except gel.errors.UnknownDatabaseError:
                        break

            except Exception:
                log.exception("error running teardown")
                # skip the exception so that original error is shown instead
                # of finalizer error
            finally:
                try:
                    if cls.admin_client is not None:
                        if cls.is_client_async:
                            cls.adapt_call(cls.admin_client.aclose())
                        else:
                            cls.admin_client.close()
                finally:
                    super().tearDownClass()


class AsyncQueryTestCase(DatabaseTestCase):
    BASE_TEST_CLASS = True
    is_client_async = True


class SyncQueryTestCase(DatabaseTestCase):
    BASE_TEST_CLASS = True
    TEARDOWN_RETRY_DROP_DB = 5
    is_client_async = False

    @classmethod
    def adapt_call(cls, result):
        return result


class BaseModelTestCase(DatabaseTestCase):
    DEFAULT_MODULE = "default"

    client: typing.ClassVar[gel.Client]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        if "models" in sys.modules:
            raise RuntimeError('"models" module has already been imported')

        cls.orm_debug = os.environ.get("GEL_PYTHON_TEST_ORM") in {"1", "true"}

        td_kwargs = {}
        if sys.version_info >= (3, 12):
            td_kwargs["delete"] = not cls.orm_debug

        cls.tmp_model_dir = tempfile.TemporaryDirectory(**td_kwargs)

        assert isinstance(cls.SCHEMA, str)
        short_name = pathlib.Path(cls.SCHEMA).stem

        if cls.orm_debug:
            print(cls.tmp_model_dir.name)

        from gel._internal._codegen._models import PydanticModelsGenerator

        gen_client = cls.make_test_client(
            connection_class=blocking_client.BlockingIOConnection,
            database=cls.get_database_name(),
        )
        try:
            gen_client.ensure_connected()
            base = pathlib.Path(cls.tmp_model_dir.name) / "models"

            cls.gen = PydanticModelsGenerator(
                argparse.Namespace(
                    no_cache=True,
                    quiet=True,
                    output=(
                        base / short_name
                    ).absolute(),
                ),
                project_dir=pathlib.Path(cls.tmp_model_dir.name),
                client=gen_client,
                interactive=False,
            )

            try:
                cls.gen.run()
                # Make sure the base "models" directory has an __init__.py
                (base / "__init__.py").touch()

            except Exception as e:
                raise RuntimeError(
                    f"error running model codegen, its stderr:\n"
                    f"{cls.gen.get_error_output()}"
                ) from e
        finally:
            gen_client.terminate()

        sys.path.insert(0, cls.tmp_model_dir.name)

        import models
        assert (
            models.__file__
            == os.path.join(cls.tmp_model_dir.name, 'models', '__init__.py')
        ), (
            models.__file__,
            os.path.join(cls.tmp_model_dir.name, 'models', '__init__.py')
        )

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            sys.path.remove(cls.tmp_model_dir.name)

            for mod_name in tuple(sys.modules.keys()):
                if mod_name.startswith("models.") or mod_name == "models":
                    del sys.modules[mod_name]

            importlib.invalidate_caches()
            linecache.clearcache()
            gc.collect()

            if not cls.orm_debug:
                cls.tmp_model_dir.cleanup()

    def assertScalarsEqual(
        self,
        tname: str,
        name: str,
        prop: str,
    ) -> None:
        self.assertTrue(
            self.client.query_single(f"""
                with
                    A := assert_single((
                        select {tname}
                        filter .name = 'hello world'
                    )),
                    B := assert_single((
                        select {tname}
                        filter .name = {name!r}
                    )),
                select A.{prop} = B.{prop}
            """),
            f"property {prop!r} value does not match",
        )

    @contextlib.contextmanager
    def assertWarns(
        self,
        *,
        msg_part: str,
        exp_category: type[Warning] = UserWarning,
    ):
        frame = sys._getframe(2)
        exp_filename = frame.f_code.co_filename

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                expected_line = frame.f_lineno + 1
                yield exp_filename

            self.assertTrue(w, "No warning captured")

            wm = w[0]  # warnings.WarningMessage
            self.assertEqual(
                pathlib.Path(wm.filename).resolve(),
                pathlib.Path(exp_filename).resolve(),
            )
            self.assertEqual(wm.lineno, expected_line)
            self.assertIs(wm.category, exp_category)
            self.assertIn(msg_part, str(wm.message))
        finally:
            del frame

    @contextlib.contextmanager
    def assertNotWarns(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            yield

        if w:
            captured_warnings = [
                f"{wm.category.__name__}: {wm.message}" for wm in w
            ]
            self.fail(
                f"Unexpected warning(s) captured:\n"
                + "\n".join(captured_warnings)
            )

    def assertPydanticChangedFields(
        self,
        model: pydantic.BaseModel,
        expected: typing.Set[str],
        *,
        expected_gel: typing.Set[str] | None = None,
    ) -> None:
        # We don't have a default for GelModel.id so we have to pass it
        # manually and then remove it right after the model is built.
        # That's why 'id' is always in changed fields. It doesn't matter
        # though. The main consumer of __gel_get_changed_fields__ is
        # `save()`, which just ignores `id` and relies on `__gel_new__`.
        self.assertEqual(model.__pydantic_fields_set__ - {"id"}, expected)
        if hasattr(model, "__gel_get_changed_fields__"):
            self.assertEqual(
                model.__gel_get_changed_fields__() - {"id"},  # type: ignore
                expected if expected_gel is None else expected_gel,
            )

    def assertPydanticSerializes(
        self,
        model: pydantic.BaseModel,
        expected: typing.Any = _unset,
    ) -> None:
        context = {}
        try:
            model.model_dump()
        except ValueError as e:
            if "If you have to dump an unsaved model" in str(e):
                context["gel_allow_unsaved"] = True
            else:
                raise

        if expected is not _unset:
            self.assertEqual(
                pop_ids(model.model_dump(context=context)), expected
            )
            self.assertEqual(
                json.loads(
                    pop_ids_json(model.model_dump_json(context=context))
                ),
                expected,
            )

        # Test that these two don't fail.
        model.model_json_schema(mode="serialization")
        model.model_json_schema(mode="validation")

        # Pydantic's model_validate() doesn't support computed fields,
        # but model_dump() cannot not include them by default.
        context["gel_exclude_computeds"] = True
        new_context = context.copy()
        new_context["gel_allow_unsaved"] = True

        new = type(model).model_validate(
            model.model_dump(
                context=context,
            )
        )
        self.assertEqual(
            new.model_dump(context=new_context),
            model.model_dump(context=context),
        )

        new = type(model)(**model.model_dump(context=context))
        self.assertEqual(
            new.model_dump(context=new_context),
            model.model_dump(context=context),
        )

        new = type(model).model_validate_json(
            model.model_dump_json(context=new_context)
        )
        self.assertEqual(
            json.loads(new.model_dump_json(context=new_context)),
            json.loads(model.model_dump_json(context=context)),
        )

    def assertPydanticPickles(
        self,
        model: pydantic.BaseModel,
    ) -> None:
        context = {}
        try:
            model.model_dump()
        except ValueError as e:
            if "If you have to dump an unsaved model" in str(e):
                context["gel_allow_unsaved"] = True
            else:
                raise

        dump = model.model_dump(context=context)
        dump_json = model.model_dump_json(context=context)
        model2 = repickle(model)
        self.assertEqual(dump, model2.model_dump(context=context))
        self.assertEqual(dump_json, model2.model_dump_json(context=context))
        self.assertEqual(
            model.__pydantic_fields_set__,
            model2.__pydantic_fields_set__,
        )
        self.assertEqual(
            getattr(model, "__gel_changed_fields__", ...),
            getattr(model2, "__gel_changed_fields__", ...),
        )


class ModelTestCase(SyncQueryTestCase, BaseModelTestCase):
    client: gel.Client


class AsyncModelTestCase(AsyncQueryTestCase, BaseModelTestCase):
    client: gel.AsyncIOClient


class ORMTestCase(SyncQueryTestCase):
    MODEL_PACKAGE = None
    DEFAULT_MODULE = "default"

    @classmethod
    def setUpClass(cls):
        # ORMs rely on psycopg2 to connect to Postgres and thus we
        # need it to run tests. Unfortunately not all test environemnts might
        # have psycopg2 installed, as long as we run this in the test
        # environments that have this, it is fine since we're not expecting
        # different functionality based on flavours of psycopg2.
        if importlib.util.find_spec("psycopg2") is None:
            raise unittest.SkipTest("need psycopg2 for ORM tests")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", GelORMWarning)

            super().setUpClass()

            class_set_up = os.environ.get("EDGEDB_TEST_CASES_SET_UP")
            if not class_set_up:
                # We'll need a temp directory to setup the generated Python
                # package
                cls.tmpormdir = tempfile.TemporaryDirectory()
                sys.path.append(cls.tmpormdir.name)
                # Now that the DB is setup, generate the ORM models from it
                cls.spec = get_schema_json(cls.client)
                cls.setupORM()

    @classmethod
    def setupORM(cls):
        raise NotImplementedError

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            # cleanup the temp modules
            sys.path.remove(cls.tmpormdir.name)
            cls.tmpormdir.cleanup()


class SQLATestCase(ORMTestCase):
    @classmethod
    def setupORM(cls):
        gen = SQLAModGen(
            outdir=os.path.join(cls.tmpormdir.name, cls.MODEL_PACKAGE),
            basemodule=cls.MODEL_PACKAGE,
        )
        gen.render_models(cls.spec)

    @classmethod
    def get_dsn_for_sqla(cls):
        cargs = cls.get_connect_args(database=cls.get_database_name())
        dsn = (
            f"postgresql://{cargs['user']}:{cargs['password']}"
            f"@{cargs['host']}:{cargs['port']}/{cargs['database']}"
        )

        return dsn


class SQLModelTestCase(ORMTestCase):
    @classmethod
    def setupORM(cls):
        gen = SQLModGen(
            outdir=os.path.join(cls.tmpormdir.name, cls.MODEL_PACKAGE),
            basemodule=cls.MODEL_PACKAGE,
        )
        gen.render_models(cls.spec)

    @classmethod
    def get_dsn_for_sqla(cls):
        cargs = cls.get_connect_args(database=cls.get_database_name())
        dsn = (
            f"postgresql://{cargs['user']}:{cargs['password']}"
            f"@{cargs['host']}:{cargs['port']}/{cargs['database']}"
        )

        return dsn


APPS_PY = """\
from django.apps import AppConfig


class TestConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = {name!r}
"""

SETTINGS_PY = """\
from pathlib import Path

mysettings = dict(
    INSTALLED_APPS=[
        '{appname}.apps.TestConfig',
        'gel.orm.django.gelmodels.apps.GelPGModel',
    ],
    DATABASES={{
        'default': {{
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': {database!r},
            'USER': {user!r},
            'PASSWORD': {password!r},
            'HOST': {host!r},
            'PORT': {port!r},
        }}
    }},
)
"""


class DjangoTestCase(ORMTestCase):
    @classmethod
    def setupORM(cls):
        pkgbase = os.path.join(cls.tmpormdir.name, cls.MODEL_PACKAGE)
        # Set up the package for testing Django models
        os.mkdir(pkgbase)
        open(os.path.join(pkgbase, "__init__.py"), "w").close()
        with open(os.path.join(pkgbase, "apps.py"), "wt") as f:
            print(
                APPS_PY.format(name=cls.MODEL_PACKAGE),
                file=f,
            )

        with open(os.path.join(pkgbase, "settings.py"), "wt") as f:
            cargs = cls.get_connect_args(database=cls.get_database_name())
            print(
                SETTINGS_PY.format(
                    appname=cls.MODEL_PACKAGE,
                    database=cargs["database"],
                    user=cargs["user"],
                    password=cargs["password"],
                    host=cargs["host"],
                    port=cargs["port"],
                ),
                file=f,
            )

        models = os.path.join(pkgbase, "models.py")
        gen = DjangoModGen(out=models)
        gen.render_models(cls.spec)


_lock_cnt = 0


def gen_lock_key():
    global _lock_cnt
    _lock_cnt += 1
    return os.getpid() * 1000 + _lock_cnt


def _typecheck(func, *, xfail=False):
    wrapped = inspect.unwrap(func)
    is_async = inspect.iscoroutinefunction(wrapped)

    source_code = inspect.getsource(wrapped)
    lines = source_code.splitlines()
    body_offset: int
    for lineno, line in enumerate(lines):
        if line.strip().startswith("async def " if is_async else "def "):
            body_offset = lineno
            break
    else:
        raise RuntimeError(f"couldn't extract source of {func}")

    source_code = "\n".join(lines[body_offset + 1 :])
    dedented_body = textwrap.dedent(source_code)
    base_class_name = "AsyncModelTestCase" if is_async else "ModelTestCase"

    source_code = f"""\
import unittest
import typing

import gel

from gel import _testbase

if not typing.TYPE_CHECKING:
    def reveal_type(_: typing.Any) -> str:
        return ''

class TestModel(_testbase.{base_class_name}):
    if typing.TYPE_CHECKING:
    {
        "      client: typing.ClassVar[gel.AsyncIOClient] = "
        "gel.create_async_client()  # type: ignore[misc]"
        if is_async
        else "       client: typing.ClassVar[gel.Client] = "
        "gel.create_client()  # type: ignore[misc]"
    }

    {"async " if is_async else ""}def {wrapped.__name__}(self) -> None:
{textwrap.indent(dedented_body, "    " * 2)}
    """

    def run(self):
        d = type(self).tmp_model_dir.name

        testfn = pathlib.Path(d) / "test.py"
        inifn = pathlib.Path(d) / "mypy.ini"

        with open(testfn, "wt") as f:
            f.write(source_code)

        with open(inifn, "wt") as f:
            f.write(
                textwrap.dedent(f"""\
                [mypy]
                strict = True
                ignore_errors = False
                follow_imports = normal
                show_error_codes = True
                local_partial_types = True

                # This is very intentional as it allows us to type check things
                # that must be flagged as an error by the type checker.
                # Don't "type: ignore" unless it's part of the test.
                warn_unused_ignores = True
            """)
            )

        try:
            cmd = [
                sys.executable,
                "-m",
                "mypy",
                "--strict",
                "--no-strict-equality",
                "--config-file",
                inifn,
                testfn,
            ]

            res = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
                cwd=inifn.parent,
            )
        finally:
            inifn.unlink()
            testfn.unlink()

        if res.returncode != 0:
            lines = source_code.split("\n")
            pad_width = max(2, len(str(len(lines))))
            source_code_numbered = "\n".join(
                f"{i + 1:0{pad_width}d}: {line}"
                for i, line in enumerate(lines)
            )

            raise RuntimeError(
                f"mypy check failed for {func.__name__} "
                f"\n\ntest code:\n{source_code_numbered}"
                f"\n\nmypy stdout:\n{res.stdout.decode()}"
                f"\n\nmypy stderr:\n{res.stderr.decode()}"
            )

        types = []

        out = res.stdout.decode().split("\n")
        for line in out:
            if m := re.match(r'.*Revealed type is "(?P<name>[^"]+)".*', line):
                types.append(m.group("name"))

        def reveal_type(_, *, ncalls=[0], types=types):  # noqa: B006
            ncalls[0] += 1
            try:
                return types[ncalls[0] - 1]
            except IndexError:
                return None

        if is_async:
            # `func` is the result of `TestCaseMeta.wrap()`, so we
            # want to use the coroutine function that was there in
            # the beginning, so let's use `wrapped`
            wrapped.__globals__["reveal_type"] = reveal_type
            return wrapped(self)
        else:
            func.__globals__["reveal_type"] = reveal_type
            return func(self)

    if is_async:

        @functools.wraps(wrapped)
        async def runner(self, run=run):
            coro = run(self)
            await coro

        rewrapped = TestCaseMeta.wrap(runner)
        return unittest.expectedFailure(rewrapped) if xfail else rewrapped

    else:
        run = functools.wraps(func)(run)
        return unittest.expectedFailure(run) if xfail else run


def typecheck(arg):
    """Type-check one test of the entire test cases class.

    This is designed to type check unit tests that work with reflected Gel
    schemas and the query builder APIs.
    """
    # Please don't add arguments to this decorator, thank you.
    if isinstance(arg, type):
        for func in arg.__dict__.values():
            if not isinstance(func, types.FunctionType):
                continue
            if not func.__name__.startswith("test_"):
                continue
            new_func = typecheck(func)
            setattr(arg, func.__name__, new_func)
        return arg
    else:
        assert isinstance(arg, types.FunctionType)
        if hasattr(arg, "_typecheck_skipped"):
            return arg
        return _typecheck(arg)


def skip_typecheck(arg):
    """Explicitly opt out the decorated test from being type-checked.

    Example:

        @tb.typecheck                                 # type-check all tests...
        class TestModelGenerator(tb.ModelTestCase):

            @tb.skip_typecheck                        # ...but this one
            def test_foo(self):
                ...


    Use it for tests where the test isn't testing the typesefety of the public
    API, but rather performs some other kind of testing. @typecheck is designed
    to test the reflected schema and the query builder APIs, everything else
    is the job of the IDE's and CI's type-checkers.
    """
    assert isinstance(arg, types.FunctionType)
    arg._typecheck_skipped = True


def must_fail(f):
    return unittest.expectedFailure(f)


def to_be_fixed(f):
    return unittest.expectedFailure(f)


def xfail(f):
    return unittest.expectedFailure(f)


if os.environ.get("USE_UVLOOP"):
    import uvloop

    uvloop.install()


def pop_ids(dct: typing.Any) -> typing.Any:
    if isinstance(dct, list):
        for item in dct:
            pop_ids(item)
        return dct
    else:
        assert isinstance(dct, dict)
        dct.pop("id", None)
        for k, v in dct.items():
            if isinstance(v, list):
                for item in v:
                    pop_ids(item)
            elif isinstance(v, dict):
                dct[k] = pop_ids(v)
        return dct


def pop_ids_json(js: str) -> str:
    dct = json.loads(js)
    assert isinstance(dct, (dict, list))
    pop_ids(dct)
    return json.dumps(dct)


T = typing.TypeVar("T")


def repickle(obj: T) -> T:
    # Use pure Python implementations (_dumps & _loads)
    # for better debugging when shtf.
    pickle._loads(pickle._dumps(obj))

    # And test that native implementation works as well
    # (kind of redundant, but just in case).
    return pickle.loads(pickle.dumps(obj))
