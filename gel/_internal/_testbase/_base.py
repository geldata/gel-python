# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# n_a
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    ClassVar,
    Concatenate,
    ParamSpec,
    TypeVar,
    TYPE_CHECKING,
)

import asyncio
import contextlib
import functools
import hashlib
import inspect
import logging
import os
import re
import time
import unittest


import gel
from gel import asyncio_client
from gel import blocking_client

from gel._internal import _captive_server


if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterator, Callable


_P = ParamSpec("_P")
_R = TypeVar("_R", covariant=True)


log = logging.getLogger(__name__)


@contextlib.contextmanager
def silence_asyncio_long_exec_warning() -> Iterator[None]:
    def flt(log_record: logging.LogRecord) -> bool:
        msg = log_record.getMessage()
        return not msg.startswith("Executing ")

    logger = logging.getLogger("asyncio")
    logger.addFilter(flt)
    try:
        yield
    finally:
        logger.removeFilter(flt)


_default_instance: _captive_server.BaseInstance | Exception | None = None


async def _start_instance() -> _captive_server.BaseInstance:
    global _default_instance  # noqa: PLW0603

    if isinstance(_default_instance, Exception):
        # when starting a server fails
        # don't retry starting one for every TestCase
        # because repeating the failure can take a while
        raise _default_instance

    if _default_instance is not None:
        return _default_instance

    try:
        _default_instance = _captive_server.TestInstance()
        await _default_instance.start()
    except Exception as e:
        _default_instance = e
        raise

    return _default_instance


_TestCase_T = TypeVar("_TestCase_T", bound="TestCase")


class TestCaseMeta(type):
    _database_names: ClassVar[set[str]] = set()

    @staticmethod
    def _iter_methods(
        bases: tuple[type, ...], ns: dict[str, Any]
    ) -> Iterator[tuple[str, Callable[..., Any]]]:
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
    def wrap(
        cls,
        meth: Callable[Concatenate[_TestCase_T, _P], Awaitable[_R]],
    ) -> Callable[Concatenate[_TestCase_T, _P], _R]:
        @functools.wraps(meth)
        def wrapper(
            self: _TestCase_T,
            *args: _P.args,
            **kwargs: _P.kwargs,
        ) -> _R:
            try_no = 1

            while True:
                try:
                    # There might be unobvious serializability
                    # anomalies across the test suite, so, rather
                    # than hunting them down every time, simply
                    # retry the test.
                    return self.loop.run_until_complete(
                        meth(self, *args, **kwargs)
                    )
                except gel.TransactionSerializationError:  # noqa: PERF203
                    if try_no == 3:
                        raise
                    else:
                        self.loop.run_until_complete(
                            self.client.execute("ROLLBACK;")  # type: ignore [attr-defined]
                        )
                        try_no += 1
                else:
                    break

        return wrapper  # type: ignore [return-value]

    @classmethod
    def add_method(
        cls,
        methname: str,
        ns: dict[str, Any],
        meth: Callable[..., Any],
    ) -> None:
        ns[methname] = cls.wrap(meth)

    def __new__(
        mcls, name: str, bases: tuple[type, ...], ns: dict[str, Any]
    ) -> type:
        for methname, meth in mcls._iter_methods(bases, ns.copy()):
            ns.pop(methname, None)
            mcls.add_method(methname, ns, meth)

        cls = super().__new__(mcls, name, bases, ns)
        if not ns.get("BASE_TEST_CLASS") and hasattr(cls, "get_database_name"):
            dbname = cls.get_database_name()  # pyright: ignore[reportAttributeAccessIssue]

            if name in mcls._database_names:
                raise TypeError(
                    f"{name} wants duplicate database name: {dbname}"
                )

            mcls._database_names.add(name)

        return cls


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):
    loop: ClassVar[asyncio.AbstractEventLoop]

    @classmethod
    def setUpClass(cls) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls) -> None:
        cls.loop.close()
        asyncio.set_event_loop(None)

    @classmethod
    def adapt_call(cls, coro: Any) -> Any:
        return cls.loop.run_until_complete(coro)

    def add_fail_notes(self, **kwargs: Any) -> None:
        if not hasattr(self, "fail_notes"):
            self.fail_notes = {}
        self.fail_notes.update(kwargs)

    @contextlib.contextmanager
    def annotate(self, **kwargs: Any) -> Iterator[None]:
        # Annotate the test in case the nested block of code fails.
        try:
            yield
        except Exception:
            self.add_fail_notes(**kwargs)
            raise

    @contextlib.contextmanager
    def assertRaisesRegex(  # type: ignore[override]  # noqa: N802
        self,
        exception: type[BaseException],
        regex: str,
        msg: str | None = None,
        **kwargs: Any,
    ) -> Iterator[None]:
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

    def addCleanup(  # type: ignore[override]  # noqa: N802
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> None:
        @functools.wraps(func)
        def cleanup() -> None:
            res = func(*args, **kwargs)
            if inspect.isawaitable(res):
                self.loop.run_until_complete(res)

        super().addCleanup(cleanup)


class InstanceTestCase(TestCase):
    BASE_TEST_CLASS = True
    instance: ClassVar[_captive_server.BaseInstance]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        loop = asyncio.get_event_loop()
        cls.instance = loop.run_until_complete(_start_instance())

    @classmethod
    def get_connect_args(
        cls,
        /,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return cls.instance.get_connect_args() | kwargs


MAX_BRANCH_NAME_LEN = 51


class BranchTestCase(InstanceTestCase):
    SETUP: str | list[str] | None = None
    TEARDOWN: str | None = None
    SCHEMA: str | None = None
    DEFAULT_MODULE = "test"

    SETUP_METHOD: str | None = None
    TEARDOWN_METHOD: str | None = None

    BASE_TEST_CLASS = True
    TEARDOWN_RETRY_DROP_DB = 1

    ISOLATED_TEST_BRANCHES = False

    server_version: ClassVar[str]
    client: ClassVar[TestClient | TestAsyncIOClient]
    admin_client: ClassVar[TestClient | TestAsyncIOClient]

    @classmethod
    def make_test_client(
        cls,
        *,
        connection_class: type[
            asyncio_client.AsyncIOConnection
            | blocking_client.BlockingIOConnection
        ]
        | None = None,
        **kwargs: str,
    ) -> TestAsyncIOClient | TestClient:
        raise NotImplementedError

    @classmethod
    def make_blocking_test_client(
        cls,
        *,
        connection_class: type[blocking_client.BlockingIOConnection]
        | None = None,
        **kwargs: str,
    ) -> TestClient:
        if connection_class is None:
            connection_class = blocking_client.BlockingIOConnection
        return cls.instance.create_blocking_client(
            client_class=TestClient,
            connection_class=connection_class,
            **cls.get_connect_args(**kwargs),
        )

    @classmethod
    def make_async_test_client(
        cls,
        *,
        connection_class: type[asyncio_client.AsyncIOConnection] | None = None,
        **kwargs: str,
    ) -> TestAsyncIOClient:
        if connection_class is None:
            connection_class = asyncio_client.AsyncIOConnection
        return cls.instance.create_async_client(
            client_class=TestAsyncIOClient,
            connection_class=connection_class,
            **cls.get_connect_args(**kwargs),
        )

    def setUp(self) -> None:
        if self.ISOLATED_TEST_BRANCHES:
            cls = type(self)
            root = cls.get_database_name()

            testdb = (
                f"{os.getpid()}_{type(self).__name__}_{self._testMethodName}"
            )
            testdb = "_" + hashlib.sha1(testdb.encode()).hexdigest()  # noqa: S324
            self.__testdb__ = testdb

            if cls.admin_client is None:
                raise RuntimeError("admin_client is None")
            self.adapt_call(
                cls.admin_client.query(f"""
                    create data branch {testdb} from {root};
                """)
            )
            client = cls.make_test_client(database=testdb)._with_debug(
                save_postcheck=True,
            )
            self.client = client  # type: ignore[misc]
            self.adapt_call(self.client.ensure_connected())

        if self.SETUP_METHOD:
            self.adapt_call(self.client.execute(self.SETUP_METHOD))

        super().setUp()

    def tearDown(self) -> None:
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
                    # Need to clear instance client reference
                    object.__setattr__(self, "client", None)  # noqa: PLC2801

                    admin_client = type(self).admin_client
                    if admin_client is None:
                        raise RuntimeError("admin_client is None")
                    self.adapt_call(
                        admin_client.query(f"""
                            drop branch {self.__testdb__};
                        """)
                    )

                super().tearDown()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        dbname = cls.get_database_name()

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
                cls.adapt_call(cls.client.execute(script))

    @classmethod
    def get_database_name(cls) -> str:
        if cls.__name__.startswith("TestEdgeQL"):
            dbname = cls.__name__[len("TestEdgeQL") :]
        elif cls.__name__.startswith("Test"):
            dbname = cls.__name__[len("Test") :]
        else:
            dbname = cls.__name__

        return dbname.lower()

    @classmethod
    def get_setup_script(cls) -> str:
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

                with open(val) as sf:  # noqa: PLW1514, FURB101
                    module = sf.read()

                if f"module {module_name}" not in module:
                    schema.append(f"\nmodule {module_name} {{ {module} }}")
                else:
                    schema.append(module)

        # Don't wrap the script into a transaction here, so that
        # potentially it's easier to stitch multiple such scripts
        # together in a fashion similar to what `edb inittestdb` does.
        script += f"\nSTART MIGRATION TO {{ {''.join(schema)} }};"
        script += "\nPOPULATE MIGRATION; \nCOMMIT MIGRATION;"

        if cls.SETUP:
            if not isinstance(cls.SETUP, (list, tuple)):
                scripts = [cls.SETUP]
            else:
                scripts = list(cls.SETUP)

            for scr in scripts:
                if "\n" not in scr and os.path.exists(scr):
                    with open(scr) as f:  # noqa: PLW1514, FURB101
                        setup = f.read()
                else:
                    setup = scr

                script += "\n" + setup

        return script.strip(" \n")

    @classmethod
    def tearDownClass(cls) -> None:
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
                if cls.admin_client is None:
                    raise RuntimeError("admin_client is None")
                for i in range(retry):
                    try:
                        cls.adapt_call(cls.admin_client.execute(script))
                    except gel.errors.ExecutionError:  # noqa: PERF203
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
                        if isinstance(
                            cls.admin_client, asyncio_client.AsyncIOClient
                        ):
                            cls.adapt_call(cls.admin_client.aclose())
                        else:
                            cls.admin_client.close()
                finally:
                    super().tearDownClass()


class TestAsyncIOClient(gel.AsyncIOClient):
    def _clear_codecs_cache(self) -> None:
        self._impl.codecs_registry.clear_cache()

    @property
    def connection(self) -> Any:
        return self._impl._holders[0]._con

    @property
    def dbname(self) -> str:
        params = self._impl._working_params
        if params is None:
            raise RuntimeError("No connection parameters available")
        return params.database  # type: ignore[no-any-return]

    @property
    def is_proto_lt_1_0(self) -> bool:
        return self.connection._protocol.is_legacy  # type: ignore[no-any-return]


class AsyncQueryTestCase(BranchTestCase):
    BASE_TEST_CLASS = True

    client: ClassVar[TestAsyncIOClient]  # pyright: ignore [reportIncompatibleVariableOverride]
    admin_client: ClassVar[TestAsyncIOClient]  # pyright: ignore [reportIncompatibleVariableOverride]

    @classmethod
    def make_test_client(  # pyright: ignore [reportIncompatibleMethodOverride]
        cls,
        *,
        connection_class: type[asyncio_client.AsyncIOConnection] | None = None,  # type: ignore [override]
        **kwargs: str,
    ) -> TestAsyncIOClient:
        return cls.make_async_test_client(
            connection_class=connection_class,
            **kwargs,
        )


class TestClient(gel.Client):
    @property
    def connection(self) -> Any:
        return self._impl._holders[0]._con

    @property
    def is_proto_lt_1_0(self) -> bool:
        return bool(self.connection._protocol.is_legacy)

    @property
    def dbname(self) -> str:
        params = self._impl._working_params
        if params is None:
            raise RuntimeError("No connection parameters available")
        return params.database  # type: ignore[no-any-return]


class SyncQueryTestCase(BranchTestCase):
    BASE_TEST_CLASS = True
    TEARDOWN_RETRY_DROP_DB = 5

    client: ClassVar[TestClient]  # pyright: ignore [reportIncompatibleVariableOverride]
    admin_client: ClassVar[TestClient]  # pyright: ignore [reportIncompatibleVariableOverride]

    @classmethod
    def adapt_call(cls, coro: Any) -> Any:
        return coro

    @classmethod
    def make_test_client(  # pyright: ignore [reportIncompatibleMethodOverride]
        cls,
        *,
        connection_class: type[blocking_client.BlockingIOConnection]  # type: ignore [override]
        | None = None,
        **kwargs: str,
    ) -> TestClient:
        return cls.make_blocking_test_client(
            connection_class=connection_class,
            **kwargs,
        )


_lock_cnt = 0


def gen_lock_key() -> int:
    global _lock_cnt  # noqa: PLW0603
    _lock_cnt += 1
    return os.getpid() * 1000 + _lock_cnt


def must_fail(f: Callable[_P, _R]) -> Callable[_P, _R]:
    return unittest.expectedFailure(f)


def to_be_fixed(f: Callable[_P, _R]) -> Callable[_P, _R]:
    return unittest.expectedFailure(f)


def xfail(f: Callable[_P, _R]) -> Callable[_P, _R]:
    return unittest.expectedFailure(f)


if os.environ.get("USE_UVLOOP"):
    import uvloop

    uvloop.install()
