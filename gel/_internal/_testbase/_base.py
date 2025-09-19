# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    Any,
    ClassVar,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    TYPE_CHECKING,
    runtime_checkable,
    overload,
)
from typing_extensions import TypeAliasType

import asyncio
import contextlib
import functools
import hashlib
import inspect
import logging
import os
import pathlib
import re
import sys
import unittest

import gel
from gel import asyncio_client
from gel import blocking_client

from gel._internal import _edgeql
from gel._internal import _version
from gel._internal._testbase import _server


if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Iterator,
        Callable,
        Mapping,
        Sequence,
    )


_P = ParamSpec("_P")
_R = TypeVar("_R", covariant=True)

_Client_T = TypeVar("_Client_T", bound="TestClient | TestAsyncIOClient")


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


def get_test_source_root() -> pathlib.Path | None:
    source_root: pathlib.Path | None = None
    test_root_env = os.environ.get("GEL_PYTHON_TEST_ROOT")
    if test_root_env is not None:
        source_root = pathlib.Path(test_root_env)
    else:
        source_root = _version.get_project_source_root()

    return source_root


Instance = TypeAliasType("Instance", _server.BaseInstance)
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
            return self.loop.run_until_complete(meth(self, *args, **kwargs))

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
        if not ns.get("BASE_TEST_CLASS") and hasattr(
            cls, "get_base_database_name"
        ):
            dbname = cls.get_base_database_name()  # pyright: ignore[reportAttributeAccessIssue]

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


def _truthy(val: str) -> bool:
    return val in {
        "on",
        "yes",
        "true",
        "1",
        "enabled",
        "enable",
    }


class UI(Protocol):
    def text(self, msg: str) -> None: ...
    def info(self, msg: str) -> None: ...
    def warning(self, msg: str) -> None: ...
    def error(self, msg: str) -> None: ...


@runtime_checkable
class Fixture(Protocol):
    def __get__(
        self,
        instance: Any | None,
        owner: type[Any] | None = None,
        /,
    ) -> Any: ...
    def set_options(self, options: Mapping[str, str]) -> None: ...
    def get_shared_data(self) -> object: ...
    def set_shared_data(self, data: object) -> None: ...
    async def set_up(self, ui: UI) -> None: ...
    async def tear_down(self, ui: UI) -> None: ...
    async def post_session_set_up(
        self, cases: Sequence[type[Any]], *, ui: UI
    ) -> None: ...


class LoggingUI:
    def __init__(self) -> None:
        self._log = logging.getLogger(__name__)

    def text(self, msg: str) -> None:
        self._log.debug(msg)

    def info(self, msg: str) -> None:
        self._log.info(msg)

    def warning(self, msg: str) -> None:
        self._log.warning(msg)

    def error(self, msg: str) -> None:
        self._log.error(msg)


logui = LoggingUI()


class InstanceFixture:
    def __init__(self) -> None:
        self._instance: Instance | None = None
        self._instance_error: Exception | None = None
        self._cache_enabled = False
        self._server_addr: dict[str, Any] | None = None
        self._server_version: _server.Version | None = None
        self._data_dir: str | None = None
        self._backend_dsn: str | None = None

    @property
    def is_set_up(self) -> bool:
        return self._instance is not None

    def __get__(
        self,
        instance: Any | None,
        owner: type[Any] | None = None,
        /,
    ) -> Instance:
        if self._instance is None:
            raise RuntimeError("instance fixture has not been initialized yet")

        return self._instance

    def set_options(self, options: Mapping[str, str]) -> None:
        self._cache_enabled = _truthy(options.get("test-db-cache", ""))
        self._data_dir = options.get("data-dir")
        self._backend_dsn = options.get("backend-dsn")

    def get_server_version(self) -> _server.Version:
        if self._server_version is None:
            if self._instance is None:
                raise RuntimeError(
                    "instance fixture has not been initialized yet"
                )

            self._server_version = self._instance.get_server_version()

        return self._server_version

    def get_shared_data(self) -> object:
        if self._instance is None:
            raise RuntimeError("instance fixture has not been initialized yet")
        return {
            "SERVER_ADDR": self._instance.get_connect_args(),
            "SERVER_VERSION": str(self._instance.get_server_version()),
        }

    def set_shared_data(self, data: object) -> None:
        if not data:
            return

        if not isinstance(data, dict):
            raise RuntimeError(
                f"expected {self.__class__.__name__} shared data to be a dict"
            )

        server_addr = data.get("SERVER_ADDR")
        if server_addr:
            if not isinstance(server_addr, dict):
                raise RuntimeError(
                    f"expected {self.__class__.__name__} "
                    "SERVER_ADDR to be a dict"
                )

            self._server_addr = server_addr

            server_ver = data.get("SERVER_VERSION")
            if server_ver:
                self._server_version = _server.Version.parse(server_ver)

    async def set_up(self, ui: UI) -> None:
        if self._instance is not None:
            return

        server_addr = self._server_addr
        if server_addr is None:
            server_addr = {
                "host": "localhost",
                "port": 5656,
                "tls_ca_file": "/home/dnwpark/work/dev-3.12/edgedb/tmp/devdatadir/edbtlscert.pem",
            }

        if server_addr is not None:
            await self._set_up_running_instance(
                server_addr,
                self._server_version,
            )
        else:
            await self._set_up_new_instance(ui)

    async def post_session_set_up(
        self, cases: Sequence[type[Any]], *, ui: UI
    ) -> None:
        if (
            self._cache_enabled
            and isinstance(self._instance, _server.ManagedInstance)
            and (cache := self._get_cache_path(self._instance)) is not None
            and any(
                # No attribute means it's not a database case
                not getattr(case, "db_cache_used", True)
                for case in cases
            )
        ):
            ui.info(f"\n -> Writing DB cache to {cache} ...")
            try:
                self._instance.backup(cache)
            except Exception as e:
                ui.warning(f"\ncould not backup instance: {e}")

    async def tear_down(self, ui: UI) -> None:
        if self._instance is not None:
            self._instance.stop()
            self._instance = None

    async def _set_up_running_instance(
        self,
        server_addr: dict[str, Any],
        server_version: _server.Version | None,
    ) -> None:
        self._instance = _server.RunningInstance(
            server_version=server_version,
            conn_args=server_addr,
        )
        await self._instance.start()

    async def _set_up_new_instance(self, ui: UI) -> None:
        data_dir = self._data_dir
        if data_dir is None:
            data_dir = os.environ.get("GEL_TEST_DATA_DIR")

        backend_dsn = self._backend_dsn
        if backend_dsn is None:
            backend_dsn = os.environ.get("GEL_TEST_BACKEND_DSN")

        self._instance = _server.TestInstance(
            cleanup_atexit=False,
            data_dir=pathlib.Path(data_dir) if data_dir is not None else None,
            backend_dsn=backend_dsn,
        )

        ui.info("\n -> Bootstrapping Gel instance...")

        if (
            self._cache_enabled
            and (cache := self._get_cache_path(self._instance)) is not None
            and cache.is_file()
        ):
            ui.text(f" (using DB cache from {cache}) ")
            self._instance.set_data_tarball(cache)

        await self._instance.start()

    def _get_cache_path(
        self, instance: _server.ManagedInstance
    ) -> pathlib.Path | None:
        cache_info = self.get_cache_info(instance)
        if cache_info is not None:
            fname = f"{cache_info[1]}-test-dbs.tar"
            return cache_info[0] / fname
        else:
            return None

    @classmethod
    def get_cache_info(
        cls, instance: _server.ManagedInstance
    ) -> tuple[pathlib.Path, str] | None:
        source_dir = get_test_source_root()
        if source_dir is None:
            # Not a development checkout
            return None

        server_ver = instance.get_server_version().to_str(include_local=False)
        cache_key = f"{server_ver}"

        return source_dir / ".cache", cache_key


instance_fixture = InstanceFixture()


class ServerVersion:
    def __get__(
        self,
        instance: InstanceTestCase | None,
        owner: type[InstanceTestCase] | None = None,
        /,
    ) -> _server.Version:
        if instance is not None:
            return instance.instance.get_server_version()
        elif owner is not None:
            return owner.instance.get_server_version()
        else:
            raise AssertionError(
                "Instance.__get__ unexpectedly called with both instance "
                "and owner set to None"
            )


class InstanceTestCase(TestCase):
    BASE_TEST_CLASS = True

    TRANSACTION_ISOLATION = False
    ISOLATED_TEST_BRANCHES = False

    DEFAULT_TRANSACTION_ISOLATION: gel.IsolationLevel | None = None

    # By default, tests from the same testsuite may be ran in parallel in
    # several test worker processes.  However, certain cases might exhibit
    # pathological locking behavior, or are parallel-unsafe altogether, in
    # which case PARALLELISM_GRANULARITY must be set to 'database', 'suite',
    # or 'system'.  The 'database' granularity signals that no two runners
    # may execute tests on the same database in parallel, although the tests
    # may still run on copies of the test database.  The 'suite' granularity
    # means that only one test worker is allowed to execute tests from this
    # suite.  Finally, the 'system' granularity means that the test suite
    # is not parallelizable at all and must run sequentially with respect
    # to *all other* suites with 'system' granularity.
    PARALLELISM_GRANULARITY = "default"

    instance: ClassVar[InstanceFixture] = instance_fixture
    server_version: ClassVar[ServerVersion] = ServerVersion()

    db_cache_enabled: ClassVar[bool] = False
    db_cache_used: ClassVar[bool] = False

    shared_data: ClassVar[dict[str, object]] = {}
    class_cleanup_once_callbacks: ClassVar[list[Callable[[], object]]] = []

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        async def setup() -> None:
            for attrname in dir(cls):
                attr = inspect.getattr_static(cls, attrname, None)
                if isinstance(attr, Fixture):
                    await attr.set_up(logui)

            if cls.responsible_for_setup():
                await cls.set_up_class_once(logui)

        asyncio.get_event_loop().run_until_complete(setup())

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls.responsible_for_setup():
                new_loop = None
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    new_loop = loop = asyncio.new_event_loop()

                try:
                    loop.run_until_complete(cls.tear_down_class_once(logui))
                finally:
                    if new_loop is not None:
                        new_loop.close()
        finally:
            super().tearDownClass()

    @classmethod
    def get_connect_args(
        cls,
        instance: _server.BaseInstance | None = None,
        /,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if instance is None:
            instance = cls.instance
        return instance.get_connect_args() | kwargs

    @classmethod
    def set_options(cls, options: Mapping[str, str]) -> None:
        cls.db_cache_enabled = _truthy(options.get("test-db-cache", ""))

    @classmethod
    def responsible_for_setup(cls) -> bool:
        """True if TestCase is responsible for fixture setup."""
        resp = os.environ.get("GEL_TEST_SETUP_RESPONSIBLE", "testcase")
        return resp == "testcase"

    @classmethod
    def get_parallelism_granularity(cls) -> str:
        return cls.PARALLELISM_GRANULARITY

    @classmethod
    async def set_up_class_once(cls, ui: UI) -> None:
        pass

    @classmethod
    def add_class_cleanup_once(cls, callback: Callable[[], object]) -> None:
        cls.class_cleanup_once_callbacks.append(callback)

    @classmethod
    async def tear_down_class_once(cls, ui: UI) -> None:
        for callback in cls.class_cleanup_once_callbacks:
            callback()

    @classmethod
    def get_shared_data(cls) -> Mapping[str, object]:
        return cls.shared_data

    @classmethod
    def update_shared_data(cls, **data: object) -> None:
        cls.shared_data.update(data)

    @classmethod
    def get_default_transaction_isolation(
        cls,
        instance: _server.BaseInstance | None = None,
    ) -> gel.IsolationLevel:
        if cls.DEFAULT_TRANSACTION_ISOLATION is None:
            if cls.server_supports_repeatable_read(instance):
                return gel.IsolationLevel.PreferRepeatableRead
            else:
                return gel.IsolationLevel.Serializable
        else:
            return cls.DEFAULT_TRANSACTION_ISOLATION

    @classmethod
    def get_default_retry_options(cls) -> gel.RetryOptions:
        return gel.RetryOptions(attempts=10)

    @classmethod
    def uses_database_copies(cls) -> bool:
        return (
            bool(os.environ.get("GEL_TEST_PARALLEL"))
            and cls.get_parallelism_granularity() == "database"
        )

    @classmethod
    def server_supports_repeatable_read(
        cls,
        instance: _server.BaseInstance | None = None,
    ) -> bool:
        if instance is None:
            instance = cls.instance
        return instance.get_server_version() >= (6, 5)


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

    # The windows test runners were timing out when it was the default
    # of 10s.
    DEFAULT_CONNECT_TIMEOUT = 30

    CLIENT_TYPE: ClassVar[type[TestClient | TestAsyncIOClient] | None]
    client: ClassVar[TestClient | TestAsyncIOClient]

    @classmethod
    async def set_up_class_once(cls, ui: UI) -> None:
        await super().set_up_class_once(ui)
        dbname = cls.get_base_database_name()

        try:
            await cls._create_empty_branch(dbname)
        except gel.DuplicateDatabaseDefinitionError:
            if cls.db_cache_enabled:
                cls.db_cache_used = True
            else:
                raise
        else:
            if script := cls.get_setup_script():
                async with cls.async_test_client(database=dbname) as client:
                    await client.execute(script)

    @classmethod
    async def tear_down_class_once(cls, ui: UI) -> None:
        try:
            dbname = cls.get_base_database_name()

            if cls.TEARDOWN and (script := cls.TEARDOWN.strip()):
                async with cls.async_test_client(database=dbname) as client:
                    await client.execute(script)

            await cls._drop_branch(dbname)
        finally:
            await super().tear_down_class_once(ui)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.loop.run_until_complete(cls.setup_and_connect())

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.loop.run_until_complete(cls.teardown_and_disconnect())
        finally:
            super().tearDownClass()

    def setUp(self) -> None:
        if self.ISOLATED_TEST_BRANCHES:
            cls = type(self)
            testdb = cls.loop.run_until_complete(self.setup_branch_copy())
            client = cls.make_test_client(
                database=testdb, client_class=self.CLIENT_TYPE
            )._with_debug(
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
                    self.loop.run_until_complete(self.drop_branch_copy())

                super().tearDown()

    @classmethod
    def make_test_client(
        cls,
        *,
        client_class: type[TestClient | TestAsyncIOClient] | None = None,
        connection_class: type[
            asyncio_client.AsyncIOConnection
            | blocking_client.BlockingIOConnection
        ]
        | None = None,
        **kwargs: Any,
    ) -> TestAsyncIOClient | TestClient:
        raise NotImplementedError

    @classmethod
    async def close_client(
        cls,
        client: gel.AsyncIOClient | gel.Client,
    ) -> None:
        try:
            if isinstance(client, gel.AsyncIOClient):
                await client.aclose()
            else:
                client.close()
        except (OSError, TimeoutError):
            client.terminate()

    @classmethod
    def configure_client(
        cls,
        client: _Client_T,
        *,
        instance: _server.BaseInstance | None = None,
    ) -> _Client_T:
        txiso = cls.get_default_transaction_isolation(instance)
        iso_opts = gel.TransactionOptions(isolation=txiso)
        client = client.with_transaction_options(iso_opts)  # type: ignore [assignment]
        retry_opts = cls.get_default_retry_options()
        client = client.with_retry_options(retry_opts)  # type: ignore [assignment]
        return client

    @classmethod
    def make_blocking_test_client(
        cls,
        *,
        instance: _server.BaseInstance,
        client_class: type[TestClient] | None = None,
        connection_class: type[blocking_client.BlockingIOConnection]
        | None = None,
        **kwargs: Any,
    ) -> TestClient:
        if client_class is None:
            client_class = TestClient
        if connection_class is None:
            connection_class = blocking_client.BlockingIOConnection
        client = instance.create_blocking_client(
            client_class=client_class,
            connection_class=connection_class,
            **cls.get_connect_args(instance, **kwargs),
        )
        return cls.configure_client(client, instance=instance)

    @classmethod
    @contextlib.contextmanager
    def blocking_test_client(
        cls,
        *,
        instance: _server.BaseInstance | None = None,
        connection_class: type[blocking_client.BlockingIOConnection]
        | None = None,
        timeout: float = DEFAULT_CONNECT_TIMEOUT,
        **kwargs: Any,
    ) -> Iterator[TestClient]:
        if instance is None:
            instance = cls.instance
        client = cls.make_blocking_test_client(
            instance=instance,
            connection_class=connection_class,
            timeout=timeout,
            **kwargs,
        )
        client.ensure_connected()
        try:
            yield client
        finally:
            client.close()

    @classmethod
    def make_async_test_client(
        cls,
        *,
        instance: _server.BaseInstance,
        client_class: type[TestAsyncIOClient] | None = None,
        connection_class: type[asyncio_client.AsyncIOConnection] | None = None,
        **kwargs: Any,
    ) -> TestAsyncIOClient:
        if client_class is None:
            client_class = TestAsyncIOClient
        if connection_class is None:
            connection_class = asyncio_client.AsyncIOConnection
        client = instance.create_async_client(
            client_class=client_class,
            connection_class=connection_class,
            **cls.get_connect_args(instance, **kwargs),
        )
        return cls.configure_client(client, instance=instance)

    @classmethod
    @contextlib.asynccontextmanager
    async def async_test_client(
        cls,
        *,
        instance: _server.BaseInstance | None = None,
        connection_class: type[asyncio_client.AsyncIOConnection] | None = None,
        timeout: float = DEFAULT_CONNECT_TIMEOUT,
        **kwargs: Any,
    ) -> AsyncIterator[TestAsyncIOClient]:
        if instance is None:
            instance = cls.instance
        client = cls.make_async_test_client(
            instance=instance,
            connection_class=connection_class,
            timeout=timeout,
            **kwargs,
        )
        await client.ensure_connected()
        try:
            yield client
        finally:
            await client.aclose()

    def _get_method_branch_copy_name(self) -> str:
        testdb = f"{os.getpid()}_{type(self).__name__}_{self._testMethodName}"
        testdb = "_" + hashlib.sha1(testdb.encode()).hexdigest()  # noqa: S324
        return testdb

    @classmethod
    async def _create_data_branch(cls, name: str, base: str) -> None:
        async with cls.async_test_client() as admin_client:
            await admin_client.with_config(__internal_testmode=True).query(
                f"create template branch {_edgeql.quote_ident(name)}"
                f" from {_edgeql.quote_ident(base)}"
            )

    @classmethod
    async def _create_empty_branch(cls, name: str) -> None:
        async with cls.async_test_client() as admin_client:
            await admin_client.query(
                f"create empty branch {_edgeql.quote_ident(name)}"
            )

    @classmethod
    async def _drop_branch(cls, name: str) -> None:
        async with cls.async_test_client() as admin_client:
            stmt = f"drop branch {_edgeql.quote_ident(name)}"
            await admin_client.query(stmt)

    async def setup_branch_copy(self) -> str:
        cls = type(self)
        root = cls.get_database_name()
        testdb = self._get_method_branch_copy_name()
        await cls._create_data_branch(testdb, root)
        return testdb

    async def drop_branch_copy(self) -> None:
        await self._drop_branch(self._get_method_branch_copy_name())

    @classmethod
    async def setup_and_connect(cls) -> None:
        dbname = cls.get_database_name()

        if cls.uses_database_copies():
            if cls.get_setup_script():
                base_db_name = cls.get_base_database_name()
                await cls._create_data_branch(dbname, base_db_name)
            else:
                await cls._create_empty_branch(dbname)

        if not cls.ISOLATED_TEST_BRANCHES:
            cls.client = cls.make_test_client(
                database=dbname, client_class=cls.CLIENT_TYPE
            )
            if isinstance(cls.client, gel.AsyncIOClient):
                await cls.client.ensure_connected()
            else:
                cls.client.ensure_connected()

    @classmethod
    async def teardown_and_disconnect(cls) -> None:
        dbname = cls.get_database_name()

        if not cls.ISOLATED_TEST_BRANCHES:
            await cls.close_client(cls.client)
            del cls.client

        if cls.uses_database_copies():
            await cls._drop_branch(dbname)

    @classmethod
    def get_base_database_name(cls) -> str:
        if cls.__name__.startswith("Test"):
            dbname = cls.__name__[len("Test") :]
        else:
            dbname = cls.__name__

        return dbname.lower()

    @classmethod
    def get_database_name(cls) -> str:
        base_dbname = cls.get_base_database_name()
        if cls.uses_database_copies():
            return f"{base_dbname}_{os.getpid()}"
        else:
            return base_dbname

    @classmethod
    def get_combined_schemas(cls) -> str:
        schema_texts: list[str] = []

        # Look at all SCHEMA entries and potentially create multiple
        # modules, but always create the test module, if not `default`.
        if cls.DEFAULT_MODULE != "default":
            schema_texts.append(f"\nmodule {cls.DEFAULT_MODULE} {{}}")

        schema_texts.extend(
            schema_text
            for name in cls.__dict__
            if (schema_text := cls.get_schema_text(name))
        )

        return "\n\n".join(st for st in schema_texts)

    @classmethod
    def get_schema_field_name(cls, field: str) -> str | None:
        if m := re.match(r"^SCHEMA(?:_(\w+))?", field):
            return m.group(1) or ""

        return None

    @classmethod
    def get_schema_text(cls, field: str) -> str | None:
        schema_name = cls.get_schema_field_name(field)
        if schema_name is None:
            return None

        val = cls.__dict__.get(field)
        if val is None:
            return None
        assert isinstance(val, str)

        module_name = (
            (schema_name or cls.DEFAULT_MODULE).lower().replace("_", "::")
        )

        if os.path.exists(val):
            module = pathlib.Path(val).read_text(encoding="utf8")
        else:
            module = val

        if f"module {module_name}" not in module:
            module = f"\nmodule {module_name} {{ {module} }}"

        return module

    @classmethod
    def get_setup_script(cls) -> str:
        script = ""

        # Don't wrap the script into a transaction here, so that
        # potentially it's easier to stitch multiple such scripts
        # together in a fashion similar to what `edb inittestdb` does.
        script += f"\nSTART MIGRATION TO {{ {cls.get_combined_schemas()} }};"
        script += "\nPOPULATE MIGRATION; \nCOMMIT MIGRATION;"

        if cls.SETUP:
            if not isinstance(cls.SETUP, (list, tuple)):
                scripts = [cls.SETUP]
            else:
                scripts = [*cls.SETUP]

            for scr in scripts:
                if "\n" not in scr and os.path.exists(scr):
                    with open(scr) as f:  # noqa: PLW1514, FURB101
                        setup = f.read()
                else:
                    setup = scr

                script += "\n" + setup

        return script.strip(" \n")


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

    @classmethod
    def make_test_client(  # pyright: ignore [reportIncompatibleMethodOverride]
        cls,
        *,
        client_class: type[TestAsyncIOClient] | None = None,
        connection_class: type[asyncio_client.AsyncIOConnection] | None = None,  # type: ignore [override]
        **kwargs: str,
    ) -> TestAsyncIOClient:
        return cls.make_async_test_client(
            instance=cls.instance,
            client_class=client_class,
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

    @classmethod
    def adapt_call(cls, coro: Any) -> Any:
        return coro

    @classmethod
    def make_test_client(  # pyright: ignore [reportIncompatibleMethodOverride]
        cls,
        *,
        client_class: type[TestClient] | None = None,
        connection_class: type[blocking_client.BlockingIOConnection]  # type: ignore [override]
        | None = None,
        **kwargs: str,
    ) -> TestClient:
        return cls.make_blocking_test_client(
            instance=cls.instance,
            client_class=client_class,
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


def xfail_unimplemented(
    reason: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def t(f: Callable[_P, _R]) -> Callable[_P, _R]:
        return unittest.expectedFailure(f)

    return t


@overload
def xfail(func: Callable[_P, _R], /) -> Callable[_P, _R]: ...


@overload
def xfail(
    reason: str, /
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]: ...


def xfail(
    func_or_reason: str | Callable[_P, _R], /
) -> Callable[_P, _R] | Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    if callable(func_or_reason):
        return unittest.expectedFailure(func_or_reason)
    else:

        def t(f: Callable[_P, _R]) -> Callable[_P, _R]:
            return unittest.expectedFailure(f)

        return t


if os.environ.get("USE_UVLOOP") and sys.platform != "win32":
    import uvloop

    uvloop.install()
