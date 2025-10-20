# ruff: noqa: N802

from __future__ import annotations
from typing import (
    Any,
    ClassVar,
    Concatenate,
    Literal,
    ParamSpec,
    TypeVar,
    TYPE_CHECKING,
)
from collections.abc import Awaitable, Collection
from typing_extensions import Self

import argparse
import base64
import contextlib
import functools
import gc
import importlib
import inspect
import json
import linecache
import os
import pathlib
import pickle  # noqa: S403
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
import warnings


from gel._internal import _dirhash
from gel._internal import _import_extras
from gel._internal._codegen._models import PydanticModelsGenerator

from ._base import (
    AsyncQueryTestCase,
    BranchTestCase,
    Instance,
    InstanceFixture,
    SyncQueryTestCase,
    TestCaseMeta,
    UI,
    get_test_source_root,
    instance_fixture,
    must_fail,
    logui,
    to_be_fixed,
    xfail,
    xfail_unimplemented,
)

__all__ = (
    "TNAME",
    "TNAME_PY",
    "AsyncModelTestCase",
    "ModelTestCase",
    "must_fail",
    "pop_ids",
    "pop_ids_json",
    "repickle",
    "skip_typecheck",
    "to_be_fixed",
    "typecheck",
    "xfail",
    "xfail_unimplemented",
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    import pydantic
    from gel._internal._qbmodel._pydantic._models import GelModel


_unset = object()

_T = TypeVar("_T")
_P = ParamSpec("_P")
_R = TypeVar("_R", covariant=True)
_ModelTestCase_T = TypeVar("_ModelTestCase_T", bound="BaseModelTestCase")
TNAME = "__tname__"
TNAME_PY = "tname__"


def _get_test_class_dir(cls: type[unittest.TestCase]) -> pathlib.Path:
    test_file = sys.modules[cls.__module__].__file__
    if test_file is None:
        raise RuntimeError("test source for cls is not a file")

    return pathlib.Path(test_file).parent


def _clear_model_imports() -> None:
    for mod_name in tuple(sys.modules.keys()):
        if mod_name.startswith(("tests.models.", "models.")) or mod_name in {
            "tests.models",
            "models",
        }:
            del sys.modules[mod_name]

    importlib.invalidate_caches()
    linecache.clearcache()
    gc.collect()


@functools.cache
def _get_impl_hash_key() -> str:
    packages = [
        "gel._internal._codegen",
        "gel._internal._reflection",
    ]

    paths: list[str] = []
    for package in packages:
        paths.extend(importlib.import_module(package).__path__)

    hashdigest = _dirhash.dirhash((pkg, ".py") for pkg in paths)
    return base64.b32encode(hashdigest[:8]).decode("ascii").rstrip("=")


def generate(
    *,
    instance: Instance,
    output_dir: pathlib.Path,
    dbname: str | None = None,
    std_only: bool = False,
    source_std_from: pathlib.Path | None = None,
    source_std_method: Literal["copy", "reexport"] = "copy",
    introspection_cache_dir: pathlib.Path | None = None,
    force_reflection: bool = False,
) -> None:
    with instance.client(database=dbname) as client:
        gen = PydanticModelsGenerator(
            argparse.Namespace(
                no_cache=force_reflection,
                quiet=True,
                output=output_dir,
                std_only=std_only,
                source_std_from=source_std_from,
                source_std_method=source_std_method,
            ),
            cache_dir=introspection_cache_dir,
            extra_cache_key=_get_impl_hash_key(),
            project_dir=output_dir.parent,
            client=client,
            interactive=False,
        )

        try:
            gen.run()
        except Exception as e:
            raise RuntimeError(
                f"error running model codegen, its stderr:\n"
                f"{gen.get_error_output()}"
            ) from e


class StdReflectionFixture:
    _DATA_KEY = "MODELS_GENERATED_STD_PATH"

    instance: ClassVar[InstanceFixture] = instance_fixture

    def __init__(self) -> None:
        self._output_path: pathlib.Path | None = None
        self._cache_dir: pathlib.Path | None = None
        self._temp_dir: tempfile.TemporaryDirectory[str] | None = None

    @property
    def is_set_up(self) -> bool:
        return self._output_path is not None

    @property
    def output_path(self) -> pathlib.Path:
        if self._output_path is None:
            raise RuntimeError(
                f"{self.__class__.__name__} has not been configured"
            )
        return self._output_path

    @property
    def temp_dir(self) -> pathlib.Path | None:
        if self._output_path is None:
            raise RuntimeError(
                f"{self.__class__.__name__} has not been configured"
            )
        if self._temp_dir is not None:
            return pathlib.Path(self._temp_dir.name)
        else:
            return None

    @property
    def cache_dir(self) -> pathlib.Path | None:
        if self._output_path is None:
            raise RuntimeError(
                f"{self.__class__.__name__} has not been configured"
            )
        return self._cache_dir

    def __get__(
        self,
        instance: Any | None,
        owner: type[Any] | None = None,
        /,
    ) -> Self:
        if self._output_path is None:
            raise RuntimeError(
                f"{self.__class__.__name__} has not been configured"
            )
        return self

    def set_options(self, options: Mapping[str, str]) -> None:
        pass

    def get_shared_data(self) -> object:
        return {
            self._DATA_KEY: str(self._output_path),
        }

    def set_shared_data(self, data: object) -> None:
        if not data:
            return

        if not isinstance(data, dict):
            raise RuntimeError(
                f"expected {self.__class__.__name__} shared data to be a dict"
            )

        path = data.get(self._DATA_KEY)
        if path:
            self._output_path = pathlib.Path(path)

    async def set_up(self, ui: UI) -> None:
        if self.is_set_up:
            return

        source_root = get_test_source_root()
        if source_root is not None:
            cache_dir = source_root / ".cache"
            self._output_path = (
                source_root / "tests" / "models" / "__sharedstd__"
            )
        else:
            cache_dir = None
            self._temp_dir = tempfile.TemporaryDirectory(
                prefix="gel-test-std-models-"
            )
            self._output_path = (
                pathlib.Path(self._temp_dir.name) / ".shared.std"
            )

        self._cache_dir = cache_dir

        ui.info("\n -> Generating stdlib models... ")
        if cache_dir is not None:
            ui.text(
                f"(to {self._output_path}, considering cached introspection) "
            )
        else:
            ui.text(f"(to {self._output_path}) ")
        generate(
            instance=self.instance,
            output_dir=self._output_path,
            introspection_cache_dir=cache_dir,
            std_only=True,
        )
        ui.info("OK\n")

    async def post_session_set_up(
        self, cases: Sequence[type[Any]], *, ui: UI
    ) -> None:
        pass

    async def tear_down(self, ui: UI) -> None:
        pass


class BaseModelTestCase(BranchTestCase):
    DEFAULT_MODULE = "default"

    orm_debug: ClassVar[bool]
    gen: ClassVar[Any]

    std: ClassVar[StdReflectionFixture] = StdReflectionFixture()
    sys_path_ctx: ClassVar[contextlib.ExitStack | None] = None

    _mypy_errors: ClassVar[dict[str, str]]
    _pyright_errors: ClassVar[dict[str, str]]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if not getattr(cls, "_typecheked", False):
            # No typecheck wrapping, inject sys-path
            test_dir = _get_test_class_dir(cls)
            cls.sys_path_ctx = contextlib.ExitStack()
            cls.sys_path_ctx.enter_context(_import_extras.sys_path(test_dir))

        try:
            _clear_model_imports()
        except Exception as e:
            logui.warning(f"could not clear models from imports: {e}")

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls.sys_path_ctx is not None:
                cls.sys_path_ctx.close()
                cls.sys_path_ctx = None

            _clear_model_imports()
        finally:
            super().tearDownClass()

    @classmethod
    def _model_info(cls) -> tuple[bool, str]:
        assert isinstance(cls.SCHEMA, str)
        if os.path.exists(cls.SCHEMA):
            return True, pathlib.Path(cls.SCHEMA).stem
        else:
            return False, cls.__name__

    @classmethod
    def get_model_package(cls) -> str:
        return cls._model_info()[1]

    @classmethod
    async def set_up_class_once(cls, ui: UI) -> None:
        await super().set_up_class_once(ui)

        base_dbname = cls.get_base_database_name()
        test_dir = _get_test_class_dir(cls)
        models_dir = test_dir / "models"
        models_dir.mkdir(exist_ok=True)
        # Make sure the base "models" directory has an __init__.py
        (models_dir / "__init__.py").write_text(
            textwrap.dedent('''\
            # Prevent unittest from recursing into the models
            def load_tests(*args, **kwargs):
                return None
        ''')
        )
        # and py.typed
        (models_dir / "py.typed").touch()

        generate(
            instance=cls.instance,
            dbname=base_dbname,
            output_dir=models_dir / cls.get_model_package(),
            source_std_from=cls.std.output_path,
            source_std_method="reexport",
            introspection_cache_dir=cls.std.cache_dir,
            force_reflection=True,
        )

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
    def assertWarns(  # type: ignore [override]
        self,
        *,
        msg_part: str,
        exp_category: type[Warning] = UserWarning,
    ) -> Iterator[str]:
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
    def assertNotWarns(self) -> Iterator[None]:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            yield

        if w:
            captured_warnings = [
                f"{wm.category.__name__}: {wm.message}" for wm in w
            ]
            self.fail(
                "Unexpected warning(s) captured:\n"
                + "\n".join(captured_warnings)
            )

    def assertPydanticChangedFields(
        self,
        model: pydantic.BaseModel,
        expected: set[str],
        *,
        expected_gel: set[str] | None = None,
    ) -> None:
        # We don't have a default for GelModel.id so we have to pass it
        # manually and then remove it right after the model is built.
        # That's why 'id' is always in changed fields. It doesn't matter
        # though. The main consumer of __gel_get_changed_fields__ is
        # `save()`, which just ignores `id` and relies on `__gel_new__`.
        self.assertEqual(model.__pydantic_fields_set__ - {"id"}, expected)
        if hasattr(model, "__gel_get_changed_fields__"):
            self.assertEqual(
                model.__gel_get_changed_fields__() - {"id"},  # pyright: ignore[reportAttributeAccessIssue]
                expected if expected_gel is None else expected_gel,
            )

    def assertPydanticSerializes(
        self,
        model: pydantic.BaseModel,
        expected: Any = _unset,
        *,
        test_pickle: bool = True,
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

        dumped = model.model_dump(
            context=context,
        )
        new = type(model).model_validate(dumped)
        self.assertEqual(
            new.model_dump(context=new_context),
            model.model_dump(context=context),
        )
        if test_pickle:
            repickle(new)

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

    def _assertObjectsWithFields(
        self,
        models: Collection[GelModel],
        identifying_field: str,
        expected_obj_fields: list[tuple[type[GelModel], dict[str, Any]]],
    ) -> None:
        """Test that models match the expected object fields.
        Pairs models with their expected fields using the identifying field.
        """
        self.assertEqual(len(models), len(expected_obj_fields))

        # Get models per identifier
        for model in models:
            self._assertHasFields(model, {identifying_field})

        object_by_identifier = {
            expected_fields[identifying_field]: next(
                iter(
                    m
                    for m in models
                    if getattr(m, identifying_field)
                    == expected_fields[identifying_field]
                ),
                None,
            )
            for _, expected_fields in expected_obj_fields
        }

        # Check that models match obj_fields one to one
        for identifier, obj in object_by_identifier.items():
            self.assertIsNotNone(
                obj, f"No model with identifier '{identifier}'"
            )
        self.assertEqual(
            len(object_by_identifier),
            len(expected_obj_fields),
            "Duplicate identifier'",
        )

        # Check each model
        for expected_type, expected_fields in expected_obj_fields:
            identifier = expected_fields[identifying_field]
            obj = object_by_identifier[identifier]
            assert obj is not None
            self.assertIsInstance(obj, expected_type)
            self._assertHasFields(obj, expected_fields)

    def _assertHasFields(
        self,
        model: GelModel,
        expected_fields: dict[str, Any] | set[str],
    ) -> None:
        for field_name in expected_fields:
            self.assertTrue(
                field_name in model.__pydantic_fields_set__,
                f"Model is missing field '{field_name}'",
            )

            if isinstance(expected_fields, dict):
                expected = expected_fields[field_name]
                actual = getattr(model, field_name)
                self.assertEqual(
                    expected,
                    actual,
                    f"Field '{field_name}' value ({actual}) different from "
                    f"expected ({expected})",
                )

    def _assertNotHasFields(
        self, model: GelModel, expected_fields: set[str]
    ) -> None:
        for field_name in expected_fields:
            self.assertTrue(
                field_name not in model.__pydantic_fields_set__,
                f"Model has unexpected field '{field_name}'",
            )


class ModelTestCase(SyncQueryTestCase, BaseModelTestCase):  # pyright: ignore[reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
    pass


class AsyncModelTestCase(AsyncQueryTestCase, BaseModelTestCase):  # pyright: ignore[reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
    pass


def _typecheck(
    cls: type[_ModelTestCase_T],  # XXX
    func: Callable[Concatenate[_ModelTestCase_T, _P], _R],
    *,
    xfail: bool = False,
) -> Callable[Concatenate[_ModelTestCase_T, _P], _R]:
    wrapped: Callable[Concatenate[_ModelTestCase_T, _P], _R] = inspect.unwrap(
        func
    )
    is_async = inspect.iscoroutinefunction(wrapped)

    # def run(self: ModelTestCase) -> Any:
    def run(
        self: _ModelTestCase_T, *args: _P.args, **kwargs: _P.kwargs
    ) -> _R | Awaitable[_R]:
        # We already ran the typechecker on everything, so now inspect
        # the results for this test.

        mypy_output = cls._mypy_errors.get(func.__name__, '')
        mypy_error = 'error:' in mypy_output

        pyright_output = cls._pyright_errors.get(func.__name__, '')
        pyright_error = '- error:' in pyright_output

        if mypy_error or pyright_error:
            source_code = _get_file_code(func)
            lines = source_code.split("\n")
            pad_width = max(2, len(str(len(lines))))
            source_code_numbered = "\n".join(
                f"{i + 1:0{pad_width}d}: {line}"
                for i, line in enumerate(lines)
            )

            if mypy_error:
                raise RuntimeError(
                    f"mypy check failed for {func.__name__} "
                    f"\n\ntest code:\n{source_code_numbered}"
                    f"\n\nmypy stdout:\n{mypy_output}"
                )

            if pyright_error:
                raise RuntimeError(
                    f"pyright check failed for {func.__name__} "
                    f"\n\ntest code:\n{source_code_numbered}"
                    f"\n\npyright stdout:\n{pyright_output}"
                )

        types = [
            m.group("name")
            for line in mypy_output.split("\n")
            if (m := re.match(r'.*Revealed type is "(?P<name>[^"]+)".*', line))
        ]

        def reveal_type(
            _: Any,
            *,
            ncalls: list[int] = [0],  # noqa: B006
            types: list[str] = types,
        ) -> str | None:
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
            return wrapped(self, *args, **kwargs)
        else:
            func.__globals__["reveal_type"] = reveal_type
            return func(self, *args, **kwargs)

    if is_async:

        @functools.wraps(wrapped)
        async def runner(
            self: _ModelTestCase_T, *args: _P.args, **kwargs: _P.kwargs
        ) -> _R:
            coro = run(self, *args, **kwargs)
            assert isinstance(coro, Awaitable)
            return await coro

        rewrapped = TestCaseMeta.wrap(runner)  # type: ignore [arg-type]
        return unittest.expectedFailure(rewrapped) if xfail else rewrapped

    else:
        run = functools.wraps(func)(run)
        return unittest.expectedFailure(run) if xfail else run  # type: ignore [return-value]


def _get_file_code(func: Callable[_P, _R]) -> str:
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
import typing_extensions

import gel

from gel._internal._testbase import _models as tb

if not typing.TYPE_CHECKING:
    def reveal_type(_: typing.Any) -> str:
        return ''

class TestModel(tb.{base_class_name}):

    {"async " if is_async else ""}def {wrapped.__name__}(self) -> None:
{textwrap.indent(dedented_body, "    " * 2)}
    """

    return source_code


def _typecheck_class(
    cls: type[_ModelTestCase_T],
    funcs: Sequence[Callable[Concatenate[_ModelTestCase_T, _P], _R]],
) -> None:
    """Extract all the typecheckable functions from a class and typecheck.

    Run both mypy and pyright, then stash the results where the
    individual functions will deal with them.
    """

    contents = [(func.__name__, _get_file_code(func)) for func in funcs]
    cls._mypy_errors = {}
    cls._pyright_errors = {}

    orig_setupclass = cls.setUpClass

    def _setUp(cls: type[_ModelTestCase_T]) -> None:
        orig_setupclass()

        orm_debug = os.environ.get("GEL_PYTHON_TEST_ORM") in {"1", "true"}

        td_kwargs: dict[str, Any] = {}
        if sys.version_info >= (3, 12):
            td_kwargs["delete"] = not orm_debug

        tmp_model_dir = tempfile.TemporaryDirectory(**td_kwargs)
        cls.add_class_cleanup_once(
            lambda: tmp_model_dir.cleanup() if not orm_debug else None
        )

        test_dir = _get_test_class_dir(cls)

        d = tmp_model_dir.name
        inifn = pathlib.Path(d) / "mypy.ini"
        tdir = pathlib.Path(d) / "tests"
        os.mkdir(tdir)

        name: str | None
        for name, code in contents:
            testfn = tdir / (name + ".py")
            with open(testfn, "w", encoding="utf-8") as f:
                f.write(code)

        with open(inifn, "w", encoding="utf-8") as f:
            f.write(
                textwrap.dedent("""\
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

        env = {**os.environ}
        if pypath := env.get("PYTHONPATH"):
            pypath = f"{test_dir}{os.pathsep}{pypath}"
        else:
            pypath = str(test_dir)
        env["PYTHONPATH"] = pypath

        try:
            cmd = [
                sys.executable,
                "-m",
                "mypy",
                "--strict",
                "--no-strict-equality",
                "--config-file",
                str(inifn),
                "--cache-dir",
                str(pathlib.Path(__file__).parent.parent / ".mypy_cache"),
                str(tdir),
            ]
            res = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
                cwd=inifn.parent,
                env=env,
            )

            cmd = [
                sys.executable,
                "-m",
                "pyright",
                str(tdir),
            ]

            pyright_res = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
                cwd=inifn.parent,
                env=env,  # work??
            )
        finally:
            inifn.unlink()
            shutil.rmtree(tdir)

        # Parse out mypy errors and assign them to test cases.
        # mypy lines are all prefixed with file name
        start = 'tests' + os.sep
        for line in res.stdout.decode('utf-8').split('\n'):
            if not (start in line and '.py' in line):
                continue
            name = line.split(start)[1].split('.')[0]
            cls._mypy_errors[name] = (
                cls._mypy_errors.get(name, '') + line + '\n'
            )

        # Parse out mypy errors and assign them to test cases.
        # Pyright lines have file name groups started by the name, and
        # then subsequent lines are indented. Messages can be
        # multiline.  They have a --outputjson mode that oculd save us
        # trouble here but would give us some more trouble on the
        # formatting side so whatever.
        name = None
        cur_lines = ''
        for line in pyright_res.stdout.decode('utf-8').split('\n'):
            if line.startswith('/'):
                if name:
                    cls._pyright_errors[name] = cur_lines
                cur_lines = ''
                name = line.split(start)[1].split('.')[0]
            else:
                cur_lines += line + '\n'
        if name:
            cls._pyright_errors[name] = cur_lines

    cls.setUpClass = classmethod(_setUp)  # type: ignore[method-assign, assignment]


def typecheck(arg: type[_ModelTestCase_T]) -> type[_ModelTestCase_T]:
    """Type-check one test of the entire test cases class.

    This is designed to type check unit tests that work with reflected Gel
    schemas and the query builder APIs.
    """
    # Please don't add arguments to this decorator, thank you.
    assert isinstance(arg, type)
    all_checked = []
    for func in arg.__dict__.values():
        if not isinstance(func, types.FunctionType):
            continue
        if not func.__name__.startswith("test_"):
            continue
        if hasattr(func, "_typecheck_skipped"):
            continue
        all_checked.append(func)
        new_func = _typecheck(arg, func)
        setattr(arg, func.__name__, new_func)

    _typecheck_class(arg, all_checked)
    return arg


def skip_typecheck(arg: Callable[_P, _R]) -> Callable[_P, _R]:
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
    arg._typecheck_skipped = True  # type: ignore[attr-defined]
    return arg


def pop_ids(dct: Any) -> Any:
    if isinstance(dct, list):
        for item in dct:
            pop_ids(item)
        return dct
    else:
        assert isinstance(dct, dict)
        dct.pop("id", None)
        dct.pop(TNAME, None)
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


def repickle(obj: _T) -> _T:
    # Use pure Python implementations (_dumps & _loads)
    # for better debugging when shtf.
    pickle._loads(pickle._dumps(obj))  # type: ignore[attr-defined]

    # And test that native implementation works as well
    # (kind of redundant, but just in case).
    return pickle.loads(pickle.dumps(obj))  # type: ignore[no-any-return]  # noqa: S301
