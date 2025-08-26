# ruff: noqa: N802

from __future__ import annotations
from typing import (
    Any,
    ClassVar,
    Concatenate,
    ParamSpec,
    TypeVar,
    TYPE_CHECKING,
)
from collections.abc import Awaitable

import argparse
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
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
import warnings


from gel import blocking_client

from gel._internal._codegen._models import PydanticModelsGenerator

from ._base import (
    AsyncQueryTestCase,
    BranchTestCase,
    SyncQueryTestCase,
    TestCaseMeta,
    TestClient,
    must_fail,
    to_be_fixed,
    xfail,
)

__all__ = (
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
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    import pydantic


_unset = object()

_T = TypeVar("_T")
_P = ParamSpec("_P")
_R = TypeVar("_R", covariant=True)
_ModelTestCase_T = TypeVar("_ModelTestCase_T", bound="BaseModelTestCase")


class BaseModelTestCase(BranchTestCase):
    DEFAULT_MODULE = "default"

    orm_debug: ClassVar[bool]
    tmp_model_dir: ClassVar[tempfile.TemporaryDirectory[str]]
    gen: ClassVar[Any]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        if "models" in sys.modules:
            raise RuntimeError('"models" module has already been imported')

        cls.orm_debug = os.environ.get("GEL_PYTHON_TEST_ORM") in {"1", "true"}

        td_kwargs: dict[str, Any] = {}
        if sys.version_info >= (3, 12):
            td_kwargs["delete"] = not cls.orm_debug

        cls.tmp_model_dir = tempfile.TemporaryDirectory(**td_kwargs)

        assert isinstance(cls.SCHEMA, str)
        short_name = pathlib.Path(cls.SCHEMA).stem

        if cls.orm_debug:
            print(cls.tmp_model_dir.name)  # noqa: T201

        gen_client = cls.make_blocking_test_client(
            connection_class=blocking_client.BlockingIOConnection,
            database=cls.get_database_name(),
        )
        if not isinstance(gen_client, TestClient):
            raise RuntimeError("Expected TestClient for generation")
        try:
            gen_client.ensure_connected()
            base = pathlib.Path(cls.tmp_model_dir.name) / "models"

            cls.gen = PydanticModelsGenerator(
                argparse.Namespace(
                    no_cache=True,
                    quiet=True,
                    output=(base / short_name).absolute(),
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

        import models  # noqa: PLC0415

        assert models.__file__ == os.path.join(
            cls.tmp_model_dir.name, "models", "__init__.py"
        ), (
            models.__file__,
            os.path.join(cls.tmp_model_dir.name, "models", "__init__.py"),
        )

    @classmethod
    def tearDownClass(cls) -> None:
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


class ModelTestCase(SyncQueryTestCase, BaseModelTestCase):  # pyright: ignore[reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
    pass


class AsyncModelTestCase(AsyncQueryTestCase, BaseModelTestCase):  # pyright: ignore[reportIncompatibleVariableOverride, reportIncompatibleMethodOverride]
    pass


def _typecheck(
    func: Callable[Concatenate[_ModelTestCase_T, _P], _R],
    *,
    xfail: bool = False,
) -> Callable[Concatenate[_ModelTestCase_T, _P], _R]:
    wrapped: Callable[Concatenate[_ModelTestCase_T, _P], _R] = inspect.unwrap(
        func
    )
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

from gel._internal._testbase import _models as tb

if not typing.TYPE_CHECKING:
    def reveal_type(_: Any) -> str:
        return ''

class TestModel(tb.{base_class_name}):
    {"async " if is_async else ""}def {wrapped.__name__}(self) -> None:
{textwrap.indent(dedented_body, "    " * 2)}
    """

    def run(
        self: _ModelTestCase_T, *args: _P.args, **kwargs: _P.kwargs
    ) -> _R | Awaitable[_R]:
        d = type(self).tmp_model_dir.name

        testfn = pathlib.Path(d) / "test.py"
        inifn = pathlib.Path(d) / "mypy.ini"

        with open(testfn, "w") as f:  # noqa: PLW1514, FURB103
            f.write(source_code)

        with open(inifn, "w") as f:  # noqa: PLW1514, FURB103
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
                cmd,  # type: ignore[arg-type]
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
                types.append(m.group("name"))  # noqa: PERF401

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


def typecheck(
    arg: type | Callable[Concatenate[_ModelTestCase_T, _P], _R],
) -> type | Callable[Concatenate[_ModelTestCase_T, _P], _R]:
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
