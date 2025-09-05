from __future__ import annotations
import typing

"""Generate Gel models using the *gel* command-line tool.

Steps performed (mirrors the user request):

1. Create a temporary directory.
2. Run ``gel init`` inside it (non-interactive).
3. Copy the test schema (``tests/dbsetup/orm.gel``) into
   ``<tmpdir>/dbschema``.
4. Execute ``gel-generate-py --no-cache models`` inside the temporary project.
5. Copy the resulting ``models`` package into the *site-packages* directory
   of **the current virtualenv** (overwriting any existing version).

Requirements
------------
* The script must run *inside* an **activated virtualenv**.  It will abort if
  no virtualenv is detected.
* The ``gel`` executable as well as ``gel-generate-py`` must be available in
  ``$PATH`` (typically installed alongside the ``gel-python`` package).
* The user must have a running Gel instance (or the environment properly
  configured with ``EDGEDB_DSN`` / ``EDGEDB_HOST`` & friends) so that
  ``gel-generate-py`` can connect and introspect the schema.
"""

import click
import filecmp
import os
import shutil
import site
import subprocess
import sys
import tempfile
from pathlib import Path

from gel._internal._testbase._models import BranchTestCase, BaseModelTestCase

# ---------------------------------------------------------------------------
# Safety checks & constants
# ---------------------------------------------------------------------------

if sys.prefix == sys.base_prefix and not os.environ.get("VIRTUAL_ENV"):
    sys.stderr.write(
        "error: this script must be executed from within an activated "
        "virtualenv (none detected)\n"
    )
    raise SystemExit(1)

SITE_PACKAGES: typing.Final[Path] = Path(site.getsitepackages()[0])
MODELS_DEST: typing.Final[Path] = SITE_PACKAGES / "models"

REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = REPO_ROOT / "tests"
SCHEMAS_ROOT = REPO_ROOT / "tests" / "dbsetup"
if not SCHEMAS_ROOT.exists():
    # Try fallback when executed from repository root.
    alt = Path(__file__).resolve().parents[1] / "tests" / "dbsetup"
    if alt.exists():
        SCHEMAS_ROOT = alt
    else:
        sys.stderr.write(f"error: schema file not found: {SCHEMAS_ROOT}\n")
        raise SystemExit(1)
CACHE_DIR = REPO_ROOT / ".gen_models_cache"


_T = typing.TypeVar('_T')


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _run(cmd: list[str] | tuple[str, ...], *, cwd: Path | None = None) -> None:
    """Run *cmd* via :pyfunc:`subprocess.run` with *check=True* and
    TTY-friendly I/O."""
    try:
        env = os.environ.copy()
        env['GEL_AUTO_BACKUP_MODE'] = 'disabled'
        subprocess.run(cmd, cwd=cwd, check=True)
    except FileNotFoundError as exc:  # pragma: no cover
        sys.stderr.write(
            f"error: cannot find executable '{cmd[0]}' – is it installed?\n"
        )
        raise SystemExit(1) from exc
    except subprocess.CalledProcessError as exc:  # pragma: no cover
        sys.stderr.write(
            f"error: command failed ({' '.join(cmd)}), code {exc.returncode}\n"
        )
        raise


def find_classes_of_type(
    directory: Path,
    base_type: typing.Type[_T],
    *,
    file_filter: typing.Callable[[Path], bool] | None = None,
) -> list[typing.Type[_T]]:
    """Find all classes in a directory which inherit from a base type.

    If file_filter is specified, only looks at files which pass the filter.
    """
    import importlib.util
    import inspect

    matching_classes = []

    for file in directory.glob("*.py"):
        if file.name == "__init__.py" or not file.name.startswith('test_'):
            continue
        if file_filter and not file_filter(file):
            continue

        module_name = file.stem

        if module_name in sys.modules:
            module = sys.modules[module_name]

        else:
            spec = importlib.util.spec_from_file_location(module_name, file)
            if not spec or not spec.loader:
                raise RuntimeError

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module_name and issubclass(obj, base_type):
                matching_classes.append(obj)

    return matching_classes


def get_schema_info(
    model_tests: typing.Collection[typing.Type[BranchTestCase]],
) -> dict[str, str | Path]:
    """Find the schemas which are directly defined in a test's SCHEMA.

    ie. those not defined in tests/dbsetup/*.gel
    """
    schema_infos: dict[str, str | Path] = {}
    for model_test in model_tests:
        if (
            issubclass(model_test, BaseModelTestCase)
            and (model_info := model_test._model_info())
            and not model_info[0]  # model not from file
        ):
            model_name = model_info[1]

            schema_infos[model_name] = model_test.get_combined_schemas()

        else:
            for name, val in model_test.__dict__.items():
                if model_test.get_schema_field_name(name) is None:
                    continue

                schema_path = Path(val)
                schema_infos[schema_path.stem] = schema_path

    return schema_infos


def find_test_classes(
    directory: Path,
    base_type: typing.Type[_T],
    include_tests: typing.Sequence[str],
) -> set[typing.Type[_T]]:
    import unittest

    # Use unittest to find tests that would run
    def get_test_class_names(tests: list[unittest.TestSuite]) -> list[str]:
        result: list[str] = []

        for test in tests:
            for test_entry in test._tests:
                if isinstance(test_entry, unittest.TestSuite):
                    result.extend(get_test_class_names([test_entry]))
                    continue

                if getattr(test_entry, '__unittest_skip__', False):
                    continue

                result.append(type(test_entry).__name__)

        return result

    class TestResult:
        def wasSuccessful(self):
            return True

    class TestRunner:
        test_class_names: set[str]

        def __init__(self):
            self.test_class_names = set()

        def run(self, test):
            self.test_class_names.update(get_test_class_names([test]))
            return TestResult()

    runner = TestRunner()
    include_tests = [x for pat in include_tests for x in ['-k', pat]]
    unittest.main(
        module=None,
        argv=['unittest', 'discover', '-s', str(directory), *include_tests],
        testRunner=runner,
        exit=False,
    )

    # Find model tests
    model_tests = find_classes_of_type(
        TESTS_ROOT,
        base_type,
        file_filter=lambda f: f.name.startswith('test_'),
    )

    # Return a filtered set
    return {
        model_test
        for model_test in model_tests
        if model_test.__name__ in runner.test_class_names
    }


def copy_schema_to(
    schema: str | Path,
    target_file: Path,
) -> None:
    if isinstance(schema, Path):
        shutil.copy2(schema, target_file)

    elif isinstance(schema, str):
        with open(target_file, "w") as f:
            f.write(schema)

    else:
        raise RuntimeError


def has_schema_changed(
    schema: str | Path,
    cached_schema_path: Path,
) -> bool:
    if not cached_schema_path.exists():
        return True

    if isinstance(schema, Path):
        return not filecmp.cmp(schema, cached_schema_path, shallow=False)

    elif isinstance(schema, str):
        with open(cached_schema_path, 'r') as f:
            cached_content = f.read()

        return cached_content != schema

    else:
        raise RuntimeError


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------


@click.command()
@click.option('-k', multiple=True)
@click.option('--cache/--no-cache', default=True)
def main(
    k: typing.Sequence[str],
    cache: bool,
) -> None:  # noqa: D401 – simple script entry-point
    # Gather schemas
    all_schemas: typing.Sequence[tuple[str, Path | str]]

    if k:
        model_tests = find_test_classes(
            TESTS_ROOT,
            BranchTestCase,
            k,
        )
        all_schemas = list(get_schema_info(model_tests).items())

    else:
        model_tests = find_classes_of_type(
            TESTS_ROOT,
            BaseModelTestCase,
            file_filter=lambda f: f.name.startswith('test_'),
        )
        all_schemas = list(get_schema_info(model_tests).items())

        # If we are not filtering, additionally look through schemas root for
        # any other schema files
        test_schema_paths = {s[1] for s in all_schemas}
        for schema_file in list(SCHEMAS_ROOT.glob('*.gel')):
            if schema_file not in test_schema_paths:
                all_schemas.append((schema_file.stem, schema_file))
                test_schema_paths.add(schema_file)

    # If we are using the cache, filter out any schemas which have not changed
    unchanged_count = 0

    if cache:
        changed_schemas: list[tuple[str, Path | str]] = []
        for schema_name, schema in all_schemas:
            if not CACHE_DIR.exists():
                os.mkdir(CACHE_DIR)

            cached_schema_path = CACHE_DIR / schema_name

            if has_schema_changed(schema, cached_schema_path):
                changed_schemas.append((schema_name, schema))

        if not changed_schemas:
            print(f"✅  No schemas have changed!")
            return

        unchanged_count = len(all_schemas) - len(changed_schemas)
        all_schemas = changed_schemas

    # Generate models for the schemas
    print(f"ℹ️  Reflecting {len(all_schemas)} models")
    if unchanged_count:
        print(f"ℹ️  Skipping {unchanged_count} unchanged models")

    with tempfile.TemporaryDirectory(
        prefix="gel_codegen_",
        suffix="_tmp",
    ) as tmp:
        tmpdir = Path(tmp)
        instance_name = tmpdir.name  # gel derives instance name from dir name

        if MODELS_DEST.exists():
            if k or unchanged_count:
                # We are not regenerating all models
                # Only remove the models we are regenerating
                for schema_name, _ in all_schemas:
                    schema_model_dir = MODELS_DEST / schema_name
                    if schema_model_dir.exists():
                        shutil.rmtree(schema_model_dir)

            else:
                # Remove all models
                shutil.rmtree(MODELS_DEST)

        instance_created = False
        try:
            # 1. Initialise a new Gel project (creates an instance)
            _run(["gel", "init", "--non-interactive"], cwd=tmpdir)
            instance_created = True

            # 2. Copy schema into the freshly created project.
            dbschema_dir = tmpdir / "dbschema"
            dbschema_dir.mkdir(exist_ok=True)

            for schema_name, schema in all_schemas:
                generated_models = tmpdir / "models"
                if generated_models.exists():
                    shutil.rmtree(generated_models)

                migrations_dir = dbschema_dir / "migrations"
                if migrations_dir.exists():
                    shutil.rmtree(migrations_dir)

                copy_schema_to(schema, dbschema_dir / 'schema.gel')

                _run(
                    ["gel", "branch", "create", "--empty", schema_name],
                    cwd=tmpdir,
                )

                _run(
                    ["gel", "branch", "switch", schema_name],
                    cwd=tmpdir,
                )

                # 3. Create & apply migration, then generate models.
                _run(
                    ["gel", "migration", "create", "--non-interactive"],
                    cwd=tmpdir,
                )
                _run(["gel", "migrate"], cwd=tmpdir)
                _run(
                    [
                        "gel-generate-py",
                        "--no-cache",
                        "models",
                        "--output",
                        f"models/{schema_name}",
                    ],
                    cwd=tmpdir,
                )
                with open(Path(tmpdir) / "models" / "py.typed", "w") as _:
                    pass

                # 4. Install models into *site-packages*.
                if (
                    not generated_models.exists()
                ):  # pragma: no cover – sanity check
                    sys.stderr.write(
                        "error: models directory not produced by codegen\n"
                    )
                    raise SystemExit(1)

                shutil.copytree(
                    generated_models, MODELS_DEST, dirs_exist_ok=True
                )

                # 5. If we are caching, copy schema to cache
                if cache:
                    cached_schema_path = CACHE_DIR / schema_name
                    if cached_schema_path.exists():
                        os.remove(cached_schema_path)
                    copy_schema_to(schema, cached_schema_path)

                print(f"✅  {schema_name} has been reflected")

            print(
                f"✅  {len(all_schemas)} models have been generated and "
                f"installed into {MODELS_DEST}"
            )

        finally:
            # Always attempt cleanup of project & instance so we don't leak
            # them
            if instance_created:
                try:
                    subprocess.run(
                        ["gel", "project", "unlink"], cwd=tmpdir, check=True
                    )
                finally:
                    subprocess.run(
                        [
                            "gel",
                            "instance",
                            "destroy",
                            "-I",
                            instance_name,
                            "--force",
                        ],
                        check=True,
                    )


if __name__ == "__main__":
    main()
