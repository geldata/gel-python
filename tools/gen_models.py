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

import os
import shutil
import site
import subprocess
import sys
import tempfile
from pathlib import Path

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


def find_direct_schemas() -> list[tuple[str, str]]:
    """Find the schemas which are directly defined in a test's SCHEMA.

    ie. those not defined in tests/dbsetup/*.gel
    """
    from gel._testbase import BaseModelTestCase

    model_tests = find_classes_of_type(
        TESTS_ROOT,
        BaseModelTestCase,
        file_filter=lambda f: f.name.startswith('test_'),
    )

    direct_schemas: list[tuple[str, str]] = []
    for model_test in model_tests:
        model_from_file, model_name = model_test._model_info()

        if model_from_file:
            continue

        direct_schemas.append((model_name, model_test.get_combined_schemas()))

    return direct_schemas


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------


def main() -> None:  # noqa: D401 – simple script entry-point
    # Gather all schemas
    file_schemas = list(
        (schema_file.stem, schema_file)
        for schema_file in SCHEMAS_ROOT.glob('*.gel')
    )
    direct_schemas = find_direct_schemas()
    all_schemas: typing.Sequence[tuple[str, Path | str]] = (
        file_schemas + direct_schemas
    )

    with tempfile.TemporaryDirectory(
        prefix="gel_codegen_",
        suffix="_tmp",
    ) as tmp:
        tmpdir = Path(tmp)
        instance_name = tmpdir.name  # gel derives instance name from dir name

        if MODELS_DEST.exists():
            shutil.rmtree(MODELS_DEST)

        instance_created = False
        try:
            # 1. Initialise a new Gel project (creates an instance)
            _run(["gel", "init", "--non-interactive"], cwd=tmpdir)
            instance_created = True

            # 2. Copy schema into the freshly created project.
            dbschema_dir = tmpdir / "dbschema"
            dbschema_dir.mkdir(exist_ok=True)

            for schema_name, schema_file in all_schemas:
                generated_models = tmpdir / "models"
                if generated_models.exists():
                    shutil.rmtree(generated_models)

                migrations_dir = dbschema_dir / "migrations"
                if migrations_dir.exists():
                    shutil.rmtree(migrations_dir)

                if isinstance(schema_file, Path):
                    shutil.copy2(schema_file, dbschema_dir / 'schema.gel')
                elif isinstance(schema_file, str):
                    with open(dbschema_dir / 'schema.gel', "w") as f:
                        f.write(schema_file)
                else:
                    raise RuntimeError

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

                print(f"✅  {schema_name} has been reflected")

            print(
                f"✅  Models have been generated and installed into "
                f"{MODELS_DEST}"
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
