from __future__ import annotations

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
from typing import Final

# ---------------------------------------------------------------------------
# Safety checks & constants
# ---------------------------------------------------------------------------

if sys.prefix == sys.base_prefix and not os.environ.get("VIRTUAL_ENV"):
    sys.stderr.write(
        "error: this script must be executed from within an activated "
        "virtualenv (none detected)\n"
    )
    raise SystemExit(1)

SITE_PACKAGES: Final[Path] = Path(site.getsitepackages()[0])
MODELS_DEST: Final[Path] = SITE_PACKAGES / "models"

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SRC = REPO_ROOT / "tests" / "dbsetup" / "orm.gel"
if not SCHEMA_SRC.exists():
    # Try fallback when executed from repository root.
    alt = Path(__file__).resolve().parents[1] / "tests" / "dbsetup" / "orm.gel"
    if alt.exists():
        SCHEMA_SRC = alt
    else:
        sys.stderr.write(f"error: schema file not found: {SCHEMA_SRC}\n")
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _run(cmd: list[str] | tuple[str, ...], *, cwd: Path | None = None) -> None:
    """Run *cmd* via :pyfunc:`subprocess.run` with *check=True* and TTY-friendly I/O."""
    try:
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


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------


def main() -> None:  # noqa: D401 – simple script entry-point
    with tempfile.TemporaryDirectory(
        prefix="gel_codegen_",
        suffix="_tmp",
    ) as tmp:
        tmpdir = Path(tmp)
        instance_name = tmpdir.name  # gel derives instance name from dir name

        instance_created = False
        try:
            # 1. Initialise a new Gel project (creates an instance)
            _run(["gel", "init", "--non-interactive"], cwd=tmpdir)
            instance_created = True

            # 2. Copy schema into the freshly created project.
            dbschema_dir = tmpdir / "dbschema"
            dbschema_dir.mkdir(exist_ok=True)
            shutil.copy2(SCHEMA_SRC, dbschema_dir / "orm.gel")

            # 3. Create & apply migration, then generate models.
            _run(
                ["gel", "migration", "create", "--non-interactive"], cwd=tmpdir
            )
            _run(["gel", "migrate"], cwd=tmpdir)
            _run(["gel-generate-py", "--no-cache", "models"], cwd=tmpdir)

            # 4. Install models into *site-packages*.
            generated_models = tmpdir / "models"
            if (
                not generated_models.exists()
            ):  # pragma: no cover – sanity check
                sys.stderr.write(
                    "error: models directory not produced by codegen\n"
                )
                raise SystemExit(1)

            if MODELS_DEST.exists():
                shutil.rmtree(MODELS_DEST)
            shutil.copytree(generated_models, MODELS_DEST)

            print(
                f"✅  Models have been generated and installed into {MODELS_DEST}"
            )

        finally:
            # Always attempt cleanup of project & instance so we don't leak them
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
