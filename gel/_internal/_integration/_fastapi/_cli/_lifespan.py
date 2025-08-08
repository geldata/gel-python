from __future__ import annotations
from typing import TYPE_CHECKING


import contextlib
import functools
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import threading


if TYPE_CHECKING:
    import io
    import types
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    import rich_toolkit  # pyright: ignore [reportMissingImports]


class SubprocessLogger:
    def __init__(
        self,
        command: list[str],
        cwd: pathlib.Path,
        cli: rich_toolkit.RichToolkit,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.process: subprocess.Popen[str] | None = None
        self.stdout_thread: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None
        self.cli = cli

    def _log_stream(
        self,
        stream: io.TextIOWrapper,
        log_func: Callable[[str], None],
    ) -> None:
        try:
            for line in iter(stream.readline, ""):
                log_func(line.strip())
        finally:
            stream.close()

    def __enter__(self) -> subprocess.Popen[str]:
        self.process = subprocess.Popen(
            self.command,
            cwd=self.cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            close_fds=True,
            text=True,
        )
        self.stdout_thread = threading.Thread(
            target=self._log_stream,
            args=(
                self.process.stdout,
                functools.partial(self.cli.print, tag="gel"),
            ),
            daemon=True,
        )
        self.stderr_thread = threading.Thread(
            target=self._log_stream,
            args=(
                self.process.stderr,
                functools.partial(self.cli.print, tag="gel"),
            ),
            daemon=True,
        )
        self.stdout_thread.start()
        self.stderr_thread.start()
        return self.process

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.cli.print("Stopping gel watch...", tag="gel")
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.cli.print(
                    "[warning]Subprocess did not exit in time; killing.",
                    tag="gel",
                )
                self.process.kill()

        if self.stdout_thread:
            self.stdout_thread.join(timeout=2)
        if self.stderr_thread:
            self.stderr_thread.join(timeout=2)
        self.cli.print("gel watch stopped", tag="gel")


@contextlib.contextmanager
def _gel_toml(app_path: pathlib.Path) -> Iterator[tuple[str, str]]:
    with tempfile.TemporaryDirectory() as tmpdir:
        output = app_path / "models"
        path = pathlib.Path(tmpdir)
        reloader = str(path / "reload_fastapi.py")
        reload_code = f"""\
            with open("{reloader}", "w+t") as f: print(file=f)
        """.strip().replace('"', '\\"')
        content = textwrap.dedent(f"""\
            [hooks-extend]
            schema.update.after="gel-generate-py models --output={output}"
            config.update.after="{sys.executable} -c '{reload_code}'"
        """)

        with (path / "gel.extend.toml").open("w+t", encoding="utf8") as f:
            print(content, file=f, flush=True)
            yield f.name, tmpdir


@contextlib.contextmanager
def fastapi_cli_lifespan(
    cli: rich_toolkit.RichToolkit,
    app_path: pathlib.Path,
) -> Iterator[str]:
    with _gel_toml(app_path) as (gel_toml, watch_file):
        cmd = [
            "gel",
            "watch",
            "--migrate",
            "--sync",
            "--extend-gel-toml",
            gel_toml,
        ]
        with SubprocessLogger(cmd, cwd=app_path, cli=cli):
            yield watch_file


def fastapi_cli_lifespan_hook(
    app_name: str,
    app_path: pathlib.Path,
    cli: rich_toolkit.RichToolkit,
) -> AbstractContextManager[str]:
    cli.print(f"Watching Gel project in [blue]{app_path}[/blue]", tag="gel")
    return fastapi_cli_lifespan(cli, app_path)
