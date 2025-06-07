from typing import (
    Any,
)

import inspect
import importlib.util
import types


def _get_fastapi_cli_import_site() -> types.FrameType | None:
    stack = inspect.stack()
    for frame_info in stack:
        frame = frame_info.frame
        caller_module = frame.f_globals.get("__name__")
        if (
            caller_module == "fastapi_cli.cli"
            and frame.f_code.co_name == "_run"
        ):
            return frame
    return None


def maybe_patch_fastapi_cli() -> None:
    if importlib.util.find_spec("fastapi") is None:
        # No FastAPI here, move along.
        return

    try:
        import uvicorn  # noqa: PLC0415  # pyright: ignore [reportMissingImports]
    except ImportError:
        return

    fastapi_cli_import_site = _get_fastapi_cli_import_site()
    if fastapi_cli_import_site is None:
        # Not being imported by fastapi.cli
        return

    def _patched_uvicorn_run(*args: Any, **kwargs: Any) -> None:
        from . import _lifespan  # noqa: PLC0415

        import_data = fastapi_cli_import_site.f_locals.get("import_data")
        cli = fastapi_cli_import_site.f_locals.get("toolkit")
        if import_data is not None and cli is not None:
            app_path = import_data.module_data.module_paths[-1].parent
            with _lifespan.fastapi_cli_lifespan(cli, app_path):
                uvicorn.run(*args, **kwargs)
        else:
            uvicorn.run(*args, **kwargs)

    class PatchedUvicornModule(types.ModuleType):
        def __getattribute__(self, name: str) -> Any:
            if name == "run":
                return _patched_uvicorn_run
            else:
                return getattr(uvicorn, name)

    fastapi_cli_import_site.f_globals["uvicorn"] = PatchedUvicornModule(
        uvicorn.__name__,
        doc=uvicorn.__doc__,
    )
