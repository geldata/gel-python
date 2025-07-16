# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import TYPE_CHECKING

import contextlib
import inspect

if TYPE_CHECKING:
    import types
    from collections.abc import Iterator


@contextlib.contextmanager
def frame(stack_offset: int = 2) -> Iterator[types.FrameType | None]:
    frame = inspect.currentframe()
    try:
        counter = 0
        while frame is not None and counter < stack_offset + 2:
            frame = frame.f_back
            counter += 1

        yield frame
    finally:
        if frame is not None:
            # Break possible refcycle (of this frame onto itself via locals)
            del frame
