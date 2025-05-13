# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.


from typing import (
    Any,
)
from collections.abc import (
    Callable,
)
from typing_extensions import (
    get_overloads,
)

import inspect


def dispatch_overload(
    func: Callable[..., object],
    *args: Any,
    **kwargs: Any,
) -> Any:
    for func_overload in get_overloads(func):
        sig = inspect.signature(func_overload)
        try:
            sig.bind(*args, **kwargs)
        except TypeError:
            pass
        else:
            result = func_overload(*args, **kwargs)
            break
    else:
        raise TypeError(
            f"cannot dispatch to {func}: no overload found for "
            f"args={args} and kwargs={kwargs}"
        )

    return result
