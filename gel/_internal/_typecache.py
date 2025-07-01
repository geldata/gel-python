# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    NamedTuple,
    ParamSpec,
    TypeVar,
    final,
)
from typing_extensions import Self, TypeAliasType

import functools
import threading
import weakref

if TYPE_CHECKING:
    from collections.abc import Callable

_T = TypeVar("_T")  # Type of the first argument
_P = ParamSpec("_P")  # Parameter specification for the rest of the args
_R = TypeVar("_R")  # Return type

# A cache-key: positional args tuple + sorted kwargs tuple
_CacheKey = TypeAliasType(
    "_CacheKey",
    tuple[tuple[Any, ...], tuple[tuple[str, Any], ...]],
)


class CacheInfo(NamedTuple):
    """Cache statistics similar to functools.lru_cache."""

    hits: int
    misses: int
    maxsize: None  # Always None for type_cache (unbounded)
    currsize: int


@final
class _TypeCacheWrapper(Generic[_T, _P, _R]):
    """Cache wrapper that mimics functools._lru_cache_wrapper interface."""

    def __init__(self, func: Callable[Concatenate[_T, _P], _R]) -> None:
        self.__wrapped__ = func
        self._cache: weakref.WeakKeyDictionary[_T, dict[_CacheKey, _R]] = (
            weakref.WeakKeyDictionary()
        )
        self._hits = 0
        self._misses = 0
        # Use a simple lock for thread safety (similar to functools.lru_cache)
        self._lock = threading.RLock()

        # Copy function metadata like functools.wraps does
        functools.update_wrapper(self, func)

    def __call__(
        self, first_arg: _T, *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        with self._lock:
            # Get or create the per-instance cache
            subcache = self._cache.get(first_arg)
            if subcache is None:
                subcache = {}
                self._cache[first_arg] = subcache

            # Build a hashable key from the remaining args/kwargs
            key: _CacheKey = (args, tuple(sorted(kwargs.items())))

            if key in subcache:
                self._hits += 1
                return subcache[key]

            # Cache miss - compute, store, and return
            self._misses += 1
            result = self.__wrapped__(first_arg, *args, **kwargs)
            subcache[key] = result
            return result

    def cache_info(self) -> CacheInfo:
        """Return cache statistics."""
        with self._lock:
            # Count current size across all weak-key subcaches
            currsize = sum(len(subcache) for subcache in self._cache.values())
            return CacheInfo(
                hits=self._hits,
                misses=self._misses,
                maxsize=None,
                currsize=currsize,
            )

    def cache_clear(self) -> None:
        """Clear the cache and reset statistics."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def __copy__(self) -> Self:
        # Return a new wrapper with the same function but fresh cache
        return type(self)(self.__wrapped__)

    def __deepcopy__(self, memo: Any) -> Self:
        # Return a new wrapper with the same function but fresh cache
        return type(self)(self.__wrapped__)

    def __repr__(self) -> str:
        return f"<type_cache_wrapper({self.__wrapped__})>"


def type_cache(
    func: Callable[Concatenate[_T, _P], _R], /
) -> _TypeCacheWrapper[_T, _P, _R]:
    """
    Decorator that caches results of `func` based on:
      1) identity of the first argument (used as a weak key)
      2) the values of any additional args/kwargs.

    When the first argument is garbage-collected, its cache entry is
    automatically removed.

    This is similar to functools.cache but uses weak references for the first
    argument, allowing objects to be garbage collected even when cached.

    The decorated function will have the following methods:
    - cache_info(): Returns CacheInfo with hit/miss statistics
    - cache_clear(): Clears the cache and resets statistics
    """
    return _TypeCacheWrapper(func)
