#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ruff: noqa: ERA001


from __future__ import annotations
from typing import TYPE_CHECKING, Any
from typing_extensions import Self

import asyncio
import functools
import itertools
import textwrap
import traceback

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import TracebackType


class TaskGroup:
    _loop: asyncio.AbstractEventLoop

    def __init__(self, *, name: str | None = None) -> None:
        if name is None:
            self._name = f"tg-{_name_counter()}"
        else:
            self._name = str(name)

        self._entered = False
        self._exiting = False
        self._aborting = False
        self._parent_task: asyncio.Task[Any] | None = None
        self._parent_cancel_requested = False
        self._tasks: set[asyncio.Task[Any]] = set()
        self._unfinished_tasks = 0
        self._errors: list[BaseException] = []
        self._base_error: BaseException | None = None
        self._on_completed_fut: asyncio.Future[Any] | None = None

    def get_name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        msg = f"<TaskGroup {self._name!r}"
        if self._tasks:
            msg += f" tasks:{len(self._tasks)}"
        if self._unfinished_tasks:
            msg += f" unfinished:{self._unfinished_tasks}"
        if self._errors:
            msg += f" errors:{len(self._errors)}"
        if self._aborting:
            msg += " cancelling"
        elif self._entered:
            msg += " entered"
        msg += ">"
        return msg

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError(f"TaskGroup {self!r} has been already entered")
        self._entered = True

        self._loop = asyncio.get_event_loop()

        self._parent_task = asyncio.current_task(self._loop)
        if self._parent_task is None:
            raise RuntimeError(
                f"TaskGroup {self!r} cannot determine the parent task"
            )
        self._patch_task(self._parent_task)

        return self

    async def __aexit__(
        self,
        et: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._exiting = True
        propagate_cancelation = False

        if (
            exc is not None
            and self._is_base_error(exc)
            and self._base_error is None
        ):
            self._base_error = exc

        if et is asyncio.CancelledError:
            if self._parent_cancel_requested:
                # Only if we did request task to cancel ourselves
                # we mark it as no longer cancelled.
                assert self._parent_task is not None
                self._parent_task.__cancel_requested__ = False  # type: ignore [attr-defined]
            else:
                propagate_cancelation = True

        if et is not None and not self._aborting:
            # Our parent task is being cancelled:
            #
            #    async with TaskGroup() as g:
            #        g.create_task(...)
            #        await ...  # <- CancelledError
            #
            if et is asyncio.CancelledError:
                propagate_cancelation = True

            # or there's an exception in "async with":
            #
            #    async with TaskGroup() as g:
            #        g.create_task(...)
            #        1 / 0
            #
            self._abort()

        # We use while-loop here because "self._on_completed_fut"
        # can be cancelled multiple times if our parent task
        # is being cancelled repeatedly (or even once, when
        # our own cancellation is already in progress)
        while self._unfinished_tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = self._loop.create_future()

            try:
                await self._on_completed_fut
            except asyncio.CancelledError:
                if not self._aborting:
                    # Our parent task is being cancelled:
                    #
                    #    async def wrapper():
                    #        async with TaskGroup() as g:
                    #            g.create_task(foo)
                    #
                    # "wrapper" is being cancelled while "foo" is
                    # still running.
                    propagate_cancelation = True
                    self._abort()

            self._on_completed_fut = None

        assert self._unfinished_tasks == 0
        self._on_completed_fut = None  # no longer needed

        if self._base_error is not None:
            raise self._base_error

        if propagate_cancelation:
            # The wrapping task was cancelled; since we're done with
            # closing all child tasks, just propagate the cancellation
            # request now.
            raise asyncio.CancelledError()

        if (
            et is not None
            and et is not asyncio.CancelledError
            and exc is not None
        ):
            self._errors.append(exc)

        if self._errors:
            # Exceptions are heavy objects that can have object
            # cycles (bad for GC); let's not keep a reference to
            # a bunch of them.
            errors = tuple(self._errors)
            self._errors.clear()

            me = TaskGroupError(
                "unhandled errors in a TaskGroup", errors=errors
            )
            raise me from None

    def create_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        if not self._entered:
            raise RuntimeError(f"TaskGroup {self!r} has not been entered")
        if self._exiting:
            raise RuntimeError(f"TaskGroup {self!r} is awaiting in exit")
        assert self._loop is not None
        task = self._loop.create_task(coro)
        task.add_done_callback(self._on_task_done)
        self._unfinished_tasks += 1
        self._tasks.add(task)
        return task

    def _is_base_error(self, exc: BaseException) -> bool:
        assert isinstance(exc, BaseException)
        return not isinstance(exc, Exception)

    def _patch_task(self, task: asyncio.Task[Any]) -> None:
        # In Python 3.8 we'll need proper API on asyncio.Task to
        # make TaskGroups possible. We need to be able to access
        # information about task cancellation, more specifically,
        # we need a flag to say if a task was cancelled or not.
        # We also need to be able to flip that flag.

        def _task_cancel(
            task: asyncio.Task[Any],
            orig_cancel: Callable[[Any], bool],
            msg: Any | None = None,
        ) -> bool:
            task.__cancel_requested__ = True  # type: ignore [attr-defined]
            return orig_cancel(msg)

        if hasattr(task, "__cancel_requested__"):
            return

        task.__cancel_requested__ = False  # type: ignore [attr-defined]
        # confirm that we were successful at adding the new attribute:
        assert not task.__cancel_requested__  # type: ignore [attr-defined]

        orig_cancel = task.cancel
        task.cancel = functools.partial(_task_cancel, task, orig_cancel)  # type: ignore [method-assign]

    def _abort(self) -> None:
        self._aborting = True

        for t in self._tasks:
            if not t.done():
                t.cancel()

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        self._unfinished_tasks -= 1
        assert self._unfinished_tasks >= 0

        if (
            self._exiting
            and not self._unfinished_tasks
            and self._on_completed_fut is not None
            and not self._on_completed_fut.done()
        ):
            self._on_completed_fut.set_result(True)

        if task.cancelled():
            return

        exc = task.exception()
        if exc is None:
            return

        self._errors.append(exc)
        if self._is_base_error(exc) and self._base_error is None:
            self._base_error = exc

        assert self._parent_task is not None
        if self._parent_task.done():
            # Not sure if this case is possible, but we want to handle
            # it anyways.
            self._loop.call_exception_handler(
                {
                    "message": f"Task {task!r} has errored out but its parent "
                    f"task {self._parent_task} is already completed",
                    "exception": exc,
                    "task": task,
                }
            )
            return

        self._abort()
        if not self._parent_task.__cancel_requested__:  # type: ignore [attr-defined]
            # If parent task *is not* being cancelled, it means that we want
            # to manually cancel it to abort whatever is being run right now
            # in the TaskGroup.  But we want to mark parent task as
            # "not cancelled" later in __aexit__.  Example situation that
            # we need to handle:
            #
            #    async def foo():
            #        try:
            #            async with TaskGroup() as g:
            #                g.create_task(crash_soon())
            #                await something  # <- this needs to be canceled
            #                                 #    by the TaskGroup, e.g.
            #                                 #    foo() needs to be cancelled
            #        except Exception:
            #            # Ignore any exceptions raised in the TaskGroup
            #            pass
            #        await something_else     # this line has to be called
            #                                 # after TaskGroup is finished.
            self._parent_cancel_requested = True
            self._parent_task.cancel()


class MultiError(Exception):
    def __init__(
        self, msg: str, *args: Any, errors: tuple[BaseException, ...] = ()
    ) -> None:
        if errors:
            types = {type(e).__name__ for e in errors}
            msg = f"{msg}; {len(errors)} sub errors: ({', '.join(types)})"
            for er in errors:
                msg += f"\n + {type(er).__name__}: {er}"
                if er.__traceback__:
                    er_tb = "".join(traceback.format_tb(er.__traceback__))
                    er_tb = textwrap.indent(er_tb, " | ")
                    msg += f"\n{er_tb}\n"
        super().__init__(msg, *args)
        self.__errors__ = tuple(errors)

    def get_error_types(self) -> set[type[BaseException]]:
        return {type(e) for e in self.__errors__}

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.args,), {"__errors__": self.__errors__})


class TaskGroupError(MultiError):
    pass


_name_counter = itertools.count(1).__next__
