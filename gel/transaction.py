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


import typing

import enum

from . import abstract
from . import errors
from . import options


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class BaseTransaction:

    __slots__ = (
        '_client',
        '_connection',
        '_options',
        '_state',
        '__retry',
        '__iteration',
        '__started',
    )

    def __init__(self, retry, client, iteration):
        self._client = client
        self._connection = None
        self._options = retry._options.transaction_options
        self._state = TransactionState.NEW
        self.__retry = retry
        self.__iteration = iteration
        self.__started = False

    def is_active(self) -> bool:
        return self._state is TransactionState.STARTED

    def __check_state_base(self, opname):
        if self._state is TransactionState.COMMITTED:
            raise errors.InterfaceError(
                'cannot {}; the transaction is already committed'.format(
                    opname))
        if self._state is TransactionState.ROLLEDBACK:
            raise errors.InterfaceError(
                'cannot {}; the transaction is already rolled back'.format(
                    opname))
        if self._state is TransactionState.FAILED:
            raise errors.InterfaceError(
                'cannot {}; the transaction is in error state'.format(
                    opname))

    def __check_state(self, opname):
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise errors.InterfaceError(
                    'cannot {}; the transaction is not yet started'.format(
                        opname))
            self.__check_state_base(opname)

    def _make_start_query(self):
        self.__check_state_base('start')
        if self._state is TransactionState.STARTED:
            raise errors.InterfaceError(
                'cannot start; the transaction is already started')

        return self._options.start_transaction_query()

    def _make_commit_query(self):
        self.__check_state('commit')
        return 'COMMIT;'

    def _make_rollback_query(self):
        self.__check_state('rollback')
        return 'ROLLBACK;'

    def __repr__(self):
        attrs = []
        attrs.append('state:{}'.format(self._state.name.lower()))
        attrs.append(repr(self._options))

        if self.__class__.__module__.startswith('gel.'):
            mod = 'gel'
        else:
            mod = self.__class__.__module__

        return '<{}.{} {} {:#x}>'.format(
            mod, self.__class__.__name__, ' '.join(attrs), id(self))

    async def _ensure_transaction(self):
        if not self.__started:
            self.__started = True
            query = self._make_start_query()
            self._connection = await self._client._impl.acquire()
            if self._connection.is_closed():
                await self._connection.connect(
                    single_attempt=self.__iteration != 0
                )
            try:
                await self._privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.STARTED

    async def _exit(self, extype, ex):
        if not self.__started:
            return False

        try:
            if extype is None:
                query = self._make_commit_query()
                state = TransactionState.COMMITTED
            else:
                query = self._make_rollback_query()
                state = TransactionState.ROLLEDBACK
            try:
                await self._privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                if extype is None:
                    # COMMIT itself may fail; recover in connection
                    await self._privileged_execute("ROLLBACK;")
                raise
            else:
                self._state = state
        except errors.EdgeDBError as err:
            if ex is None:
                # On commit we don't know if commit is succeeded before the
                # database have received it or after it have been done but
                # network is dropped before we were able to receive a response.
                # On a TransactionError, though, we know the we need
                # to retry.
                # TODO(tailhook) should other errors have retries?
                if (
                    isinstance(err, errors.TransactionError)
                    and err.has_tag(errors.SHOULD_RETRY)
                    and self.__retry._retry(err)
                ):
                    pass
                else:
                    raise err
            # If we were going to rollback, look at original error
            # to find out whether we want to retry, regardless of
            # the rollback error.
            # In this case we ignore rollback issue as original error is more
            # important, e.g. in case `CancelledError` it's important
            # to propagate it to cancel the whole task.
            # NOTE: rollback error is always swallowed, should we use
            # on_log_message for it?
        finally:
            await self._client._impl.release(self._connection)

        if (
            extype is not None and
            issubclass(extype, errors.EdgeDBError) and
            ex.has_tag(errors.SHOULD_RETRY)
        ):
            return self.__retry._retry(ex)

    def _get_query_cache(self) -> abstract.QueryCache:
        return self._client._get_query_cache()

    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]:
        return None

    def _get_state(self) -> options.State:
        return self._client._get_state()

    def _get_warning_handler(self) -> options.WarningHandler:
        return self._client._get_warning_handler()

    def _get_annotations(self) -> typing.Dict[str, str]:
        return self._client._get_annotations()

    async def _query(self, query_context: abstract.QueryContext):
        await self._ensure_transaction()
        return await self._connection.raw_query(query_context)

    async def _execute(self, execute_context: abstract.ExecuteContext) -> None:
        await self._ensure_transaction()
        await self._connection._execute(execute_context)

    async def _privileged_execute(self, query: str) -> None:
        await self._connection.privileged_execute(abstract.ExecuteContext(
            query=abstract.QueryWithArgs(query, (), {}),
            cache=self._get_query_cache(),
            state=self._get_state(),
            retry_options=self._get_retry_options(),
            warning_handler=self._get_warning_handler(),
            annotations=self._get_annotations(),
        ))


class BaseRetry:

    def __init__(self, owner):
        self._owner = owner
        self._iteration = 0
        self._done = False
        self._next_backoff = 0
        self._options = owner._options

    def _retry(self, exc):
        self._last_exception = exc
        rule = self._options.retry_options.get_rule_for_exception(exc)
        if self._iteration >= rule.attempts:
            return False
        self._done = False
        self._next_backoff = rule.backoff(self._iteration)
        return True
