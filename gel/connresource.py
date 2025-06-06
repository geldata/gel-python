#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import functools

from . import errors


def guarded(meth):
    """A decorator to add a sanity check to ConnectionResource methods."""

    @functools.wraps(meth)
    def _check(self, *args, **kwargs):
        self._check_conn_validity(meth.__name__)
        return meth(self, *args, **kwargs)

    return _check


class ConnectionResource:
    def __init__(self, connection):
        self._connection = connection
        self._con_release_ctr = connection._pool_release_ctr

    def _check_conn_validity(self, meth_name):
        con_release_ctr = self._connection._pool_release_ctr
        if con_release_ctr != self._con_release_ctr:
            raise errors.InterfaceError(
                "cannot call {}.{}(): "
                "the underlying connection has been released back "
                "to the pool".format(self.__class__.__name__, meth_name)
            )

        if self._connection.is_closed():
            raise errors.InterfaceError(
                "cannot call {}.{}(): "
                "the underlying connection is closed".format(
                    self.__class__.__name__, meth_name
                )
            )
